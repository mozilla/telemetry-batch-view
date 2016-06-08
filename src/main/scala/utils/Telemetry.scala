package telemetry.utils

import awscala.s3.{Bucket, S3}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import telemetry.ObjectSummary
import telemetry.heka.{HekaFrame, Message}
import scala.collection.JavaConverters._
import scala.io.Source

object Telemetry {
  implicit lazy val s3: S3 = S3()

  private def listS3Keys(bucket: Bucket, prefix: String, delimiter: String = "/"): Stream[String] = {
    import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

    val request = new ListObjectsRequest().withBucketName(bucket.getName).withPrefix(prefix).withDelimiter(delimiter)
    val firstListing = s3.listObjects(request)

    def completeStream(listing: ObjectListing): Stream[String] = {
      val prefixes = listing.getCommonPrefixes.asScala.toStream
      prefixes #::: (if (listing.isTruncated) completeStream(s3.listNextBatchOfObjects(listing)) else Stream.empty)
    }

    completeStream(firstListing)
  }

  private def matchingPrefixes(bucket: Bucket, seenPrefixes: Stream[String], pattern: List[String]): Stream[String] = {
    if (pattern.isEmpty) {
      seenPrefixes
    } else {
      val matching = seenPrefixes
        .flatMap(prefix => listS3Keys(bucket, prefix))
        .filter(prefix => (pattern.head == "*" || prefix.endsWith(pattern.head + "/")))
      matchingPrefixes(bucket, matching, pattern.tail)
    }
  }

  def appendToFile(p: String, s: String): Unit = {
    val pw = new java.io.PrintWriter(new java.io.FileOutputStream(new java.io.File(p),true))
    try pw.write(s) finally pw.close()
  }

  def getRecords(sc: SparkContext, submissionDate: DateTime, pingPath: List[String]): RDD[Map[String, Any]] = {
    getMessages(sc, submissionDate, pingPath).map(HekaFrame.fields)
  }

  def getMessages(sc: SparkContext, submissionDate: DateTime, pingPath: List[String]): RDD[Message] = {
    // obtain the prefix of the telemetry data source
    val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")
    val Some(sourcesObj) = metadataBucket.get("sources.json")
    val metaSources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(telemetryPrefix) = metaSources \\ "telemetry" \\ "prefix"
    val JString(dataBucket) = metaSources \\ "telemetry" \\ "bucket"

    // get a stream of object summaries that match the desired criteria
    val bucket = Bucket(dataBucket)

    val summaries = matchingPrefixes(
      bucket,
      List("").toStream,
      List(telemetryPrefix, submissionDate.toString("yyyyMMdd")) ++ pingPath
    ).flatMap(prefix => s3.objectSummaries(bucket, prefix)).map(summary => ObjectSummary(summary.getKey, summary.getSize))

    // Partition the files into groups of approximately-equal size
    val groups = ObjectSummary.groupBySize(summaries.toIterator)
    sc.parallelize(groups, groups.size).flatMap(x => x).flatMap(o => {
      val hekaFile = bucket.getObject(o.key).getOrElse(throw new Exception(s"File missing on S3: ${o.key}"))
      for (message <- HekaFrame.parse(hekaFile.getObjectContent, o.key)) yield message
    })
  }
}