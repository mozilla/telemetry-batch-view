package telemetry.utils

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import telemetry.heka.{HekaFrame, Message}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import awscala.s3.{S3, Bucket}
import org.apache.spark.SparkContext

object Telemetry {
  implicit lazy val s3: S3 = S3()

  private def listS3Keys(bucket: Bucket, prefix: String, delimiter: String = "/"): Stream[String] = {
    import com.amazonaws.services.s3.model.{ ListObjectsRequest, ObjectListing }

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
    // obtain the prefix of the telemetry data source
    val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")
    val Some(sourcesObj) = metadataBucket.get("sources.json")
    val metaSources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(telemetryPrefix) = metaSources \\ "telemetry" \\ "prefix"

    // get a stream of object summaries that match the desired criteria
    val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-data")
    val keys = matchingPrefixes(
      bucket,
      List("").toStream,
      List(telemetryPrefix, submissionDate.toString("yyyyMMdd")) ++ pingPath
    ).flatMap(prefix => s3.objectSummaries(bucket, prefix)).map(summary => summary.getKey())

    sc.parallelize(keys).flatMap(key => {
      val hekaFile = bucket.getObject(key).getOrElse(throw new Exception(s"File missing on S3: $key"))
      for (message <- HekaFrame.parse(hekaFile.getObjectContent(), key)) yield HekaFrame.fields(message)
    })
  }

  def listOptions(submissionDate: DateTime, pingPath: List[String]): Stream[String] = {
    // obtain the prefix of the telemetry data source
    val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")
    val Some(sourcesObj) = metadataBucket.get("sources.json")
    val metaSources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(telemetryPrefix) = metaSources \\ "telemetry" \\ "prefix"

    // get a stream of object summaries that match the desired criteria
    val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-data")
    matchingPrefixes(
      bucket,
      List("").toStream,
      List(telemetryPrefix, submissionDate.toString("yyyyMMdd")) ++ pingPath ++ List("*")
    )
  }
}
