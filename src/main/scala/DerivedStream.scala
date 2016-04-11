package telemetry

import DerivedStream.s3
import awscala.s3._
import com.typesafe.config._
import java.io.File
import java.util.UUID
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import scala.io.Source
import telemetry.streams.{E10sExperiment, ExecutiveStream, Churn, Longitudinal}
import telemetry.streams.main_summary.MainSummary
import telemetry.utils.Utils

// key is the S3 filename, size is the object size in bytes.
case class ObjectSummary(key: String, size: Long) // S3ObjectSummary can't be serialized

abstract class DerivedStream extends java.io.Serializable{
  private val appConf = ConfigFactory.load()
  private val parquetBucket = Bucket(appConf.getString("app.parquetBucket"))
  private val metaBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")
  protected lazy val metaSources = {
    val Some(sourcesObj) = metaBucket.get(s"sources.json")
    parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
  }
  private lazy val metaPrefix = {
    val JString(metaPrefix) = metaSources \\ streamName \\ "metadata_prefix"
    metaPrefix
  }
  
  protected val clsName = Utils.uncamelize(this.getClass.getSimpleName.replace("$", ""))  // Use classname as stream prefix on S3
  protected lazy val partitioning = {
    val Some(schemaObj) = metaBucket.get(s"$metaPrefix/schema.json")
    val schema = Source.fromInputStream(schemaObj.getObjectContent()).getLines().mkString("\n")
    Partitioning(schema)
  }

  protected def isS3PrefixEmpty(prefix: String): Boolean = {
    s3.objectSummaries(parquetBucket, s"$clsName/$prefix").isEmpty
  }

  protected def uploadLocalFileToS3(path: Path, prefix: String) {
    val uuid = UUID.randomUUID.toString
    val key = s"$clsName/$prefix/$uuid"
    val file = new File(path.toUri())
    val bucketName = parquetBucket.name
    println(s"Uploading Parquet file to $bucketName/$key")
    s3.putObject(bucketName, key, file)
    file.delete()
  }

  protected def streamName: String
  protected def filterPrefix: String = ""
  protected def transform(sc: SparkContext, bucket: Bucket, input: RDD[ObjectSummary], from: String, to: String)
}

object DerivedStream {
  private type OptionMap = Map[Symbol, String]

  implicit lazy val s3: S3 = S3()
  private lazy val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")

  private def parseOptions(args: Array[String]): OptionMap = {
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--from-date" :: value :: tail =>
          nextOption(map ++ Map('fromDate -> value), tail)
        case "--to-date" :: value :: tail =>
          nextOption(map ++ Map('toDate -> value), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('stream -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('stream -> string), list.tail)
        case option :: tail => Map()
      }
    }

    nextOption(Map(), args.toList)
  }

  private def S3Prefix(logical: String): String = {
    val Some(sourcesObj) = metadataBucket.get(s"sources.json")
    val sources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(prefix) = sources \\ logical \\ "prefix"
    prefix
  }

  private def S3ls(bucket: Bucket, prefix: String, delimiter: String = "/"): Stream[String] = {
    import com.amazonaws.services.s3.model.{ ListObjectsRequest, ObjectListing }

    val request = new ListObjectsRequest().withBucketName(bucket.getName).withPrefix(prefix).withDelimiter(delimiter)
    val firstListing = s3.listObjects(request)

    def completeStream(listing: ObjectListing): Stream[String] = {
      val prefixes = listing.getCommonPrefixes.asScala.toStream
      prefixes #::: (if (listing.isTruncated) completeStream(s3.listNextBatchOfObjects(listing)) else Stream.empty)
    }

    completeStream(firstListing)
  }

  private def matchingPrefixes(bucket: Bucket, prefixes: Stream[String], pattern: List[String]): Stream[String] = {
    if (pattern.isEmpty) {
      prefixes
    } else {
      val matching = prefixes
        .flatMap(prefix => S3ls(bucket, prefix))
        .filter(prefix => (pattern.head == "*" || prefix.endsWith(pattern.head + "/")))
      matchingPrefixes(bucket, matching, pattern.tail)
    }
  }

  private def convert(converter: DerivedStream, from: String, to: String) {
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = formatter.parseDateTime(from)
    val toDate = formatter.parseDateTime(to)
    val daysCount = Days.daysBetween(fromDate, toDate).getDays()
    val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-data")
    val prefix = S3Prefix(converter.streamName)
    val filterPrefix = converter.filterPrefix

    val conf = new SparkConf().setAppName("telemetry-batch-view")
    conf.setMaster(conf.get("spark.master", "local[*]"))

    val sc = new SparkContext(conf)
    println("Spark parallelism level: " + sc.defaultParallelism)

    val summaries = sc.parallelize(0 to daysCount)
      .map(fromDate.plusDays(_).toString("yyyyMMdd"))
      .flatMap(date => {
                 val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-data")
                 matchingPrefixes(bucket, List("").toStream, s"$prefix/$date/$filterPrefix".split("/").toList)
                   .flatMap(prefix => s3.objectSummaries(bucket, prefix))
                   .map(summary => ObjectSummary(summary.getKey(), summary.getSize()))})

    converter.transform(sc, bucket, summaries, from, to)
  }

  def groupBySize(keys: Iterator[ObjectSummary]): List[List[ObjectSummary]] = {
    val threshold = 1L << 31
    keys.foldRight((0L, List[List[ObjectSummary]]()))(
      (x, acc) => {
        acc match {
          case (size, head :: tail) if size + x.size < threshold =>
            (size + x.size, (x :: head) :: tail)
          case (size, res) if size + x.size < threshold =>
            (size + x.size, List(x) :: res)
          case (_, res) =>
            (x.size, List(x) :: res)
        }
      })._2
  }

  def main(args: Array[String]) {
    val usage = "converter --from-date YYYYMMDD --to-date YYYYMMDD stream_name"
    val options = parseOptions(args)

    val res = for {
      stream <- options.get('stream)

      to = options.get('toDate) match {
        case Some(date) => date
        case None =>
          val formatter = DateTimeFormat.forPattern("yyyyMMdd")
          // Default to processing "yesterday" to ensure we process a complete day.
          formatter.print(DateTime.now().minusDays(1))
      }

      (from, ds) <- stream match {
        case "Longitudinal" =>
          val longitudinal = Longitudinal()
          Some(options.getOrElse('fromDate, to), longitudinal)

        case "ExecutiveStream" =>
          Some(options.getOrElse('fromDate, to), ExecutiveStream)

        case "Churn" =>
          val churn = Churn("telemetry/4/main/Firefox")
          Some(options.getOrElse('fromDate, to), churn)

        case "E10SExperiment" =>
          val from = options.getOrElse('fromDate, "20160321")
          val exp = E10sExperiment("e10s-beta46-cohorts", "telemetry/4/saved_session/Firefox/beta/46.0/")
          Some(from, exp)

        case "MainSummary" => // https://bugzilla.mozilla.org/show_bug.cgi?id=1260847
          val mainSummary = MainSummary("telemetry/4/main/Firefox")
          Some(options.getOrElse('fromDate, to), mainSummary)

        case _ =>
          None
      }

      res = convert(ds, from, to)
    } yield res

    if (res.isEmpty)
      println(usage)
  }
}
