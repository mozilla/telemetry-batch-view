package telemetry.views

import awscala.s3._
import com.typesafe.config._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext, Accumulator}
import org.apache.spark.rdd.RDD
import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.math.{max, abs}
import telemetry.heka.{HekaFrame, Message}
import telemetry.utils.Utils
import org.joda.time._
import org.rogach.scallop._

object CrashAggregateView {
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    verify()
  }

  def main(args: Array[String]) {
    // load configuration for the 
    val conf = new Conf(args)
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val to = conf.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }
    val from = conf.from.get match {
      case Some(f) => fmt.parseDateTime(f)
      case _ => DateTime.now.minusDays(1)
    }

    // set up the Spark, Hadoop, and other configurations
    val sparkConf = new SparkConf().setAppName("CrashAggregateVie")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    val appConf = ConfigFactory.load()
    val parquetBucket = appConf.getString("app.parquetBucket")

    for (offset <- 0 until Days.daysBetween(from, to).getDays()) {
      val currentDate = from.plusDays(offset)

      // obtain the crash aggregates from telemetry ping data
      val messages = getRecords(sc, currentDate, "crash").union(getRecords(sc, currentDate, "main"))
      val (rowRDD, main_processed, main_ignored, crash_processed, crash_ignored) = compareCrashes(sc, messages)

      // create a dataframe containing all the crash aggregates
      val schema = buildSchema()
      val records = sqlContext.createDataFrame(rowRDD, schema)

      // upload the resulting aggregate Spark records to S3
      records.write.mode(SaveMode.Overwrite).parquet(s"s3://$parquetBucket/crash_aggregates/v$currentDate")

      println("=======================================================================================")
      println(s"JOB COMPLETED SUCCESSFULLY FOR $currentDate")
      println(s"${main_processed.value} main pings processed, ${main_ignored.value} pings ignored")
      println(s"${crash_processed.value} crash pings processed, ${crash_ignored.value} pings ignored")
      println("=======================================================================================")
    }
  }

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
  private def getRecords(sc: SparkContext, submissionDate: DateTime, docType: String): RDD[Map[String, Any]] = {
    // obtain the prefix of the telemetry data source
    val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")
    val Some(sourcesObj) = metadataBucket.get("sources.json")
    val metaSources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(telemetryPrefix) = metaSources \\ "telemetry" \\ "prefix"

    // get a stream of object summaries that match the desired criteria
    val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-data")
    val summaries = matchingPrefixes(
      bucket,
      List("").toStream,
      List(telemetryPrefix, submissionDate.toString("yyyyMMdd"), "telemetry", "4", docType)
    ).flatMap(prefix => s3.objectSummaries(bucket, prefix))

    sc.parallelize(summaries).flatMap(summary => {
      val key = summary.getKey()
      val hekaFile = bucket.getObject(key).getOrElse(throw new Exception(s"File missing on S3: $key"))
      for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey())) yield HekaFrame.fields(message)
    })
  }

  // paths/dimensions within the ping to compare by
  val comparableDimensions = List(
    List("environment.build", "version"),
    List("environment.build", "buildId"),
    List(null, "normalizedChannel"),
    List(null, "appName"),
    List("environment.system", "os", "name"),
    List("environment.system", "os", "version"),
    List("environment.build", "architecture"),
    List(null, "geoCountry"),
    List("environment.addons", "activeExperiment", "id"),
    List("environment.addons", "activeExperiment", "branch"),
    List("environment.settings", "e10sEnabled"),
    List("environment.settings", "e10sCohort")
  )

  // names of the comparable dimensions above, used as dimension names in the database
  val dimensionNames = List(
    "build_version",
    "build_id",
    "channel",
    "application",
    "os_name",
    "os_version",
    "architecture",
    "country",
    "experiment_id",
    "experiment_branch",
    "e10s_enabled",
    "e10s_cohort"
  )

  val statsNames = List(
    "ping_count",
    "usage_hours", "main_crashes", "content_crashes",
    "plugin_crashes", "gmplugin_crashes",
    "usage_hours_squared", "main_crashes_squared", "content_crashes_squared",
    "plugin_crashes_squared", "gmplugin_crashes_squared"
  )

  private def getCountHistogramValue(histogram: JValue): Double = {
    histogram \ "values" \ "0" match {
      case JInt(count) => count.toDouble
      case _ => 0
    }
  }

  def compareCrashes(sc: SparkContext, messages: RDD[Map[String, Any]]): (RDD[Row], Accumulator[Int], Accumulator[Int], Accumulator[Int], Accumulator[Int]) = {
    // get the crash pairs for all of the pings, keeping track of how many we see
    val mainProcessedAccumulator = sc.accumulator(0, "Number of processed main pings")
    val mainIgnoredAccumulator = sc.accumulator(0, "Number of ignored main pings")
    val crashProcessedAccumulator = sc.accumulator(0, "Number of processed crash pings")
    val crashIgnoredAccumulator = sc.accumulator(0, "Number of ignored crash pings")
    val crashPairs = messages.flatMap((pingFields) => {
      getCrashPair(pingFields) match {
        case Some(crashPair) => {
          pingFields.get("docType") match {
            case Some("crash") => crashProcessedAccumulator += 1
            case Some("main") => mainProcessedAccumulator += 1
            case _ => null
          }
          List(crashPair)
        }
        case None => {
          pingFields.get("docType") match {
            case Some("crash") => crashIgnoredAccumulator += 1
            case Some("main") => mainIgnoredAccumulator += 1
            case _ => null
          }
          List()
        }
      }
    })

    // aggregate crash pairs by their keys
    val aggregates = crashPairs.reduceByKey(
      (crashStatsA: List[Double], crashStatsB: List[Double]) =>
        (crashStatsA, crashStatsB).zipped.map(_ + _)
    )

    val records = aggregates.map((aggregatedCrashPair: (List[Any], List[Double])) => {
      // extract and compute the record fields
      val (uniqueKey, stats) = aggregatedCrashPair
      val (activityDate, dimensions) = (uniqueKey.head.asInstanceOf[String], uniqueKey.tail.asInstanceOf[List[Option[String]]])
      val dimensionsMap: Map[String, String] = (dimensionNames, dimensions).zipped.flatMap((key, value) =>
        (key, value) match { // remove dimensions that don't have values
          case (key, Some(value)) => Some(key, value)
          case (key, None) => None
        }
      ).toMap
      val statsMap = (statsNames, stats).zipped.toMap

      Row(activityDate, mapAsJavaMap(dimensionsMap), mapAsJavaMap(statsMap))
    })

    (records, mainProcessedAccumulator, mainIgnoredAccumulator, crashProcessedAccumulator, crashIgnoredAccumulator)
  }

  private def getCrashPair(pingFields: Map[String, Any]): Option[(List[java.io.Serializable], List[Double])] = {
    val build = pingFields.get("environment.build") match {
      case Some(value: String) => parse(value)
      case _ => JObject()
    }
    val info = pingFields.get("payload.info") match {
      case Some(value: String) => parse(value)
      case _ => JObject()
    }
    val keyedHistograms = pingFields.get("payload.keyedHistograms") match {
      case Some(value: String) => parse(value)
      case _ => JObject()
    }

    // obtain the activity date clamped to a reasonable time range
    val submissionDate = pingFields.get("submissionDate") match {
      case Some(date: String) =>
        // convert YYYYMMDD timestamp to a real date
        try {
          format.DateTimeFormat.forPattern("yyyyMMdd").withZone(org.joda.time.DateTimeZone.UTC).parseDateTime(date)
        } catch {
          case _: Throwable => return None
        }
      case _ => return None
    }
    val activityDateRaw = pingFields.get("creationTimestamp") match {
      case Some(date: Double) => {
        // convert nanosecond timestamp to a second timestamp to a real date
        try {
          new DateTime((date / 1e6).toLong).withZone(org.joda.time.DateTimeZone.UTC).withMillisOfDay(0) // only keep the date part of the timestamp
        } catch {
          case _: Throwable => return None
        }
      }
      case _ => return None
    }
    val activityDate = if (activityDateRaw.isBefore(submissionDate.minusDays(7))) { // clamp activity date to a good range
      submissionDate.minusDays(7)
    } else if (activityDateRaw.isAfter(submissionDate)) {
      submissionDate
    } else {
      activityDateRaw
    }
    val activityDateString = format.DateTimeFormat.forPattern("yyyy-MM-dd").print(activityDate) // format activity date as YYYY-MM-DD

    // obtain the unique key of the aggregate that this ping belongs to
    val uniqueKey = activityDateString :: (
      for (path <- comparableDimensions) yield {
        if (path.head == null) {
          pingFields.get(path.tail.head) match {
            case Some(topLevelField: String) => Some(topLevelField)
            case _ => None
          }
        } else {
          pingFields.get(path.head) match {
            case Some(topLevelField: String) =>
              val dimensionValue = path.tail.foldLeft(parse(topLevelField))((value, fieldName) => value \ fieldName) // retrieve the value at the given path
              dimensionValue match {
                case JString(value) => Some(value)
                case JBool(value) => Some(if (value) "True" else "False")
                case JInt(value) => Some(value.toString)
                case _ => None
              }
            case _ => None
          }
        }
      }
    )

    // validate build IDs
    val buildId = uniqueKey(dimensionNames.indexOf("build_id") + 1) // we add 1 because the first element is taken by activityDateString
    buildId match {
      case Some(value: String) if value.matches("\\d{14}") => null
      case _ => return None
    }

    // obtain the relevant stats for the ping
    val usageHours: Double = info \ "subsessionLength" match {
      case JInt(subsessionLength) =>
        Math.min(25, Math.max(0, subsessionLength.toDouble / 3600))
      case JNothing => 0
      case _ => return None
    }
    val mainCrashes: Double = pingFields.get("docType") match {
      case Some("main") => 0
      case Some("crash") => 1
      case _ => return None
    }
    val contentCrashes: Double = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "content")
    } catch { case _: Throwable => 0 }
    val pluginCrashes: Double = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "plugin")
    } catch { case _: Throwable => 0 }
    val geckoMediaPluginCrashes: Double = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "gmplugin")
    } catch { case _: Throwable => 0 }
    val stats = List(
      1, // number of pings represented by the aggregate
      usageHours, mainCrashes, contentCrashes,
      pluginCrashes, geckoMediaPluginCrashes,

      // squared versions in order to compute stddev (with $$\sigma = \sqrt{\frac{\sum X^2}{N} - \mu^2}$$)
      usageHours * usageHours, mainCrashes * mainCrashes, contentCrashes * contentCrashes,
      pluginCrashes * pluginCrashes, geckoMediaPluginCrashes * geckoMediaPluginCrashes
    )

    // return a pair so we can use PairRDD operations on this data later
    Some((uniqueKey, stats))
  }

  def buildSchema(): StructType = {
    StructType(
      StructField("activity_date", StringType, false) ::
      StructField("dimensions", MapType(StringType, StringType, true), false) ::
      StructField("stats", MapType(StringType, DoubleType, true), false) ::
      Nil
    )
  }
}
