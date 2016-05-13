package telemetry.views

import awscala.s3.{S3, Bucket}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, Accumulator}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types._
import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.math.{min, max}
import telemetry.heka.{HekaFrame, Message}
import telemetry.utils.Utils
import telemetry.utils.Telemetry
import org.joda.time.{format, DateTime, Days}
import com.typesafe.config._
import org.rogach.scallop._

object CrashAggregateView {
  // configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val to = conf.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }
    val from = conf.from.get match {
      case Some(f) => fmt.parseDateTime(f)
      case _ => DateTime.now.minusDays(1)
    }

    // set up Spark
    val sparkConf = new SparkConf().setAppName("CrashAggregateView")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    val appConf = ConfigFactory.load()
    val parquetBucket = appConf.getString("app.parquetBucket")

    for (offset <- 0 to Days.daysBetween(from, to).getDays()) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyy-MM-dd")

      // obtain the crash aggregates from telemetry ping data
      val messages = Telemetry.getRecords(sc, currentDate, List("telemetry", "4", "main"))
              .union(Telemetry.getRecords(sc, currentDate, List("telemetry", "4", "crash")))
      val (rowRDD, mainProcessed, mainIgnored, crashProcessed, crashIgnored) = compareCrashes(sc, messages)

      // create a dataframe containing all the crash aggregates
      val schema = buildSchema()
      val records = sqlContext.createDataFrame(rowRDD.coalesce(1), schema)

      // upload the resulting aggregate Spark records to S3
      records.write.mode(SaveMode.Overwrite).parquet(s"s3://$parquetBucket/crash_aggregates/v1/submission_date=$currentDateString")

      println("=======================================================================================")
      println(s"JOB COMPLETED SUCCESSFULLY FOR $currentDate")
      println(s"${mainProcessed.value} main pings processed, ${mainIgnored.value} pings ignored")
      println(s"${crashProcessed.value} crash pings processed, ${crashIgnored.value} pings ignored")
      println("=======================================================================================")
    }
  }

  // paths/dimensions within the ping to compare by
  // if the path only has a single element, then the field is interpreted as a literal string rather than a JSON string
  val comparableDimensions = List(
    List("environment.build", "version"),
    List("environment.build", "buildId"),
    List("normalizedChannel"),
    List("appName"),
    List("environment.system", "os", "name"),
    List("environment.system", "os", "version"),
    List("environment.build", "architecture"),
    List("geoCountry"),
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
    "plugin_crashes", "gmplugin_crashes", "content_shutdown_crashes",
    "usage_hours_squared", "main_crashes_squared", "content_crashes_squared",
    "plugin_crashes_squared", "gmplugin_crashes_squared", "content_shutdown_crashes_squared"
  )

  private def getCountHistogramValue(histogram: JValue): Double = {
    try {
      histogram \ "values" \ "0" match {
        case JInt(count) => count.toDouble
        case _ => 0
      }
    } catch { case _: Throwable => 0 }
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
        pingFields.get(path.head) match {
          case Some(topLevelField: String) =>
            if (path.tail == List.empty) { // list of length 1, interpret field as string rather than JSON
              Some(topLevelField)
            } else { // JSON field, the rest of the path tells us where to look in the JSON
              val dimensionValue = path.tail.foldLeft(parse(topLevelField))((value, fieldName) => value \ fieldName) // retrieve the value at the given path
              dimensionValue match {
                case JString(value) => Some(value)
                case JBool(value) => Some(if (value) "True" else "False")
                case JInt(value) => Some(value.toString)
                case _ => None
              }
            }
          case _ => None
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
    val isMainPing = pingFields.get("docType") match {
      case Some("main") => true
      case Some("crash") => false
      case _ => return None
    }
    val usageHours: Double = info \ "subsessionLength" match {
      case JInt(subsessionLength) if isMainPing => // main ping, which should always have the subsession length field
        min(25, max(0, subsessionLength.toDouble / 3600))
      case JNothing if !isMainPing => 0 // crash ping, which shouldn't have the subsession length field
      case _ => return None // invalid ping - main ping without subsession length or crash ping with subsession length
    }
    val mainCrashes = if (isMainPing) 0 else 1 // if this is a crash ping, it represents one main process crash
    val contentCrashes: Double = getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "content")
    val pluginCrashes: Double = getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "plugin")
    val geckoMediaPluginCrashes: Double = getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "gmplugin")
    val contentShutdownCrashes: Double = getCountHistogramValue(keyedHistograms \ "SUBPROCESS_KILL_HARD" \ "ShutDownKill")
    val stats = List(
      if (isMainPing) 1 else 0, // number of pings represented by the aggregate
      usageHours, mainCrashes, contentCrashes,
      pluginCrashes, geckoMediaPluginCrashes, contentShutdownCrashes,

      // squared versions in order to compute stddev (with $$\sigma = \sqrt{\frac{\sum X^2}{N} - \mu^2}$$)
      usageHours * usageHours, mainCrashes * mainCrashes, contentCrashes * contentCrashes,
      pluginCrashes * pluginCrashes, geckoMediaPluginCrashes * geckoMediaPluginCrashes,
      contentShutdownCrashes * contentShutdownCrashes
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
