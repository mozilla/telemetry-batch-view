package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.spark.{SparkContext, Partitioner}
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.math.{max, abs}
import scala.reflect.ClassTag
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.avro
import telemetry.heka.{HekaFrame, Message}
import telemetry.histograms._
import telemetry.parquet.ParquetFile
import telemetry.utils.Utils
import org.joda.time._

case class Crash(prefix: String) extends DerivedStream {
  override def streamName: String = "telemetry"
  override def filterPrefix: String = prefix

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
    List("environment.settings", "e10sEnabled")
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
    "e10s_enabled"
  )

  val statsNames = List(
    "ping_count",
    "usage_hours", "main_crashes", "content_crashes",
    "plugin_crashes", "gmplugin_crashes",
    "usage_hours_squared", "main_crashes_squared", "content_crashes_squared",
    "plugin_crashes_squared", "gmplugin_crashes_squared"
  )

  override def transform(sc: SparkContext, bucket: Bucket, summaries: RDD[ObjectSummary], from: String, to: String) {
    val prefix = s"v$to"

    if (!isS3PrefixEmpty(prefix)) {
      println(s"Warning: prefix $prefix already exists on S3!")
      return
    }

    val groups = DerivedStream.groupBySize(summaries.collect().toIterator)
    val messages = sc.parallelize(groups, groups.size)
      .flatMap(x => x) // convert the list of heka key groups into a list of heka keys
      .flatMap{ case obj => // convert the list of heka keys to a stream of actual heka messages
        val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3: %s".format(obj.key)))
        for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey())) yield HekaFrame.fields(message)
      }

    // obtain the records for the crash aggregates as an iterator
    val records = compareCrashes(sc, messages).toLocalIterator

    // upload the resulting aggregate Avro records to S3
    val schema = buildSchema()
    val localFile = ParquetFile.serialize(records, schema)
    uploadLocalFileToS3(localFile, prefix)
  }

  private def getCountHistogramValue(histogram: JValue): Double = {
    histogram \ "values" \ "0" match {
      case JInt(count) => count.toDouble
      case _ => 0
    }
  }

  private def compareCrashes(sc: SparkContext, messages: RDD[Map[String, Any]]): RDD[GenericRecord] = {
    // get the crash pairs for all of the pings, keeping track of how many we see
    val processedAccumulator = sc.accumulator(0, "Number of processed pings")
    val ignoredAccumulator = sc.accumulator(0, "Number of ignored pings")
    val crashPairs = messages.flatMap((pingFields) => {
      getCrashPair(pingFields) match {
        case Some(crashPair) => {
          processedAccumulator += 1
          List(crashPair)
        }
        case None => {
          ignoredAccumulator += 1
          List()
        }
      }
    })

    // aggregate crash pairs by their keys
    val aggregates = crashPairs.reduceByKey(
      (crashStatsA: List[Double], crashStatsB: List[Double]) =>
        (crashStatsA, crashStatsB).zipped.map(_ + _)
    )

    // build Avro records from the aggregated crash pairs
    val schema = buildSchema()
    aggregates.map(
      (aggregatedCrashPair) => buildRecord(aggregatedCrashPair, schema)
    )
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
      case Some(date: Double) =>
        // convert nanosecond timestamp to a second timestamp to a real date
        try { new DateTime(date / 1e6).withMillisOfDay(0) } catch { case _: Throwable => return None } // only keep the date part of the timestamp
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
              try {
                val dimensionValue = path.tail.foldLeft(parse(topLevelField))((value, fieldName) => value \ fieldName) // retrieve the value at the given path
                Some(dimensionValue)
              } catch { case _: Throwable => None }
            case _ => None
          }
        }
      }
    )
    val buildId = uniqueKey(dimensionNames.indexOf("build_id") + 1).asInstanceOf[String] // we add 1 because the first element is taken by activityDateString
    if (!buildId.matches("\\d{14}")) { // validate build IDs
      return None
    }

    // obtain the relevant stats for the ping
    val usageHours: Double = info \ "subsessionLength" match {
      case JDouble(subsessionLength) =>
        Math.min(25, Math.max(0, subsessionLength / 3600))
      case JNothing => 0
      case _ => return None
    }
    val mainCrashes = pingFields.get("docType") match {
      case Some(JString(docType)) => if (docType == "crash") 1 else 0
      case _ => return None
    }
    val contentCrashes = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "content")
    } catch { case _: Throwable => 0 }
    val pluginCrashes = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "plugin")
    } catch { case _: Throwable => 0 }
    val geckoMediaPluginCrashes = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "gmplugin")
    } catch { case _: Throwable => 0 }
    val stats = List(
      1,  // number of pings represented by the aggregate
      usageHours, mainCrashes, contentCrashes,
      pluginCrashes, geckoMediaPluginCrashes,

      // squared versions in order to compute stddev (with $$\sigma = \sqrt{\frac{\sum X^2}{N} - \mu^2}$$)
      usageHours * usageHours, mainCrashes * mainCrashes, contentCrashes * contentCrashes,
      pluginCrashes * pluginCrashes, geckoMediaPluginCrashes * geckoMediaPluginCrashes
    )

    // return a pair so we can use PairRDD operations on this data later
    Some((uniqueKey, stats))
  }

  private def buildSchema(): Schema = {
    SchemaBuilder
      .record("Submission").fields()
        .name("activity_date").`type`().stringType().noDefault()
        .name("dimensions").`type`().optional().map().values().stringType()
        .name("stats").`type`().optional().map().values().doubleType()
      .endRecord()
  }

  private def buildRecord(aggregatedCrashPair: (List[Any], List[Double]), schema: Schema): GenericRecord = {
    // extract and compute the record fields
    val (uniqueKey, stats) = aggregatedCrashPair
    val (activityDate, dimensions) = (uniqueKey.head.asInstanceOf[DateTime], uniqueKey.tail.asInstanceOf[List[Option[String]]])
    val dimensionsMap = (dimensionNames, dimensions).zipped.flatMap((key, value) => (key, value) match { // remove dimensions that don't have values
      case (key, Some(value)) => Some(key, value)
      case (key, None) => None
    }).toMap
    val statsMap = (statsNames, stats).zipped.toMap

    val root = new GenericRecordBuilder(schema)
      .set("activity_date", activityDate)
      .set("dimensions", mapAsJavaMap(dimensionsMap))
      .set("stats", mapAsJavaMap(statsMap))

    root.build()
  }
}
