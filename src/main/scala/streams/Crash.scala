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

case class Crash(prefix: String) extends DerivedStream {
  override def streamName: String = "telemetry"
  override def filterPrefix: String = prefix

  // paths/dimensions within the ping to compare by
  def comparableDimensions = List(
    ("environment.build", "version"),
    ("environment.build", "buildId"),
    (null, "normalizedChannel"),
    (null, "appName"),
    ("environment.system", "os", "name"),
    ("environment.system", "os", "version"),
    ("environment.build", "architecture"),
    (null, "geoCountry"),
    ("environment.addons", "activeExperiment", "id"),
    ("environment.addons", "activeExperiment", "branch"),
    ("environment.settings", "e10sEnabled")
  )

  // names of the comparable dimensions above, used as dimension names in the database
  def dimensionNames = List(
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
        for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey())) yield message
      }

    val aggregates = messages.flatMap(getCrashPairs)
  }

  private def buildSchema: Schema = {
    SchemaBuilder
      .record("Submission").fields()
        .name("activity_date").`type`().stringType().noDefault()
        .name("dimensions").`type`().optional().map().values().stringType()
        .name("stats").`type`().optional().map().values().doubleType()
      .endRecord()
  }

  private def getCountHistogramValue(histogram: JValue): BigInt = {
    histogram \ "values" \ "0" match {
      case JInt(count) => count
      case _ => 0
    }
  }

  private def getCrashPairs(pingFields: Map[String, Any]): Option[Map[String, Any]] = {
    val info = parse(pingFields.getOrElse("payload.info", "{}"))
    val build = parse(pingFields.getOrElse("environment.build", "{}"))
    val keyedHistograms = parse(pingFields.getOrElse("payload.keyedHistograms", "{}"))

    val submissionDate = pingFields.get("submissionDate") match {
      case Some(date: String) =>
        // convert YYYYMMDD timestamp to a real date
        try { normalizeYYYYMMDDTimestamp(date) } catch { case _ => return None }
      case _ => return None
    }
    val activityDateRaw = pingFields.get("creationTimestamp") match {
      case Some(date: Double) =>
        // convert nanosecond timestamp to a second timestamp to a real date
        try { normalizeEpochTimestamp(date / 1e9) } catch { case _ => return None }
      case _ => return None
    }
    val activityDate = activityDateRaw //wip: round to nearest day, clamp based on submissionDate
    val uniqueKey = activityDate :: (
      for (path <- comparableDimensions) yield (
        if (path.head == null) {
          pingFields.get(path.tail.head) match {
            case field: String => Some(field)
            case _ => None
          }
        } else {
          pingFields.get(path.head) match {
            case Some(field: String) =>
              try {
                val dimensionValue = foldLeft(parse(field))(path.tail) // retrieve the value at the given path
                Some(dimensionValue)
              } catch { case _ => None }
            case None => None
          }
        }
      )
    )
    if (!uniqueKey("build_id").matches("\\d{14}")) { // validate build IDs
      return None
    }

    val usageHours = info \ "subsessionLength" match {
      case JDouble(subsessionLength) =>
        min(25, max(0, subsessionLength / 3600))
      case JNothing => 0
      case _ => return None
    }
    val mainCrashes = pingFields \ "docType" match {
      case JString(docType) => if (docType == "crash") 1 else 0
      case _ => return None
    }
    val contentCrashes = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "content")
    } catch { case _ => 0 }
    val pluginCrashes = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "plugin")
    } catch { case _ => 0 }
    val geckoMediaPluginCrashes = try {
      getCountHistogramValue(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "gmplugin")
    } catch { case _ => 0 }
    val stats = List(
      1,  // number of pings represented by the aggregate
      usageHours, mainCrashes, contentCrashes,
      pluginCrashes, geckoMediaPluginCrashes,

      // squared versions in order to compute stddev (with $$\sigma = \sqrt{\frac{\sum X^2}{N} - \mu^2}$$)
      usageHours ** 2, mainCrashes ** 2, contentCrashes ** 2,
      pluginCrashes ** 2, geckoMediaPluginCrashes ** 2
    )

    Some((uniqueKey, stats))
  }

  private def buildRecord(history: Iterable[Map[String, Any]], schema: Schema): Option[GenericRecord] = {
    val sorted = history.foldLeft((List[Map[String, Any]](), Set[String]()))(
      { case ((submissions, seen), current) =>
        current.get("documentId") match {
          case Some(docId) =>
            if (seen.contains(docId.asInstanceOf[String]))
              (submissions, seen) // Duplicate documentId, ignore it
            else
              (current :: submissions, seen + docId.asInstanceOf[String]) // new documentId, add it to the list
          case None =>
            (submissions, seen) // Ignore clients with records that have a missing documentId
        }
      })._1.reverse  // preserve ordering

    val root = new GenericRecordBuilder(schema)
      .set("client_id", sorted(0)("clientId").asInstanceOf[String])
      .set("os", sorted(0)("os").asInstanceOf[String])
      .set("normalized_channel", sorted(0)("normalizedChannel").asInstanceOf[String])

    try {
      threadHangStats2Avro(sorted, root, schema)
    } catch {
      case e : Throwable =>
        // Ignore buggy clients
        return None
    }

    Some(root.build)
  }
}
