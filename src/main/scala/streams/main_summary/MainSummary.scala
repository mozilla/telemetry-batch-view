package telemetry.streams.main_summary

import awscala.s3.Bucket
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import org.json4s.JsonAST.{JBool, JInt, JString, JValue}
import org.json4s.jackson.JsonMethods.parse
import telemetry.DerivedStream.s3
import telemetry.heka.{HekaFrame, Message}
import telemetry.parquet.ParquetFile
import telemetry.{DerivedStream, ObjectSummary}

// Partition a PairRDD by treating the key as a sample_id. Put missing
// values into a separate bucket. We modulo the key in case of
// unexpected numeric values.
private class SampleIdPartitioner extends Partitioner{
  def numPartitions: Int = 101
  def getPartition(key: Any): Int = key match {
    case Some(sampleId: Long) => (sampleId % (numPartitions - 1)).toInt
    case _ => numPartitions - 1
  }
}

case class MainSummary(prefix: String) extends DerivedStream{
  override def filterPrefix: String = prefix
  override def streamName: String = "telemetry"
  def streamVersion: String = "v2"

  // Convert the given Heka message containing a "main" ping
  // to a map containing just the fields we're interested in.
  def messageToMap(message: Message): Option[Map[String,Any]] = {
    val fields = HekaFrame.fields(message)

    // Don't compute the expensive stuff until we need it. We may skip a record
    // due to missing required fields.
    lazy val addons = parse(fields.getOrElse("environment.addons", "{}").asInstanceOf[String])
    lazy val payload = parse(message.payload.getOrElse("{}").asInstanceOf[String])
    lazy val application = payload \ "application"
    lazy val build = parse(fields.getOrElse("environment.build", "{}").asInstanceOf[String])
    lazy val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
    lazy val partner = parse(fields.getOrElse("environment.partner", "{}").asInstanceOf[String])
    lazy val settings = parse(fields.getOrElse("environment.settings", "{}").asInstanceOf[String])
    lazy val system = parse(fields.getOrElse("environment.system", "{}").asInstanceOf[String])
    lazy val info = parse(fields.getOrElse("payload.info", "{}").asInstanceOf[String])
    lazy val histograms = parse(fields.getOrElse("payload.histograms", "{}").asInstanceOf[String])
    lazy val keyedHistograms = parse(fields.getOrElse("payload.keyedHistograms", "{}").asInstanceOf[String])

    lazy val weaveConfigured = Utils.booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
    lazy val weaveDesktop = Utils.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
    lazy val weaveMobile = Utils.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")

    // Get the "sum" field from histogram h as an Int. Consider a
    // wonky histogram (one for which the "sum" field is not a
    // valid number) as null.
    val hsum = (h: JValue) => h \ "sum" match {
      case JInt(x) => x.toInt
      case _ => null
    }

    // Get the value of a given histogram bucket as an Int.
    val hval = (h: JValue, b: String) => h \ "values" \ b match {
      case JInt(x) => x.toInt
      case _ => null
    }

    val map = Map[String, Any](
      "document_id" -> (fields.getOrElse("documentId", None) match {
        case x: String => x
        // documentId is required, and must be a string. If either
        // condition is not satisfied, we skip this record.
        case _ => return None
      }),
      "submission_date" -> (fields.getOrElse("submissionDate", None) match {
        case x: String => x
        case _ => return None // required
      }),
      "timestamp" -> message.timestamp, // required
      "client_id" -> (fields.getOrElse("clientId", None) match {
        case x: String => x
        case _ => null
      }),
      "sample_id" -> (fields.getOrElse("sampleId", None) match {
        case x: Long => x
        case x: Double => x.toLong
        case _ => null
      }),
      "channel" -> (fields.getOrElse("appUpdateChannel", None) match {
        case x: String => x
        case _ => ""
      }),
      "normalized_channel" -> (fields.getOrElse("normalizedChannel", None) match {
        case x: String => x
        case _ => ""
      }),
      "country" -> (fields.getOrElse("geoCountry", None) match {
        case x: String => x
        case _ => ""
      }),
      "city" -> (fields.getOrElse("geoCity", None) match {
        case x: String => x
        case _ => ""
      }),
      "profile_creation_date" -> ((profile \ "creationDate") match {
        case JInt(x) => x.toLong
        case _ => null
      }),
      "sync_configured" -> weaveConfigured.getOrElse(null),
      "sync_count_desktop" -> weaveDesktop.getOrElse(null),
      "sync_count_mobile" -> weaveMobile.getOrElse(null),
      "subsession_start_date" -> ((info \ "subsessionStartDate") match {
        case JString(x) => x
        case _ => null
      }),
      "subsession_length" -> ((info \ "subsessionLength") match {
        case JInt(x) => x.toLong
        case _ => null
      }),
      "distribution_id" -> ((partner \ "distributionId") match {
        case JString(x) => x
        case _ => null
      }),
      "e10s_enabled" -> ((settings \ "e10sEnabled") match {
        case JBool(x) => x
        case _ => null
      }),
      "e10s_cohort" -> ((settings \ "e10sCohort") match {
        case JString(x) => x
        case _ => null
      }),
      "os" -> ((system \ "os" \ "name") match {
        case JString(x) => x
        case _ => null
      }),
      "os_version" -> ((system \ "os" \ "version") match {
        case JString(x) => x
        case _ => null
      }),
      "os_service_pack_major" -> ((system \ "os" \ "servicePackMajor") match {
        case JString(x) => x
        case _ => null
      }),
      "os_service_pack_minor" -> ((system \ "os" \ "servicePackMinor") match {
        case JString(x) => x
        case _ => null
      }),
      "app_build_id" -> ((application \ "buildId") match {
        case JString(x) => x
        case _ => null
      }),
      "app_display_version" -> ((application \ "displayVersion") match {
        case JString(x) => x
        case _ => null
      }),
      "app_name" -> ((application \ "name") match {
        case JString(x) => x
        case _ => null
      }),
      "app_version" -> ((application \ "version") match {
        case JString(x) => x
        case _ => null
      }),
      "env_build_id" -> ((build \ "buildId") match {
        case JString(x) => x
        case _ => null
      }),
      "env_build_version" -> ((build \ "version") match {
        case JString(x) => x
        case _ => null
      }),
      "env_build_arch" -> ((build \ "architecture") match {
        case JString(x) => x
        case _ => null
      }),
      "locale" -> ((settings \ "locale") match {
        case JString(x) => x
        case _ => null
      }),
      "active_experiment_id" -> ((addons \ "activeExperiment" \ "id") match {
        case JString(x) => x
        case _ => null
      }),
      "active_experiment_branch" -> ((addons \ "activeExperiment" \ "branch") match {
        case JString(x) => x
        case _ => null
      }),
      "reason" -> ((info \ "reason") match {
        case JString(x) => x
        case _ => null
      }),
      "vendor" -> ((application \ "vendor") match {
        case JString(x) => x
        case _ => null
      }),
      "timezone_offset" -> ((info \ "timezoneOffset") match {
        case JInt(x) => x.toInt
        case _ => null
      }),
      // Crash count fields
      "plugin_hangs" -> hsum(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "pluginhang"),
      "aborts_plugin" -> hsum(keyedHistograms \ "SUBPROCESS_ABNORMAL_ABORT" \ "plugin"),
      "aborts_content" -> hsum(keyedHistograms \ "SUBPROCESS_ABNORMAL_ABORT" \ "content"),
      "aborts_gmplugin" -> hsum(keyedHistograms \ "SUBPROCESS_ABNORMAL_ABORT" \ "gmplugin"),
      "crashes_detected_plugin" -> hsum(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "plugin"),
      "crashes_detected_content" -> hsum(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "content"),
      "crashes_detected_gmplugin" -> hsum(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "gmplugin"),
      "crash_submit_attempt_main" -> hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "main-crash"),
      "crash_submit_attempt_content" -> hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "content-crash"),
      "crash_submit_attempt_plugin" -> hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "plugin-crash"),
      "crash_submit_success_main" -> hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "main-crash"),
      "crash_submit_success_content" -> hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "content-crash"),
      "crash_submit_success_plugin" -> hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "plugin-crash"),
      // End crash count fields
      "active_addons_count" -> (Utils.countKeys(addons \ "activeAddons") match {
        case Some(x) => x
        case _ => null
      }),
      "flash_version" -> (Utils.getFlashVersion(addons) match {
        case Some(x) => x
        case _ => null
      }),
      "is_default_browser" -> ((settings \ "isDefaultBrowser") match {
        case JBool(x) => x
        case _ => null
      }),
      "default_search_engine_data_name" -> ((settings \ "defaultSearchEngineData" \ "name") match {
        case JString(x) => x
        case _ => null
      }),
      // Loop activity counters
      "loop_activity_open_panel"        -> hval(histograms \ "LOOP_ACTIVITY_COUNTER", "0"),
      "loop_activity_open_conversation" -> hval(histograms \ "LOOP_ACTIVITY_COUNTER", "1"),
      "loop_activity_room_open"         -> hval(histograms \ "LOOP_ACTIVITY_COUNTER", "2"),
      "loop_activity_room_share"        -> hval(histograms \ "LOOP_ACTIVITY_COUNTER", "3"),
      "loop_activity_room_delete"       -> hval(histograms \ "LOOP_ACTIVITY_COUNTER", "4"),
      "devtools_toolbox_opened_count"   -> hsum(histograms \ "DEVTOOLS_TOOLBOX_OPENED_COUNT"),
      "search_counts" -> Utils.getSearchCounts(keyedHistograms \ "SEARCH_COUNTS")
    )
    Some(map)
  }

  override def transform(sc: SparkContext, bucket: Bucket, ignoreMe: RDD[ObjectSummary], from: String, to: String) {
    // Iterate by day, ignoring the passed-in s3objects
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = formatter.parseDateTime(from)
    val toDate = formatter.parseDateTime(to)
    val daysCount = Days.daysBetween(fromDate, toDate).getDays()
    val bucket = {
      val JString(bucketName) = metaSources \\ streamName \\ "bucket"
      Bucket(bucketName)
    }
    val dataPrefix = {
      val JString(prefix) = metaSources \\ streamName \\ "prefix"
      prefix
    }

    // Process each day from fromDate to toDate (inclusive) separately.
    for (i <- 0 to daysCount) {
      val currentDay = fromDate.plusDays(i).toString("yyyyMMdd")
      println("Processing day: " + currentDay)
      val summaries = sc.parallelize(s3.objectSummaries(bucket, s"$dataPrefix/$currentDay/$filterPrefix")
                        .map(summary => ObjectSummary(summary.getKey(), summary.getSize())))

      val groups = DerivedStream.groupBySize(summaries.collect().toIterator)
      val ignoredCount = sc.accumulator(0, "Number of Records Ignored")
      val processedCount = sc.accumulator(0, "Number of Records Processed")
      val summaryMessages = sc.parallelize(groups, groups.size)
        .flatMap(x => x)
        .flatMap { case obj =>
          val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3: " + obj.key))
          for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey())) yield message
        }
        .flatMap { case message => {
          messageToMap(message) match {
            case None =>
              ignoredCount += 1
              None
            case x =>
              processedCount += 1
              x
          }
        } }
        // Partition by sampleId (with one extra partition for missing values)
        .flatMap { case m => List((m.get("sample_id"), m)) }
        .partitionBy(new SampleIdPartitioner())

      summaryMessages.values.foreachPartition { case partitionIterator =>
        val schema = buildSchema
        val records = for {
          record <- partitionIterator
          saveable <- buildRecord(record, schema)
        } yield saveable

        while (!records.isEmpty) {
          val localFile = ParquetFile.serialize(records, schema)
          uploadLocalFileToS3(localFile, s"$streamVersion/submission_date_s3=$currentDay")
        }
      }

      println("%s: Records seen:    %d".format(currentDay, ignoredCount.value + processedCount.value))
      println("%s: Records ignored: %d".format(currentDay, ignoredCount.value))
    }
  }

  def buildSchema: Schema = {
    // Type for encapsulating search counts
    val searchCountsType = SchemaBuilder
      .record("SearchCounts").fields()
      .name("engine").`type`().stringType().noDefault() // Name of the search engine
      .name("source").`type`().stringType().noDefault() // Source of the search (urlbar, etc)
      .name("count").`type`().longType().noDefault() // Number of searches
      .endRecord()

    SchemaBuilder
      .record("MainSummary").fields
      .name("document_id").`type`().stringType().noDefault() // id
      .name("client_id").`type`().nullable().stringType().noDefault() // clientId
      .name("sample_id").`type`().nullable().intType().noDefault() // Fields[sampleId]
      .name("channel").`type`().nullable().stringType().noDefault() // appUpdateChannel
      .name("normalized_channel").`type`().nullable().stringType().noDefault() // normalizedChannel
      .name("country").`type`().nullable().stringType().noDefault() // geoCountry
      .name("city").`type`().nullable().stringType().noDefault() // geoCity
      .name("os").`type`().nullable().stringType().noDefault() // environment/system/os/name
      .name("os_version").`type`().nullable().stringType().noDefault() // environment/system/os/version
      .name("os_service_pack_major").`type`().nullable().stringType().noDefault() // environment/system/os/servicePackMajor
      .name("os_service_pack_minor").`type`().nullable().stringType().noDefault() // environment/system/os/servicePackMinor

      // TODO: use proper 'date' type for date columns.
      .name("profile_creation_date").`type`().nullable().intType().noDefault() // environment/profile/creationDate
      .name("subsession_start_date").`type`().nullable().stringType().noDefault() // info/subsessionStartDate
      .name("subsession_length").`type`().nullable().intType().noDefault() // info/subsessionLength
      .name("distribution_id").`type`().nullable().stringType().noDefault() // environment/partner/distributionId
      .name("submission_date").`type`().stringType().noDefault()
      // See bug 1232050
      .name("sync_configured").`type`().nullable().booleanType().noDefault() // WEAVE_CONFIGURED
      .name("sync_count_desktop").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_DESKTOP
      .name("sync_count_mobile").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_MOBILE
      .name("app_build_id").`type`().nullable().stringType().noDefault() // application/buildId
      .name("app_display_version").`type`().nullable().stringType().noDefault() // application/displayVersion
      .name("app_name").`type`().nullable().stringType().noDefault() // application/name
      .name("app_version").`type`().nullable().stringType().noDefault() // application/version
      .name("timestamp").`type`().longType().noDefault() // server-assigned timestamp when record was received

      .name("env_build_id").`type`().nullable().stringType().noDefault() // environment/build/buildId
      .name("env_build_version").`type`().nullable().stringType().noDefault() // environment/build/version
      .name("env_build_arch").`type`().nullable().stringType().noDefault() // environment/build/architecture

      // See bug 1251259
      .name("e10s_enabled").`type`().nullable().booleanType().noDefault() // environment/settings/e10sEnabled
      .name("e10s_cohort").`type`().nullable().stringType().noDefault() // environment/settings/e10sCohort
      .name("locale").`type`().nullable().stringType().noDefault() // environment/settings/locale

      .name("active_experiment_id").`type`().nullable().stringType().noDefault() // environment/addons/activeExperiment/id
      .name("active_experiment_branch").`type`().nullable().stringType().noDefault() // environment/addons/activeExperiment/branch
      .name("reason").`type`().nullable().stringType().noDefault() // info/reason

      .name("timezone_offset").`type`().nullable().intType().noDefault() // info/timezoneOffset

      // Different types of crashes / hangs:
      .name("plugin_hangs").`type`().nullable().intType().noDefault() // SUBPROCESS_CRASHES_WITH_DUMP / pluginhang
      .name("aborts_plugin").`type`().nullable().intType().noDefault() // SUBPROCESS_ABNORMAL_ABORT / plugin
      .name("aborts_content").`type`().nullable().intType().noDefault() // SUBPROCESS_ABNORMAL_ABORT / content
      .name("aborts_gmplugin").`type`().nullable().intType().noDefault() // SUBPROCESS_ABNORMAL_ABORT / gmplugin
      .name("crashes_detected_plugin").`type`().nullable().intType().noDefault() // SUBPROCESS_CRASHES_WITH_DUMP / plugin
      .name("crashes_detected_content").`type`().nullable().intType().noDefault() // SUBPROCESS_CRASHES_WITH_DUMP / content
      .name("crashes_detected_gmplugin").`type`().nullable().intType().noDefault() // SUBPROCESS_CRASHES_WITH_DUMP / gmplugin
      .name("crash_submit_attempt_main").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_ATTEMPT / main-crash
      .name("crash_submit_attempt_content").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_ATTEMPT / content-crash
      .name("crash_submit_attempt_plugin").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_ATTEMPT / plugin-crash
      .name("crash_submit_success_main").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_SUCCESS / main-crash
      .name("crash_submit_success_content").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_SUCCESS / content-crash
      .name("crash_submit_success_plugin").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_SUCCESS / plugin-crash

      .name("active_addons_count").`type`().nullable().intType().noDefault() // number of keys in environment/addons/activeAddons

      // See https://github.com/mozilla-services/data-pipeline/blob/master/hindsight/modules/fx/ping.lua#L82
      .name("flash_version").`type`().nullable().stringType().noDefault() // latest installable version of flash plugin.
      .name("vendor").`type`().nullable().stringType().noDefault() // application/vendor
      .name("is_default_browser").`type`().nullable().booleanType().noDefault() // environment/settings/isDefaultBrowser
      .name("default_search_engine_data_name").`type`().nullable().stringType().noDefault() // environment/settings/defaultSearchEngineData/name

      // Loop activity counters per bug 1261829
      .name("loop_activity_open_panel").`type`().nullable().intType().noDefault() // LOOP_ACTIVITY_COUNTER bucket 0
      .name("loop_activity_open_conversation").`type`().nullable().intType().noDefault() // LOOP_ACTIVITY_COUNTER bucket 1
      .name("loop_activity_room_open").`type`().nullable().intType().noDefault() // LOOP_ACTIVITY_COUNTER bucket 2
      .name("loop_activity_room_share").`type`().nullable().intType().noDefault() // LOOP_ACTIVITY_COUNTER bucket 3
      .name("loop_activity_room_delete").`type`().nullable().intType().noDefault() // LOOP_ACTIVITY_COUNTER bucket 4

      // DevTools usage per bug 1262478
      .name("devtools_toolbox_opened_count").`type`().nullable().intType().noDefault() // DEVTOOLS_TOOLBOX_OPENED_COUNT

      // Search counts
      .name("search_counts").`type`().optional().array().items(searchCountsType) // split up and organize the SEARCH_COUNTS keyed histogram
      .endRecord
  }

  def buildRecord(fields: Map[String,Any], schema: Schema): Option[GenericRecord] = {
    val root = new GenericRecordBuilder(schema)
    for ((fieldName, fieldValue) <- fields) {
      fieldValue match {
        case Some(optValue) => optValue match {
          case listValue: List[Map[String,Any]] => {
            // Handle "list" type fields (namely search counts)
            val items = scala.collection.mutable.ArrayBuffer.empty[GenericRecord]
            val fieldSchema = schema.getField(fieldName).schema().getTypes().get(1).getElementType()
            for (r <- listValue) {
              val built = buildRecord(r, fieldSchema)
              for (b <- built)
                items.append(b)
            }
            if (items.nonEmpty) {
              root.set(fieldName, items.toArray)
            }
          }
          case _ => root.set(fieldName, optValue) // Treat it as Some(<scalar>)
        }
        case None => println(s"Skipping None field $fieldName")
        case _ => root.set(fieldName, fieldValue) // Simple case: scalar fields.
      }
    }
    Some(root.build)
  }
}