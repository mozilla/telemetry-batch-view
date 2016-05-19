package telemetry.views

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days, format}
import org.json4s.JsonAST.{JBool, JInt, JString, JValue}
import org.json4s.jackson.JsonMethods.parse
import org.rogach.scallop._
import telemetry.heka.{HekaFrame, Message}
import telemetry.streams.main_summary.Utils
import telemetry.utils.Telemetry

object MainSummaryView {
  def streamVersion: String = "v3"
  def jobName: String = "main_summary"

  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val bucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = false)
    val filter = opt[String]("filter", descr = "S3 filter for source data", required = false)
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

    val appConf = ConfigFactory.load()

    // Use the 'bucket' parameter if supplied, else default to
    // what's in the config.
    val parquetBucket = conf.bucket.get match {
      case Some(b) => b
      case _ => appConf.getString("app.parquetBucket")
    }

    // Use the supplied S3 filter path, or default to all main
    // pings from Firefox.
    val s3filter = conf.filter.get match {
      case Some(f) => f
      case _ => "telemetry/4/main/Firefox"
    }

    // Set up Spark
    val sparkConf = new SparkConf().setAppName(jobName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // We want to end up with reasonably large parquet files on S3.
    val parquetSize = 512 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    // Don't write temp files to S3 while building parquet files.
    hadoopConf.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")

      println("=======================================================================================")
      println(s"BEGINNING JOB $jobName FOR $currentDateString")

      val schema = buildSchema
      val ignoredCount = sc.accumulator(0, "Number of Records Ignored")
      val processedCount = sc.accumulator(0, "Number of Records Processed")
      val messages = Telemetry.getMessages(sc, currentDate, s3filter.split('/').toList)
      val rowRDD = messages.flatMap(m => {
        messageToRow(m) match {
          case None =>
            ignoredCount += 1
            None
          case x =>
            processedCount += 1
            x
        }
      })

      val records = sqlContext.createDataFrame(rowRDD, schema)

      // Note we cannot just use 'partitionBy' below to automatically populate
      // the submission_date partition, because none of the write modes do
      // quite what we want:
      //  - "overwrite" causes the entire vX partition to be deleted and replaced with
      //    the current day's data, so doesn't work with incremental jobs
      //  - "append" would allow us to generate duplicate data for the same day, so
      //    we would need to add some manual checks before running
      //  - "error" (the default) causes the job to fail after any data is
      //    loaded, so we can't do single day incremental updates.
      //  - "ignore" causes new data not to be saved.
      // So we manually add the "submission_date_s3" parameter to the s3path.
      val s3path = s"s3://$parquetBucket/$jobName/$streamVersion/submission_date_s3=$currentDateString"

      // Repartition the dataframe by sample_id before saving.
      val partitioned = records.repartition(100, records.col("sample_id"))

      // Then write to S3 using the given fields as path name partitions. If any
      // data already exists for the target day, cowardly refuse to run. In
      // that case, go delete the data from S3 and try again.
      partitioned.write.partitionBy("sample_id").mode("error").parquet(s3path)

      println(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      println("     RECORDS SEEN:    %d".format(ignoredCount.value + processedCount.value))
      println("     RECORDS IGNORED: %d".format(ignoredCount.value))
      println("=======================================================================================")
    }
  }

  // Convert the given Heka message containing a "main" ping
  // to a map containing just the fields we're interested in.
  def messageToRow(message: Message): Option[Row] = {
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

    val row = Row(
      // Row fields must match the structure in 'buildSchema'
      fields.getOrElse("documentId", None) match {
        case x: String => x
        // documentId is required, and must be a string. If either
        // condition is not satisfied, we skip this record.
        case _ => return None
      },
      fields.getOrElse("clientId", None) match {
        case x: String => x
        case _ => null
      },
      fields.getOrElse("sampleId", None) match {
        case x: Long => x
        case x: Double => x.toLong
        case _ => null
      },
      fields.getOrElse("appUpdateChannel", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("normalizedChannel", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("geoCountry", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("geoCity", None) match {
        case x: String => x
        case _ => ""
      },
      system \ "os" \ "name" match {
        case JString(x) => x
        case _ => null
      },
      system \ "os" \ "version" match {
        case JString(x) => x
        case _ => null
      },
      system \ "os" \ "servicePackMajor" match {
        case JString(x) => x
        case _ => null
      },
      system \ "os" \ "servicePackMinor" match {
        case JString(x) => x
        case _ => null
      },
      profile \ "creationDate" match {
        case JInt(x) => x.toLong
        case _ => null
      },
      info \ "subsessionStartDate" match {
        case JString(x) => x
        case _ => null
      },
      info \ "subsessionLength" match {
        case JInt(x) => x.toLong
        case _ => null
      },
      partner \ "distributionId" match {
        case JString(x) => x
        case _ => null
      },
      fields.getOrElse("submissionDate", None) match {
        case x: String => x
        case _ => return None // required
      },
      weaveConfigured.orNull,
      weaveDesktop.orNull,
      weaveMobile.orNull,
      application \ "buildId" match {
        case JString(x) => x
        case _ => null
      },
      application \ "displayVersion" match {
        case JString(x) => x
        case _ => null
      },
      application \ "name" match {
        case JString(x) => x
        case _ => null
      },
      application \ "version" match {
        case JString(x) => x
        case _ => null
      },
      message.timestamp, // required
      build \ "buildId" match {
        case JString(x) => x
        case _ => null
      },
      build \ "version" match {
        case JString(x) => x
        case _ => null
      },
      build \ "architecture" match {
        case JString(x) => x
        case _ => null
      },
      settings \ "e10sEnabled" match {
        case JBool(x) => x
        case _ => null
      },
      settings \ "e10sCohort" match {
        case JString(x) => x
        case _ => null
      },
      settings \ "locale" match {
        case JString(x) => x
        case _ => null
      },
      addons \ "activeExperiment" \ "id" match {
        case JString(x) => x
        case _ => null
      },
      addons \ "activeExperiment" \ "branch" match {
        case JString(x) => x
        case _ => null
      },
      info \ "reason" match {
        case JString(x) => x
        case _ => null
      },
      info \ "timezoneOffset" match {
        case JInt(x) => x.toInt
        case _ => null
      },
      hsum(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "pluginhang"),
      hsum(keyedHistograms \ "SUBPROCESS_ABNORMAL_ABORT" \ "plugin"),
      hsum(keyedHistograms \ "SUBPROCESS_ABNORMAL_ABORT" \ "content"),
      hsum(keyedHistograms \ "SUBPROCESS_ABNORMAL_ABORT" \ "gmplugin"),
      hsum(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "plugin"),
      hsum(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "content"),
      hsum(keyedHistograms \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "gmplugin"),
      hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "main-crash"),
      hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "content-crash"),
      hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "plugin-crash"),
      hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "main-crash"),
      hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "content-crash"),
      hsum(keyedHistograms \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "plugin-crash"),
      Utils.countKeys(addons \ "activeAddons") match {
        case Some(x) => x
        case _ => null
      },
      Utils.getFlashVersion(addons) match {
        case Some(x) => x
        case _ => null
      },
      application \ "vendor" match {
        case JString(x) => x
        case _ => null
      },
      settings \ "isDefaultBrowser" match {
        case JBool(x) => x
        case _ => null
      },
      settings \ "defaultSearchEngineData" \ "name" match {
        case JString(x) => x
        case _ => null
      },
      hval(histograms \ "LOOP_ACTIVITY_COUNTER", "0"),
      hval(histograms \ "LOOP_ACTIVITY_COUNTER", "1"),
      hval(histograms \ "LOOP_ACTIVITY_COUNTER", "2"),
      hval(histograms \ "LOOP_ACTIVITY_COUNTER", "3"),
      hval(histograms \ "LOOP_ACTIVITY_COUNTER", "4"),
      hsum(histograms \ "DEVTOOLS_TOOLBOX_OPENED_COUNT"),
      Utils.getSearchCounts(keyedHistograms \ "SEARCH_COUNTS").orNull
    )
    Some(row)
  }

  def buildSchema: StructType = {
    // Type for encapsulating search counts
    val searchCountsType = StructType(List(
      StructField("engine", StringType, true), // Name of the search engine
      StructField("source", StringType, true), // Source of the search (urlbar, etc)
      StructField("count", LongType, true) // Number of searches
    ))

    StructType(List(
      StructField("document_id", StringType, false), // id
      StructField("client_id", StringType, true), // clientId
      StructField("sample_id", LongType, true), // Fields[sampleId]
      StructField("channel", StringType, true), // appUpdateChannel
      StructField("normalized_channel", StringType, true), // normalizedChannel
      StructField("country", StringType, true), // geoCountry
      StructField("city", StringType, true), // geoCity
      StructField("os", StringType, true), // environment/system/os/name
      StructField("os_version", StringType, true), // environment/system/os/version
      StructField("os_service_pack_major", StringType, true), // environment/system/os/servicePackMajor
      StructField("os_service_pack_minor", StringType, true), // environment/system/os/servicePackMinor

      // TODO: use proper 'date' type for date columns.
      StructField("profile_creation_date", LongType, true), // environment/profile/creationDate
      StructField("subsession_start_date", StringType, true), // info/subsessionStartDate
      StructField("subsession_length", LongType, true), // info/subsessionLength
      StructField("distribution_id", StringType, true), // environment/partner/distributionId
      StructField("submission_date", StringType, false), // YYYYMMDD version of 'timestamp'
      // See bug 1232050
      StructField("sync_configured", BooleanType, true), // WEAVE_CONFIGURED
      StructField("sync_count_desktop", IntegerType, true), // WEAVE_DEVICE_COUNT_DESKTOP
      StructField("sync_count_mobile", IntegerType, true), // WEAVE_DEVICE_COUNT_MOBILE
      StructField("app_build_id", StringType, true), // application/buildId
      StructField("app_display_version", StringType, true), // application/displayVersion
      StructField("app_name", StringType, true), // application/name
      StructField("app_version", StringType, true), // application/version
      StructField("timestamp", LongType, false), // server-assigned timestamp when record was received

      StructField("env_build_id", StringType, true), // environment/build/buildId
      StructField("env_build_version", StringType, true), // environment/build/version
      StructField("env_build_arch", StringType, true), // environment/build/architecture

      // See bug 1251259
      StructField("e10s_enabled", BooleanType, true), // environment/settings/e10sEnabled
      StructField("e10s_cohort", StringType, true), // environment/settings/e10sCohort
      StructField("locale", StringType, true), // environment/settings/locale

      StructField("active_experiment_id", StringType, true), // environment/addons/activeExperiment/id
      StructField("active_experiment_branch", StringType, true), // environment/addons/activeExperiment/branch
      StructField("reason", StringType, true), // info/reason

      StructField("timezone_offset", IntegerType, true), // info/timezoneOffset

      // Different types of crashes / hangs:
      StructField("plugin_hangs", IntegerType, true), // SUBPROCESS_CRASHES_WITH_DUMP / pluginhang
      StructField("aborts_plugin", IntegerType, true), // SUBPROCESS_ABNORMAL_ABORT / plugin
      StructField("aborts_content", IntegerType, true), // SUBPROCESS_ABNORMAL_ABORT / content
      StructField("aborts_gmplugin", IntegerType, true), // SUBPROCESS_ABNORMAL_ABORT / gmplugin
      StructField("crashes_detected_plugin", IntegerType, true), // SUBPROCESS_CRASHES_WITH_DUMP / plugin
      StructField("crashes_detected_content", IntegerType, true), // SUBPROCESS_CRASHES_WITH_DUMP / content
      StructField("crashes_detected_gmplugin", IntegerType, true), // SUBPROCESS_CRASHES_WITH_DUMP / gmplugin
      StructField("crash_submit_attempt_main", IntegerType, true), // PROCESS_CRASH_SUBMIT_ATTEMPT / main-crash
      StructField("crash_submit_attempt_content", IntegerType, true), // PROCESS_CRASH_SUBMIT_ATTEMPT / content-crash
      StructField("crash_submit_attempt_plugin", IntegerType, true), // PROCESS_CRASH_SUBMIT_ATTEMPT / plugin-crash
      StructField("crash_submit_success_main", IntegerType, true), // PROCESS_CRASH_SUBMIT_SUCCESS / main-crash
      StructField("crash_submit_success_content", IntegerType, true), // PROCESS_CRASH_SUBMIT_SUCCESS / content-crash
      StructField("crash_submit_success_plugin", IntegerType, true), // PROCESS_CRASH_SUBMIT_SUCCESS / plugin-crash

      StructField("active_addons_count", LongType, true), // number of keys in environment/addons/activeAddons

      // See https://github.com/mozilla-services/data-pipeline/blob/master/hindsight/modules/fx/ping.lua#L82
      StructField("flash_version", StringType, true), // latest installable version of flash plugin.
      StructField("vendor", StringType, true), // application/vendor
      StructField("is_default_browser", BooleanType, true), // environment/settings/isDefaultBrowser
      StructField("default_search_engine_data_name", StringType, true), // environment/settings/defaultSearchEngineData/name

      // Loop activity counters per bug 1261829
      StructField("loop_activity_open_panel", IntegerType, true), // LOOP_ACTIVITY_COUNTER bucket 0
      StructField("loop_activity_open_conversation", IntegerType, true), // LOOP_ACTIVITY_COUNTER bucket 1
      StructField("loop_activity_room_open", IntegerType, true), // LOOP_ACTIVITY_COUNTER bucket 2
      StructField("loop_activity_room_share", IntegerType, true), // LOOP_ACTIVITY_COUNTER bucket 3
      StructField("loop_activity_room_delete", IntegerType, true), // LOOP_ACTIVITY_COUNTER bucket 4

      // DevTools usage per bug 1262478
      StructField("devtools_toolbox_opened_count", IntegerType, true), // DEVTOOLS_TOOLBOX_OPENED_COUNT

      // Search counts
      StructField("search_counts", ArrayType(searchCountsType, false), true) // split up and organize the SEARCH_COUNTS keyed histogram
    ))
  }
}
