package com.mozilla.telemetry.views

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days, format}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse
import org.rogach.scallop._
import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils.{Addon, Attribution, MainPing, S3Store}
import org.json4s.{DefaultFormats, JValue}

import scala.util.{Success, Try}

object MainSummaryView {
  def schemaVersion: String = "v3"
  def jobName: String = "main_summary"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    val channel = opt[String]("channel", descr = "Only process data from the given channel", required = false)
    val appVersion = opt[String]("version", descr = "Only process data from the given app version", required = false)
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

    // Set up Spark
    val sparkConf = new SparkConf().setAppName(jobName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    implicit val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // We want to end up with reasonably large parquet files on S3.
    val parquetSize = 512 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    // Don't write metadata files, because they screw up partition discovery.
    // This is fixed in Spark 2.0, see:
    //   https://issues.apache.org/jira/browse/SPARK-13207
    //   https://issues.apache.org/jira/browse/SPARK-15454
    //   https://issues.apache.org/jira/browse/SPARK-15895
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")
      val filterChannel = conf.channel.get
      val filterVersion = conf.appVersion.get

      println("=======================================================================================")
      println(s"BEGINNING JOB $jobName FOR $currentDateString")
      if (filterChannel.nonEmpty)
        println(s" Filtering for channel = '${filterChannel.get}'")
      if (filterVersion.nonEmpty)
        println(s" Filtering for version = '${filterVersion.get}'")

      val schema = buildSchema
      val ignoredCount = sc.accumulator(0, "Number of Records Ignored")
      val processedCount = sc.accumulator(0, "Number of Records Processed")
      val messages = Dataset("telemetry")
        .where("sourceName") {
          case "telemetry" => true
        }.where("sourceVersion") {
          case "4" => true
        }.where("docType") {
          case "main" => true
        }.where("appName") {
          case "Firefox" => true
        }.where("submissionDate") {
          case date if date == currentDate.toString("yyyyMMdd") => true
        }.where("appUpdateChannel") {
          case channel => filterChannel.isEmpty || channel == filterChannel.get
        }.where("appVersion") {
          case v => filterVersion.isEmpty || v == filterVersion.get
        }.records(conf.limit.get)

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
      val s3prefix = s"$jobName/$schemaVersion/submission_date_s3=$currentDateString"
      val s3path = s"s3://${conf.outputBucket()}/$s3prefix"

      // Repartition the dataframe by sample_id before saving.
      val partitioned = records.repartition(100, records.col("sample_id"))

      // Then write to S3 using the given fields as path name partitions. If any
      // data already exists for the target day, cowardly refuse to run. In
      // that case, go delete the data from S3 and try again.
      partitioned.write.partitionBy("sample_id").mode("error").parquet(s3path)

      // Then remove the _SUCCESS file so we don't break Spark partition discovery.
      S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")

      println(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      println("     RECORDS SEEN:    %d".format(ignoredCount.value + processedCount.value))
      println("     RECORDS IGNORED: %d".format(ignoredCount.value))
      println("=======================================================================================")
    }
    sc.stop()
  }

  def getActiveAddons(activeAddons: JValue): Option[List[Row]] = {
    implicit val formats = DefaultFormats
    Try(activeAddons.extract[Map[String, Addon]]) match {
      case Success(addons) => {
        val rows = addons.map { case (addonId, addonData) =>
          Row(addonId,
              addonData.blocklisted.orNull,
              addonData.name.orNull,
              addonData.userDisabled.orNull,
              addonData.appDisabled.orNull,
              addonData.version.orNull,
              addonData.scope.orNull,
              addonData.`type`.orNull,
              addonData.foreignInstall.orNull,
              addonData.hasBinaryComponents.orNull,
              addonData.installDay.orNull,
              addonData.updateDay.orNull,
              addonData.signedState.orNull,
              addonData.isSystem.orNull)
        }
        Some(rows.toList)
      }
      case _ => None
    }
  }

  def getTheme(theme: JValue): Option[Row] = {
    implicit val formats = DefaultFormats
    Try(theme.extract[Addon]) match {
      case Success(addonData) =>
        Some(Row(addonData.id.getOrElse("MISSING"),
          addonData.blocklisted.orNull,
          addonData.name.orNull,
          addonData.userDisabled.orNull,
          addonData.appDisabled.orNull,
          addonData.version.orNull,
          addonData.scope.orNull,
          addonData.`type`.orNull,
          addonData.foreignInstall.orNull,
          addonData.hasBinaryComponents.orNull,
          addonData.installDay.orNull,
          addonData.updateDay.orNull,
          addonData.signedState.orNull,
          addonData.isSystem.orNull))
      case _ => None
    }
  }

  def getAttribution(attribution: JValue): Option[Row] = {
    // Return value mirrors the case class Attribution. If all the columns
    // are null, then then whole attribution field is null.
    implicit val formats = DefaultFormats
    Try(attribution.extract[Attribution]) match {
      case Success(attributionData) =>
        val row = Row(
          attributionData.source.orNull,
          attributionData.medium.orNull,
          attributionData.campaign.orNull,
          attributionData.content.orNull)
        row match {
          case Row(null, null, null, null) => None
          case attrib => Some(attrib)
        }
      case _ => None
    }
  }

  def getEvents(events: JValue): Option[List[Row]] = {
    implicit val formats = DefaultFormats
    Try(events.extract[List[List[Any]]]) match {
      case Success(eventList) => {
        val rows = eventList.flatMap {
          case _ @ List(
            timestamp: BigInt,
            category:  String,
            method:    String,
            obj:       String,
            strValue:  String,
            mapValues: Map[String @unchecked, Any @unchecked])
            => List(Row(timestamp.toLong, category, method, obj, strValue, mapValues.map { // Bug 1339130
                case (k: String, null) => (k, "null")
                case (k: String, v: Any) => (k, v.toString())
              }
            ))
          case _ @ List(
            timestamp: BigInt,
            category:  String,
            method:    String,
            obj:       String,
            strValue:  String)
            => List(Row(timestamp.toLong, category, method, obj, strValue, null))
          case _ @ List(
            timestamp: BigInt,
            category:  String,
            method:    String,
            obj:       String)
            => List(Row(timestamp.toLong, category, method, obj, null, null))
          case _ => None
        }
        rows match {
          case Nil => None
          case x => Some(x)
        }
      }
      case _ => None
    }
  }

  def getUserPrefs(prefs: JValue): Option[Row] = {
    prefs \ "dom.ipc.processCount" match {
      case JInt(pc) => Some(Row(pc.toInt))
      case _ => None
    }
  }

  def getBrowserEngagement(scalars: JValue, engagementMetric: String): Integer = {
    val prefix = "browser.engagement."
    asInt(scalars \ (prefix + engagementMetric))
  }

  def asInt(v: JValue): Integer = v match {
    case JInt(x) => x.toInt
    case _ => null
  }

  // Convert the given Heka message containing a "main" ping
  // to a map containing just the fields we're interested in.
  def messageToRow(message: Message): Option[Row] = {
    try {
      val fields = message.fieldsAsMap

      // Don't compute the expensive stuff until we need it. We may skip a record
      // due to missing required fields.
      lazy val addons = parse(fields.getOrElse("environment.addons", "{}").asInstanceOf[String])
      lazy val payload = parse(message.payload.getOrElse(fields.getOrElse("submission", "{}")).asInstanceOf[String])
      lazy val application = payload \ "application"
      lazy val build = parse(fields.getOrElse("environment.build", "{}").asInstanceOf[String])
      lazy val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
      lazy val partner = parse(fields.getOrElse("environment.partner", "{}").asInstanceOf[String])
      lazy val settings = parse(fields.getOrElse("environment.settings", "{}").asInstanceOf[String])
      lazy val system = parse(fields.getOrElse("environment.system", "{}").asInstanceOf[String])
      lazy val info = parse(fields.getOrElse("payload.info", "{}").asInstanceOf[String])
      lazy val histograms = parse(fields.getOrElse("payload.histograms", "{}").asInstanceOf[String])
      lazy val keyedHistograms = parse(fields.getOrElse("payload.keyedHistograms", "{}").asInstanceOf[String])

      lazy val weaveConfigured = MainPing.booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
      lazy val weaveDesktop = MainPing.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
      lazy val weaveMobile = MainPing.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")
      lazy val parentScalars = payload \ "payload" \ "processes" \ "parent" \ "scalars"
      lazy val parentEvents = payload \ "payload" \ "processes" \ "parent" \ "events"

      val loopActivityCounterKeys = (0 to 4).map(_.toString)
      val sslHandshakeResultKeys = (0 to 671).map(_.toString)

      // Messy list of known enum values for POPUP_NOTIFICATION_STATS.
      val popupNotificationStatsKeys = (0 to 8).union(10 to 11).union(20 to 28).union(30 to 31).map(_.toString)

      // Get the "sum" field from histogram h as an Int. Consider a
      // wonky histogram (one for which the "sum" field is not a
      // valid number) as null.
      val hsum = (h: JValue) => h \ "sum" match {
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
          case JInt(x) => x.toString()
          case _ => null
        },
        system \ "os" \ "servicePackMajor" match {
          case JInt(x) => x.toLong
          case _ => null
        },
        system \ "os" \ "servicePackMinor" match {
          case JInt(x) => x.toLong
          case _ => null
        },
        system \ "os" \ "windowsBuildNumber" match {
          case JInt(x) => x.toLong
          case _ => null
        },
        system \ "os" \ "windowsUBR" match {
          case JInt(x) => x.toLong
          case _ => null
        },
        system \ "os" \ "installYear" match {
          case JInt(x) => x.toLong
          case _ => null
        },
        system \ "isWow64" match {
          case JBool(x) => x
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
        getAttribution(settings \ "attribution").orNull,
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
        asInt(info \ "timezoneOffset"),
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
        hsum(keyedHistograms \ "SUBPROCESS_KILL_HARD" \ "ShutDownKill"),        
        MainPing.countKeys(addons \ "activeAddons") match {
          case Some(x) => x
          case _ => null
        },
        MainPing.getFlashVersion(addons) match {
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
        settings \ "defaultSearchEngine" match {
          case JString(x) => x
          case _ => null
        },
        MainPing.enumHistogramToRow(histograms \ "LOOP_ACTIVITY_COUNTER", loopActivityCounterKeys),
        hsum(histograms \ "DEVTOOLS_TOOLBOX_OPENED_COUNT"),
        fields.getOrElse("Date", None) match {
          case x: String => x
          case _ => null
        },
        MainPing.histogramToMean(histograms \ "PLACES_BOOKMARKS_COUNT").orNull,
        MainPing.histogramToMean(histograms \ "PLACES_PAGES_COUNT").orNull,
        hsum(histograms \ "PUSH_API_NOTIFY"),
        hsum(histograms \ "WEB_NOTIFICATION_SHOWN"),

        MainPing.keyedEnumHistogramToMap(keyedHistograms \ "POPUP_NOTIFICATION_STATS",
          popupNotificationStatsKeys).orNull,

        MainPing.getSearchCounts(keyedHistograms \ "SEARCH_COUNTS").orNull,

        getActiveAddons(addons \ "activeAddons").orNull,
        getTheme(addons \ "theme").orNull,
        settings \ "blocklistEnabled" match {
          case JBool(x) => x
          case _ => null
        },
        settings \ "addonCompatibilityCheckEnabled" match {
          case JBool(x) => x
          case _ => null
        },
        settings \ "telemetryEnabled" match {
          case JBool(x) => x
          case _ => null
        },
        getUserPrefs(settings \ "userPrefs").orNull,
        getBrowserEngagement(parentScalars, "max_concurrent_tab_count"),
        getBrowserEngagement(parentScalars, "tab_open_event_count"),
        getBrowserEngagement(parentScalars, "max_concurrent_window_count"),
        getBrowserEngagement(parentScalars, "window_open_event_count"),
        getBrowserEngagement(parentScalars, "total_uri_count"),
        getBrowserEngagement(parentScalars, "unfiltered_uri_count"),
        getBrowserEngagement(parentScalars, "unique_domains_count"),
        getEvents(parentEvents).orNull,

        // bug 1339655
        MainPing.enumHistogramBucketCount(histograms \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys.head).orNull,
        MainPing.enumHistogramSumCounts(histograms \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys.tail),
        MainPing.enumHistogramToMap(histograms \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys)
      )
      Some(row)
    } catch {
      case _: Exception =>
        None
    }
  }

  // Type for encapsulating search counts
  def buildSearchSchema = StructType(List(
    StructField("engine", StringType, nullable = true), // Name of the search engine
    StructField("source", StringType, nullable = true), // Source of the search (urlbar, etc)
    StructField("count",  LongType,   nullable = true)  // Number of searches
  ))

  // Enumerated buckets from LOOP_ACTIVITY_COUNTER histogram
  def buildLoopSchema = StructType(List(
    StructField("open_panel",        IntegerType, nullable = true), // bucket 0
    StructField("open_conversation", IntegerType, nullable = true), // bucket 1
    StructField("room_open",         IntegerType, nullable = true), // bucket 2
    StructField("room_share",        IntegerType, nullable = true), // bucket 3
    StructField("room_delete",       IntegerType, nullable = true)  // bucket 4
  ))

  // Enumerated buckets from POPUP_NOTIFICATION_STATS keyed histogram
  // Field names based on toolkit/modules/PopupNotifications.jsm
  def buildPopupSchema = StructType(List(
    StructField("offered",                          IntegerType, nullable = true), // bucket 0
    StructField("action_1",                         IntegerType, nullable = true), // bucket 1
    StructField("action_2",                         IntegerType, nullable = true), // bucket 2
    StructField("action_3",                         IntegerType, nullable = true), // bucket 3
    StructField("action_last",                      IntegerType, nullable = true), // bucket 4
    StructField("dismissal_click_elsewhere",        IntegerType, nullable = true), // bucket 5
    StructField("dismissal_leave_page",             IntegerType, nullable = true), // bucket 6
    StructField("dismissal_close_button",           IntegerType, nullable = true), // bucket 7
    StructField("dismissal_not_now",                IntegerType, nullable = true), // bucket 8
    StructField("open_submenu",                     IntegerType, nullable = true), // bucket 10
    StructField("learn_more",                       IntegerType, nullable = true), // bucket 11
    StructField("reopen_offered",                   IntegerType, nullable = true), // bucket 20
    StructField("reopen_action_1",                  IntegerType, nullable = true), // bucket 21
    StructField("reopen_action_2",                  IntegerType, nullable = true), // bucket 22
    StructField("reopen_action_3",                  IntegerType, nullable = true), // bucket 23
    StructField("reopen_action_last",               IntegerType, nullable = true), // bucket 24
    StructField("reopen_dismissal_click_elsewhere", IntegerType, nullable = true), // bucket 25
    StructField("reopen_dismissal_leave_page",      IntegerType, nullable = true), // bucket 26
    StructField("reopen_dismissal_close_button",    IntegerType, nullable = true), // bucket 27
    StructField("reopen_dismissal_not_now",         IntegerType, nullable = true), // bucket 28
    StructField("reopen_open_submenu",              IntegerType, nullable = true), // bucket 30
    StructField("reopen_learn_more",                IntegerType, nullable = true)  // bucket 31
  ))

  // Data for a single addon per Bug 1290181
  def buildAddonSchema = StructType(List(
      StructField("addon_id",              StringType,  nullable = false),
      StructField("blocklisted",           BooleanType, nullable = true),
      // Note: Skip "description" field - if needed, look it up from AMO.
      StructField("name",                  StringType,  nullable = true),
      StructField("user_disabled",         BooleanType, nullable = true),
      StructField("app_disabled",          BooleanType, nullable = true),
      StructField("version",               StringType,  nullable = true),
      StructField("scope",                 IntegerType, nullable = true),
      StructField("type",                  StringType,  nullable = true),
      StructField("foreign_install",       BooleanType, nullable = true),
      StructField("has_binary_components", BooleanType, nullable = true),
      StructField("install_day",           IntegerType, nullable = true),
      StructField("update_day",            IntegerType, nullable = true),
      StructField("signed_state",          IntegerType, nullable = true),
      StructField("is_system",             BooleanType, nullable = true)
    ))

  def buildAttributionSchema = StructType(List(
    StructField("source",   StringType, nullable = true),
    StructField("medium",   StringType, nullable = true),
    StructField("campaign", StringType, nullable = true),
    StructField("content",  StringType, nullable = true)
  ))

  def buildEventSchema = StructType(List(
    StructField("timestamp",    LongType, nullable = false),
    StructField("category",     StringType, nullable = false),
    StructField("method",       StringType, nullable = false),
    StructField("object",       StringType, nullable = false),
    StructField("string_value", StringType, nullable = true),
    StructField("map_values",   MapType(StringType, StringType), nullable = true)
  ))

  // Data for user prefs
  def buildUserPrefsSchema = StructType(List(
    StructField("dom_ipc_process_count", IntegerType, nullable = true) // dom.ipc.processCount
  ))

  def buildSchema: StructType = {
    StructType(List(
      StructField("document_id", StringType, nullable = false), // id
      StructField("client_id", StringType, nullable = true), // clientId
      StructField("sample_id", LongType, nullable = true), // Fields[sampleId]
      StructField("channel", StringType, nullable = true), // appUpdateChannel
      StructField("normalized_channel", StringType, nullable = true), // normalizedChannel
      StructField("country", StringType, nullable = true), // geoCountry
      StructField("city", StringType, nullable = true), // geoCity
      StructField("os", StringType, nullable = true), // environment/system/os/name
      StructField("os_version", StringType, nullable = true), // environment/system/os/version
      StructField("os_service_pack_major", LongType, nullable = true), // environment/system/os/servicePackMajor
      StructField("os_service_pack_minor", LongType, nullable = true), // environment/system/os/servicePackMinor
      StructField("windows_build_number", LongType, nullable = true), // environment/system/os/windowsBuildNumber
      StructField("windows_ubr", LongType, nullable = true), // environment/system/os/windowsUBR

      // Note: Windows only!
      StructField("install_year", LongType, nullable = true), // environment/system/os/installYear
      StructField("is_wow64", BooleanType, nullable = true), // environment/system/isWow64

      // TODO: use proper 'date' type for date columns.
      StructField("profile_creation_date", LongType, nullable = true), // environment/profile/creationDate
      StructField("subsession_start_date", StringType, nullable = true), // info/subsessionStartDate
      StructField("subsession_length", LongType, nullable = true), // info/subsessionLength
      StructField("distribution_id", StringType, nullable = true), // environment/partner/distributionId
      StructField("submission_date", StringType, nullable = false), // YYYYMMDD version of 'timestamp'
      // See bug 1232050
      StructField("sync_configured", BooleanType, nullable = true), // WEAVE_CONFIGURED
      StructField("sync_count_desktop", IntegerType, nullable = true), // WEAVE_DEVICE_COUNT_DESKTOP
      StructField("sync_count_mobile", IntegerType, nullable = true), // WEAVE_DEVICE_COUNT_MOBILE
      StructField("app_build_id", StringType, nullable = true), // application/buildId
      StructField("app_display_version", StringType, nullable = true), // application/displayVersion
      StructField("app_name", StringType, nullable = true), // application/name
      StructField("app_version", StringType, nullable = true), // application/version
      StructField("timestamp", LongType, nullable = false), // server-assigned timestamp when record was received

      StructField("env_build_id", StringType, nullable = true), // environment/build/buildId
      StructField("env_build_version", StringType, nullable = true), // environment/build/version
      StructField("env_build_arch", StringType, nullable = true), // environment/build/architecture

      // See bug 1251259
      StructField("e10s_enabled", BooleanType, nullable = true), // environment/settings/e10sEnabled
      StructField("e10s_cohort", StringType, nullable = true), // environment/settings/e10sCohort
      StructField("locale", StringType, nullable = true), // environment/settings/locale
      // See bug 1331082
      StructField("attribution", buildAttributionSchema, nullable = true), // environment/settings/attribution/

      StructField("active_experiment_id", StringType, nullable = true), // environment/addons/activeExperiment/id
      StructField("active_experiment_branch", StringType, nullable = true), // environment/addons/activeExperiment/branch
      StructField("reason", StringType, nullable = true), // info/reason

      StructField("timezone_offset", IntegerType, nullable = true), // info/timezoneOffset

      // Different types of crashes / hangs:
      StructField("plugin_hangs", IntegerType, nullable = true), // SUBPROCESS_CRASHES_WITH_DUMP / pluginhang
      StructField("aborts_plugin", IntegerType, nullable = true), // SUBPROCESS_ABNORMAL_ABORT / plugin
      StructField("aborts_content", IntegerType, nullable = true), // SUBPROCESS_ABNORMAL_ABORT / content
      StructField("aborts_gmplugin", IntegerType, nullable = true), // SUBPROCESS_ABNORMAL_ABORT / gmplugin
      StructField("crashes_detected_plugin", IntegerType, nullable = true), // SUBPROCESS_CRASHES_WITH_DUMP / plugin
      StructField("crashes_detected_content", IntegerType, nullable = true), // SUBPROCESS_CRASHES_WITH_DUMP / content
      StructField("crashes_detected_gmplugin", IntegerType, nullable = true), // SUBPROCESS_CRASHES_WITH_DUMP / gmplugin
      StructField("crash_submit_attempt_main", IntegerType, nullable = true), // PROCESS_CRASH_SUBMIT_ATTEMPT / main-crash
      StructField("crash_submit_attempt_content", IntegerType, nullable = true), // PROCESS_CRASH_SUBMIT_ATTEMPT / content-crash
      StructField("crash_submit_attempt_plugin", IntegerType, nullable = true), // PROCESS_CRASH_SUBMIT_ATTEMPT / plugin-crash
      StructField("crash_submit_success_main", IntegerType, nullable = true), // PROCESS_CRASH_SUBMIT_SUCCESS / main-crash
      StructField("crash_submit_success_content", IntegerType, nullable = true), // PROCESS_CRASH_SUBMIT_SUCCESS / content-crash
      StructField("crash_submit_success_plugin", IntegerType, nullable = true), // PROCESS_CRASH_SUBMIT_SUCCESS / plugin-crash
      StructField("shutdown_kill", IntegerType, nullable = true), // SUBPROCESS_KILL_HARD / ShutDownKill
      
      StructField("active_addons_count", LongType, nullable = true), // number of keys in environment/addons/activeAddons

      // See https://github.com/mozilla-services/data-pipeline/blob/master/hindsight/modules/fx/ping.lua#L82
      StructField("flash_version", StringType, nullable = true), // latest installable version of flash plugin.
      StructField("vendor", StringType, nullable = true), // application/vendor
      StructField("is_default_browser", BooleanType, nullable = true), // environment/settings/isDefaultBrowser
      StructField("default_search_engine_data_name", StringType, nullable = true), // environment/settings/defaultSearchEngineData/name
      StructField("default_search_engine", StringType, nullable = true), // environment/settings/defaultSearchEngine

      // LOOP_ACTIVITY_COUNTER histogram per bug 1261829
      StructField("loop_activity_counter", buildLoopSchema, nullable = true),

      // DevTools usage per bug 1262478
      StructField("devtools_toolbox_opened_count", IntegerType, nullable = true), // DEVTOOLS_TOOLBOX_OPENED_COUNT

      // client date per bug 1270505
      StructField("client_submission_date", StringType, nullable = true), // Fields[Date], the HTTP Date header sent by the client

      // We use the mean for bookmarks and pages because we do not expect them to be
      // heavily skewed during the lifetime of a subsession. Using the median for a
      // histogram would probably be better in general, but the granularity of the
      // buckets for these particular histograms is not fine enough for the median
      // to give a more accurate value than the mean.
      StructField("places_bookmarks_count", IntegerType, nullable = true), // mean of PLACES_BOOKMARKS_COUNT
      StructField("places_pages_count", IntegerType, nullable = true), // mean of PLACES_PAGES_COUNT

      // Push metrics per bug 1270482 and bug 1311174
      StructField("push_api_notify", IntegerType, nullable = true), // PUSH_API_NOTIFY
      StructField("web_notification_shown", IntegerType, nullable = true), // WEB_NOTIFICATION_SHOWN

      // Info from POPUP_NOTIFICATION_STATS keyed histogram
      StructField("popup_notification_stats", MapType(StringType, buildPopupSchema), nullable = true),

      // Search counts
      StructField("search_counts", ArrayType(buildSearchSchema, containsNull = false), nullable = true), // split up and organize the SEARCH_COUNTS keyed histogram

      // Addon and configuration settings per Bug 1290181
      StructField("active_addons", ArrayType(buildAddonSchema, containsNull = false), nullable = true), // One per item in environment.addons.activeAddons
      StructField("active_theme", buildAddonSchema, nullable = true), // environment.addons.theme
      StructField("blocklist_enabled", BooleanType, nullable = true), // environment.settings.blocklistEnabled
      StructField("addon_compatibility_check_enabled", BooleanType, nullable = true), // environment.settings.addonCompatibilityCheckEnabled
      StructField("telemetry_enabled", BooleanType, nullable = true), // environment.settings.telemetryEnabled
      StructField("user_prefs", buildUserPrefsSchema, nullable = true), // environment.settings.userPrefs

      // Engagement measures per Bug 1315663, taken from
      //  payload.processes.parent.scalars["browser.engagement.*"]
      // For more information, see the Scalars definitions at
      //  https://dxr.mozilla.org/mozilla-central/source/toolkit/components/telemetry/Scalars.yaml
      StructField("max_concurrent_tab_count", IntegerType, nullable = true),
      StructField("tab_open_event_count", IntegerType, nullable = true),
      StructField("max_concurrent_window_count", IntegerType, nullable = true),
      StructField("window_open_event_count", IntegerType, nullable = true),
      StructField("total_uri_count", IntegerType, nullable = true),
      StructField("unfiltered_uri_count", IntegerType, nullable = true),
      StructField("unique_domains_count", IntegerType, nullable = true),
      StructField("events", ArrayType(buildEventSchema, containsNull = false), nullable = true), // payload.processes.parent.events

      // bug 1339655
      StructField("ssl_handshake_result_success", IntegerType, nullable = true),
      StructField("ssl_handshake_result_failure", IntegerType, nullable = true),
      StructField("ssl_handshake_result", MapType(StringType, IntegerType), nullable = true) // SSL_HANDSHAKE_RESULT
    ))
  }
}
