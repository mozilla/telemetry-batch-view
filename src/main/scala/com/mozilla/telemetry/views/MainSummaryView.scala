package com.mozilla.telemetry.views

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.joda.time.{DateTime, Days, format}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse
import org.rogach.scallop._
import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils.{Addon, Attribution, Events,
  Experiment, getOrCreateSparkSession, MainPing, S3Store}
import com.mozilla.telemetry.utils.{BooleanUserPref, IntegerUserPref, StringUserPref, UserPref}
import org.json4s.{JValue, DefaultFormats}
import com.mozilla.telemetry.metrics._

import scala.util.{Success, Try}

object MainSummaryView {

  def schemaVersion: String = "v4"
  def jobName: String = "main_summary"

  val histogramsWhitelist =
    "A11Y_INSTANTIATED_FLAG" ::
    "A11Y_CONSUMERS" ::
    "CERT_VALIDATION_SUCCESS_BY_CA" ::
    "CYCLE_COLLECTOR_MAX_PAUSE" ::
    "FX_SEARCHBAR_SELECTED_RESULT_METHOD" ::
    "FX_URLBAR_SELECTED_RESULT_INDEX" ::
    "FX_URLBAR_SELECTED_RESULT_INDEX_BY_TYPE" ::
    "FX_URLBAR_SELECTED_RESULT_METHOD" ::
    "FX_URLBAR_SELECTED_RESULT_TYPE" ::
    "GC_MAX_PAUSE_MS" ::
    "GC_MAX_PAUSE_MS_2" ::
    "GHOST_WINDOWS" ::
    "HTTP_CHANNEL_DISPOSITION" ::
    "HTTP_PAGELOAD_IS_SSL" ::
    "HTTP_TRANSACTION_IS_SSL" ::
    "INPUT_EVENT_RESPONSE_COALESCED_MS" ::
    "SEARCH_RESET_RESULT" ::
    "SSL_HANDSHAKE_RESULT" ::
    "SSL_HANDSHAKE_VERSION" ::
    "SSL_TLS12_INTOLERANCE_REASON_PRE" ::
    "SSL_TLS13_INTOLERANCE_REASON_PRE" ::
    "TIME_TO_DOM_COMPLETE_MS" ::
    "TIME_TO_DOM_CONTENT_LOADED_END_MS" ::
    "TIME_TO_DOM_CONTENT_LOADED_START_MS" ::
    "TIME_TO_DOM_INTERACTIVE_MS" ::
    "TIME_TO_DOM_LOADING_MS" ::
    "TIME_TO_FIRST_CLICK_MS" ::
    "TIME_TO_FIRST_INTERACTION_MS" ::
    "TIME_TO_FIRST_KEY_INPUT_MS" ::
    "TIME_TO_FIRST_MOUSE_MOVE_MS" ::
    "TIME_TO_FIRST_SCROLL_MS" ::
    "TIME_TO_LOAD_EVENT_END_MS" ::
    "TIME_TO_LOAD_EVENT_START_MS" ::
    "TIME_TO_NON_BLANK_PAINT_MS" ::
    "TIME_TO_RESPONSE_START_MS" ::
    "TOUCH_ENABLED_DEVICE" ::
    "TRACKING_PROTECTION_ENABLED" ::
    "UPTAKE_REMOTE_CONTENT_RESULT_1" ::
    "WEBVR_TIME_SPENT_VIEWING_IN_2D" ::
    "WEBVR_TIME_SPENT_VIEWING_IN_OCULUS" ::
    "WEBVR_TIME_SPENT_VIEWING_IN_OPENVR" ::
    "WEBVR_USERS_VIEW_IN" :: Nil

  // The following user prefs will be included as top-level
  // fields, named according to UserPref.fieldName()
  //
  // Prefs where we only record whether a pref has been set should
  // use StringUserPref as we observe a value of "<user-set>".
  //
  // Supported pref data types are:
  //   nsIPrefBranch.PREF_STRING -> StringUserPref
  //   nsIPrefBranch.PREF_BOOL -> BooleanUserPref
  //   nsIPrefBranch.PREF_INT -> IntegerUserPref
  // See the `_getPrefData()` function in TelemetryEnvironment.jsm
  // for reference: https://mzl.la/2zo7kyK
  val userPrefsList =
    IntegerUserPref("dom.ipc.processCount") ::
    BooleanUserPref("extensions.allow-non-mpc-extensions") ::
    BooleanUserPref("extensions.legacy.enabled") ::
    BooleanUserPref("browser.search.widget.inNavBar") ::
    StringUserPref("general.config.filename") :: Nil


  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    val channel = opt[String]("channel", descr = "Only process data from the given channel", required = false)
    val appVersion = opt[String]("version", descr = "Only process data from the given app version", required = false)
    val allHistograms = opt[Boolean]("all-histograms", descr = "Flag to use all histograms", required = false)
    val docType = opt[String]("doc-type", descr = "DocType of pings conforming to main ping schema", required=false, default=Some("main"))
    // 500,000 rows yields ~ 200MB files in snappy+parquet
    val maxRecordsPerFile = opt[Int]("max-records-per-file", descr = "Max number of rows to write to output files before splitting", required = false, default=Some(500000))
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
    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val spark = getOrCreateSparkSession(jobName)
      implicit val sc = spark.sparkContext
      val sqlContext = spark.sqlContext
      val hadoopConf = sc.hadoopConfiguration
      hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")
      val filterChannel = conf.channel.get
      val filterVersion = conf.appVersion.get
      val filterDocType = conf.docType()

      println("=======================================================================================")
      println(s"BEGINNING JOB $jobName FOR $currentDateString")
      println(s" Filtering for docType = '${filterDocType}'")
      if (filterChannel.nonEmpty)
        println(s" Filtering for channel = '${filterChannel.get}'")
      if (filterVersion.nonEmpty)
        println(s" Filtering for version = '${filterVersion.get}'")

      val scalarDefinitions = Scalars.definitions(includeOptin = true).toList.sortBy(_._1)

      val histogramDefinitions = filterHistogramDefinitions(
        Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _, includeCategorical = true),
        useWhitelist = !conf.allHistograms())

      val schema = buildSchema(userPrefsList, scalarDefinitions, histogramDefinitions)
      val ignoredCount = sc.accumulator(0, "Number of Records Ignored")
      val processedCount = sc.accumulator(0, "Number of Records Processed")

      val telemetrySource = currentDate match {
        case d if d.isBefore(fmt.parseDateTime("20161012")) => "telemetry-oldinfra"
        case _ => "telemetry"
      }

      val messages = Dataset(telemetrySource)
        .where("sourceName") {
          case "telemetry" => true
        }.where("sourceVersion") {
          case "4" => true
        }.where("docType") {
          case dt => dt == filterDocType
        }.where("appName") {
          case "Firefox" => true
        }.where("submissionDate") {
          case date if date == currentDate.toString("yyyyMMdd") => true
        }.where("appUpdateChannel") {
          case channel => filterChannel.isEmpty || channel == filterChannel.get
        }.where("appVersion") {
          case v => filterVersion.isEmpty || v == filterVersion.get
        }.records(conf.limit.get)

      if(!messages.isEmpty()){
        val rowRDD = messages.flatMap(m => {
          messageToRow(m, scalarDefinitions, histogramDefinitions) match {
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
        val s3prefix = s"${filterDocType}_summary/$schemaVersion/submission_date_s3=$currentDateString"
        val s3path = s"s3://${conf.outputBucket()}/$s3prefix"

        // Repartition the dataframe by sample_id before saving.
        val partitioned = records.repartition(100, records.col("sample_id"))

        // limit the size of output files so they don't break during s3 upload
        val maxRecordsPerFile = conf.maxRecordsPerFile()

        // Then write to S3 using the given fields as path name partitions. Overwrites
        // existing data.
        partitioned.write.partitionBy("sample_id").mode("overwrite").option("maxRecordsPerFile", maxRecordsPerFile).parquet(s3path)

        // Then remove the _SUCCESS file so we don't break Spark partition discovery.
        S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")
      }

      println(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      println("     RECORDS SEEN:    %d".format(ignoredCount.value + processedCount.value))
      println("     RECORDS IGNORED: %d".format(ignoredCount.value))
      println("=======================================================================================")

      sc.stop()
    }
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
              addonData.isSystem.orNull,
              addonData.isWebExtension.orNull,
              addonData.multiprocessCompatible.orNull)
        }
        Some(rows.toList)
      }
      case _ => None
    }
  }

  def getDisabledAddons(activeAddons: JValue, addonDetails: JValue): Option[List[String]] = {
    // Get the list of ids from the active addons.
    val activeIds = activeAddons match {
      case JObject(addons) => addons.map(k => k._1)
      case _ => List()
    }
    // Only report the ids of the addons which are in the addonDetails but not in the activeAddons.
    // They are the disabled addons (possibly because they are legacy). We need this as addonDetails
    // may contain both disabled and active addons.
    addonDetails match {
      case JObject(addons) => Some(addons.map(k => k._1).filter(k => !activeIds.contains(k)))
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
          addonData.isSystem.orNull,
          addonData.isWebExtension.orNull,
          addonData.multiprocessCompatible.orNull))
      case _ => None
    }
  }

  def getQuantumReady(e10sStatus: JValue, addons: JValue, theme: JValue): Option[Boolean] = {
    val e10sEnabled = e10sStatus match {
      case JBool(x) => Some(x)
      case _ => None
    }

    val allowedAddons = getActiveAddons(addons) match {
      case Some(l) if l.nonEmpty => Some(
        l.map(row => {
          val isSystem = row.get(13) match {
            case b: Boolean => b
            case _ => false
          }

          val isWebExtension = row.get(14) match {
            case b: Boolean => b
            case _ => false
          }

          isSystem || isWebExtension
        }).reduce(_ && _))
      case Some(l) => Some(true) // no addons => quantumReady = true
      case _ => None
    }

    val requiredThemes = List(
      "{972ce4c6-7e08-4474-a285-3208198ce6fd}",
      "firefox-compact-light@mozilla.org",
      "firefox-compact-dark@mozilla.org"
    )

    val allowedTheme = getTheme(theme) match {
      case Some(t) => t.get(0) match {
        case id: String => id match {
            case "MISSING" => None
            case other => Some(requiredThemes.contains(id))
          }
        case _ => None
      }
      case _ => None
    }

    for {
      e10s <- e10sEnabled
      theme <- allowedTheme
      addons <- allowedAddons
    } yield (e10s && theme && addons)
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

  @deprecated
  def getOldUserPrefs(prefs: JValue): Option[Row] = {
    val pc = prefs \ "dom.ipc.processCount" match {
      case JInt(x) => x.toInt
      case _ => null
    }
    val anme = prefs \ "extensions.allow-non-mpc-extensions" match {
      case JBool(x) => x
      case _ => null
    }
    val row = Row(pc, anme)
    row match {
      case Row(null, null) => None
      case nonempty => Some(nonempty)
    }
  }

  def getUserPrefs(prefs: JValue, prefsList: List[UserPref]): Row = {
    val prefValues = prefsList.map(p => p.getValue(prefs \ p.name))
    Row.fromSeq(prefValues)
  }

  def getExperiments(jExperiments: JValue): Option[Map[String, String]] = {
    implicit val formats = DefaultFormats
    Try(jExperiments.extract[Map[String, Experiment]]) match {
      case Success(experiments) => {
        if (experiments.nonEmpty) {
          Some(experiments.map { case (id, data) => id -> data.branch.orNull })
        } else {
          None
        }
      }
      case _ => None
    }
  }

  // Convert the given Heka message containing a "main" ping
  // to a map containing just the fields we're interested in.
  def messageToRow(message: Message, scalarDefinitions: List[(String, ScalarDefinition)], histogramDefinitions: List[(String, HistogramDefinition)], userPrefs: List[UserPref] = userPrefsList): Option[Row] = {
    try {
      implicit val formats = DefaultFormats

      val doc = message.toJValue match {
        case Some(doc) => doc
        case None => return None
      }

      val environment = doc \ "environment"
      val payload = doc \ "payload"
      val meta = doc \ "meta"

      val addons = environment \ "addons"
      val addonDetails = payload \ "addonDetails"
      val application = doc \ "application"
      val build = environment \ "build"
      val experiments = environment \ "experiments"
      val profile = environment \ "profile"
      val partner = environment \ "partner"
      val settings = environment \ "settings"
      val system = environment \ "system"
      val info = payload \ "info"
      val simpleMeasures = payload \ "simpleMeasurements"

      val histograms = MainPing.ProcessTypes.map{
        _ match {
          case "parent" => "parent" -> payload \ "histograms"
          case p => p -> payload \ "processes" \ p \ "histograms"
        }
      }.toMap

      val keyedHistograms = MainPing.ProcessTypes.map{
        _ match {
          case "parent" => "parent" -> payload \ "keyedHistograms"
          case p => p -> payload \ "processes" \ p \ "keyedHistograms"
        }
      }.toMap

      val scalars = MainPing.ProcessTypes.map{
        p => p -> payload \ "processes" \ p \ "scalars"
      }.toMap

      val keyedScalars = MainPing.ProcessTypes.map{
        p => p -> payload \ "processes" \ p \ "keyedScalars"
      }.toMap

      val weaveConfigured = MainPing.booleanHistogramToBoolean(histograms("parent") \ "WEAVE_CONFIGURED")
      val weaveDesktop = MainPing.enumHistogramToCount(histograms("parent") \ "WEAVE_DEVICE_COUNT_DESKTOP")
      val weaveMobile = MainPing.enumHistogramToCount(histograms("parent") \ "WEAVE_DEVICE_COUNT_MOBILE")

      val events = ("dynamic" :: MainPing.ProcessTypes).map {
        p => p -> payload \ "processes" \ p \ "events"
      }

      val sslHandshakeResultKeys = (0 to 671).map(_.toString)

      // Messy list of known enum values for POPUP_NOTIFICATION_STATS.
      val popupNotificationStatsKeys = (0 to 8).union(10 to 11).union(20 to 28).union(30 to 31).map(_.toString)

      val pluginNotificationUserActionKeys = (0 to 2).map(_.toString)

      // Get the "sum" field from histogram h as an Int. Consider a
      // wonky histogram (one for which the "sum" field is not a
      // valid number) as null.
      @inline def hsum(h: JValue): Any = (h \ "sum").extractOpt[Int]

      val row = Row.fromSeq(Seq(
        // Row fields must match the structure in 'buildSchema'
        (meta \ "documentId").extractOpt[String],
        (meta \ "clientId").extractOpt[String],
        (meta \ "sampleId").extractOpt[Long],
        (meta \ "appUpdateChannel").extractOpt[String],
        (meta \ "normalizedChannel").extractOpt[String],
        (meta \ "geoCountry").extractOpt[String],
        (meta \ "geoCity").extractOpt[String],
        (system \ "os" \ "name").extractOpt[String],
        (system \ "os" \ "version").extractOpt[String],
        (system \ "os" \ "servicePackMajor").extractOpt[Long],
        (system \ "os" \ "servicePackMinor").extractOpt[Long],
        (system \ "os" \ "windowsBuildNumber").extractOpt[Long],
        (system \ "os" \ "windowsUBR").extractOpt[Long],
        (system \ "os" \ "installYear").extractOpt[Long],
        (system \ "isWow64").extractOpt[Boolean],
        (system \ "memoryMB").extractOpt[Int],
        (system \ "appleModelId").extractOpt[String],
        (system \ "sec" \ "antivirus").extract[Option[Seq[String]]],
        (system \ "sec" \ "antispyware").extract[Option[Seq[String]]],
        (system \ "sec" \ "firewall").extract[Option[Seq[String]]],
        (profile \ "creationDate").extractOpt[Long],
        (profile \ "resetDate").extractOpt[Long],
        (info \ "subsessionStartDate").extractOpt[String],
        (info \ "subsessionLength").extractOpt[Long],
        (info \ "subsessionCounter").extractOpt[Int],
        (info \ "profileSubsessionCounter").extractOpt[Int],
        (doc \ "creationDate").extractOpt[String],
        (partner \ "distributionId").extractOpt[String],
        (meta \ "submissionDate").extractOpt[String],
        weaveConfigured,
        weaveDesktop,
        weaveMobile,
        (application \ "buildId").extractOpt[String],
        (application \ "displayVersion").extractOpt[String],
        (application \ "name").extractOpt[String],
        (application \ "version").extractOpt[String],
        message.timestamp, // required
        (build \ "buildId").extractOpt[String],
        (build \ "version").extractOpt[String],
        (build \ "architecture").extractOpt[String],
        (settings \ "e10sEnabled").extractOpt[Boolean],
        (settings \ "e10sMultiProcesses").extractOpt[Long],
        (settings \ "locale").extractOpt[String],
        getAttribution(settings \ "attribution"),
        (addons \ "activeExperiment" \ "id").extractOpt[String],
        (addons \ "activeExperiment" \ "branch").extractOpt[String],
        (info \ "reason").extractOpt[String],
        (info \ "timezoneOffset").extractOpt[Int],
        hsum(keyedHistograms("parent") \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "pluginhang"),
        hsum(keyedHistograms("parent") \ "SUBPROCESS_ABNORMAL_ABORT" \ "plugin"),
        hsum(keyedHistograms("parent") \ "SUBPROCESS_ABNORMAL_ABORT" \ "content"),
        hsum(keyedHistograms("parent") \ "SUBPROCESS_ABNORMAL_ABORT" \ "gmplugin"),
        hsum(keyedHistograms("parent") \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "plugin"),
        hsum(keyedHistograms("parent") \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "content"),
        hsum(keyedHistograms("parent") \ "SUBPROCESS_CRASHES_WITH_DUMP" \ "gmplugin"),
        hsum(keyedHistograms("parent") \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "main-crash"),
        hsum(keyedHistograms("parent") \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "content-crash"),
        hsum(keyedHistograms("parent") \ "PROCESS_CRASH_SUBMIT_ATTEMPT" \ "plugin-crash"),
        hsum(keyedHistograms("parent") \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "main-crash"),
        hsum(keyedHistograms("parent") \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "content-crash"),
        hsum(keyedHistograms("parent") \ "PROCESS_CRASH_SUBMIT_SUCCESS" \ "plugin-crash"),
        hsum(keyedHistograms("parent") \ "SUBPROCESS_KILL_HARD" \ "ShutDownKill"),
        MainPing.countKeys(addons \ "activeAddons"),
        MainPing.getFlashVersion(addons),
        (application \ "vendor").extractOpt[String],
        (settings \ "isDefaultBrowser").extractOpt[Boolean],
        (settings \ "defaultSearchEngineData" \ "name").extractOpt[String],
        (settings \ "defaultSearchEngineData" \ "loadPath").extractOpt[String],
        (settings \ "defaultSearchEngineData" \ "origin").extractOpt[String],
        (settings \ "defaultSearchEngineData" \ "submissionURL").extractOpt[String],
        (settings \ "defaultSearchEngine").extractOpt[String],
        hsum(histograms("parent") \ "DEVTOOLS_TOOLBOX_OPENED_COUNT"),
        (meta \ "Date").extractOpt[String],
        MainPing.histogramToMean(histograms("parent") \ "PLACES_BOOKMARKS_COUNT"),
        MainPing.histogramToMean(histograms("parent") \ "PLACES_PAGES_COUNT"),
        hsum(histograms("parent") \ "PUSH_API_NOTIFY"),
        hsum(histograms("parent") \ "WEB_NOTIFICATION_SHOWN"),

        MainPing.keyedEnumHistogramToMap(keyedHistograms("parent") \ "POPUP_NOTIFICATION_STATS",
          popupNotificationStatsKeys),

        MainPing.getSearchCounts(keyedHistograms("parent") \ "SEARCH_COUNTS"),

        getActiveAddons(addons \ "activeAddons"),
        getDisabledAddons(addons \ "activeAddons", addonDetails \ "XPI"),
        getTheme(addons \ "theme"),
        (settings \ "blocklistEnabled").extractOpt[Boolean],
        (settings \ "addonCompatibilityCheckEnabled").extractOpt[Boolean],
        (settings \ "telemetryEnabled").extractOpt[Boolean],
        getOldUserPrefs(settings \ "userPrefs"),

        Option(events.flatMap {case (p, e) => Events.getEvents(e, p)}).filter(!_.isEmpty),

        // bug 1339655
        MainPing.enumHistogramBucketCount(histograms("parent") \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys.head),
        MainPing.enumHistogramSumCounts(histograms("parent") \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys.tail),
        MainPing.enumHistogramToMap(histograms("parent") \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys),

        // bug 1382002 - use scalar version when available.
        Try(MainPing.getScalarByName(scalars, scalarDefinitions, "scalar_parent_browser_engagement_active_ticks")) match {
          case Success(x: Integer) => x
          case _ => (simpleMeasures \ "activeTicks").extractOpt[Int]
        },

        // bug 1353114 - payload.simpleMeasurements.*
        (simpleMeasures \ "main").extractOpt[Int],

        // Use scalar version when available.
        Try(MainPing.getScalarByName(scalars, scalarDefinitions, "scalar_parent_timestamps_first_paint")) match {
          case Success(x: Integer) => x
          case _ => (simpleMeasures \ "firstPaint").extractOpt[Int]
        },

        // bug 1353114 - payload.simpleMeasurements.*
        (simpleMeasures \ "sessionRestored").extractOpt[Int],
        (simpleMeasures \ "totalTime").extractOpt[Int],

        // bug 1362520 - plugin notifications
        hsum(histograms("parent") \ "PLUGINS_NOTIFICATION_SHOWN"),
        MainPing.enumHistogramToRow(histograms("parent") \ "PLUGINS_NOTIFICATION_USER_ACTION", pluginNotificationUserActionKeys),
        hsum(histograms("parent") \ "PLUGINS_INFOBAR_SHOWN"),
        hsum(histograms("parent") \ "PLUGINS_INFOBAR_BLOCK"),
        hsum(histograms("parent") \ "PLUGINS_INFOBAR_ALLOW"),
        hsum(histograms("parent") \ "PLUGINS_INFOBAR_DISMISSED"),

        // bug 1366253 - active experiments
        getExperiments(experiments),

        (settings \ "searchCohort").extractOpt[String],

        // bug 1366838 - Quantum Release Criteria
        (system \ "gfx" \ "features" \ "compositor").extractOpt[String],

        getQuantumReady(
          settings \ "e10sEnabled",
          addons \ "activeAddons",
          addons \ "theme"
        ),

        MainPing.histogramToThresholdCount(histograms("parent") \ "GC_MAX_PAUSE_MS_2", 150),
        MainPing.histogramToThresholdCount(histograms("parent") \ "GC_MAX_PAUSE_MS_2", 250),
        MainPing.histogramToThresholdCount(histograms("parent") \ "GC_MAX_PAUSE_MS_2", 2500),

        MainPing.histogramToThresholdCount(histograms("content") \ "GC_MAX_PAUSE_MS_2", 150),
        MainPing.histogramToThresholdCount(histograms("content") \ "GC_MAX_PAUSE_MS_2", 250),
        MainPing.histogramToThresholdCount(histograms("content") \ "GC_MAX_PAUSE_MS_2", 2500),

        MainPing.histogramToThresholdCount(histograms("parent") \ "CYCLE_COLLECTOR_MAX_PAUSE", 150),
        MainPing.histogramToThresholdCount(histograms("parent") \ "CYCLE_COLLECTOR_MAX_PAUSE", 250),
        MainPing.histogramToThresholdCount(histograms("parent") \ "CYCLE_COLLECTOR_MAX_PAUSE", 2500),

        MainPing.histogramToThresholdCount(histograms("content") \ "CYCLE_COLLECTOR_MAX_PAUSE", 150),
        MainPing.histogramToThresholdCount(histograms("content") \ "CYCLE_COLLECTOR_MAX_PAUSE", 250),
        MainPing.histogramToThresholdCount(histograms("content") \ "CYCLE_COLLECTOR_MAX_PAUSE", 2500),

        MainPing.histogramToThresholdCount(histograms("parent") \ "INPUT_EVENT_RESPONSE_COALESCED_MS", 150),
        MainPing.histogramToThresholdCount(histograms("parent") \ "INPUT_EVENT_RESPONSE_COALESCED_MS", 250),
        MainPing.histogramToThresholdCount(histograms("parent") \ "INPUT_EVENT_RESPONSE_COALESCED_MS", 2500),

        MainPing.histogramToThresholdCount(histograms("content") \ "INPUT_EVENT_RESPONSE_COALESCED_MS", 150),
        MainPing.histogramToThresholdCount(histograms("content") \ "INPUT_EVENT_RESPONSE_COALESCED_MS", 250),
        MainPing.histogramToThresholdCount(histograms("content") \ "INPUT_EVENT_RESPONSE_COALESCED_MS", 2500),

        MainPing.histogramToThresholdCount(histograms("parent") \ "GHOST_WINDOWS", 1),
        MainPing.histogramToThresholdCount(histograms("content") \ "GHOST_WINDOWS", 1)
      ).map {
        _ match {
          case e: Option[Any] => e.orNull
          case o => o
        }
      })

      val userPrefsRow = getUserPrefs(settings \ "userPrefs", userPrefs)

      val scalarRow = MainPing.scalarsToRow(
        MainPing.ProcessTypes.map{ p => p -> (scalars(p) merge keyedScalars(p)) }.toMap,
        scalarDefinitions
      )

      val histogramRow = MainPing.histogramsToRow(
        MainPing.ProcessTypes.map{ p => p -> (histograms(p) merge keyedHistograms(p)) }.toMap,
        histogramDefinitions
      )

      Some(Row.merge(row, userPrefsRow, scalarRow, histogramRow))
    } catch {
      case e: Exception =>
        None
    }
  }

  // Type for encapsulating search counts
  def buildSearchSchema = StructType(List(
    StructField("engine", StringType, nullable = true), // Name of the search engine
    StructField("source", StringType, nullable = true), // Source of the search (urlbar, etc)
    StructField("count",  LongType,   nullable = true)  // Number of searches
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
      StructField("is_system",             BooleanType, nullable = true),
      StructField("is_web_extension",      BooleanType, nullable = true),
      StructField("multiprocess_compatible", BooleanType, nullable = true)
    ))

  def buildAttributionSchema = StructType(List(
    StructField("source",   StringType, nullable = true),
    StructField("medium",   StringType, nullable = true),
    StructField("campaign", StringType, nullable = true),
    StructField("content",  StringType, nullable = true)
  ))

  def buildOldUserPrefsSchema = StructType(List(
    StructField("dom_ipc_process_count", IntegerType, nullable = true), // dom.ipc.processCount
    StructField("extensions_allow_non_mpc_extensions", BooleanType, nullable = true) // extensions.allow-non-mpc-extensions
  ))

  // Bug 1390707 - Include pref fields as top-level fields to support schema evolution.
  def buildUserPrefsSchema(userPrefs: List[UserPref]) = StructType(
    userPrefs.map(p => p.asField())
  )

  def buildScalarSchema(scalarDefinitions: List[(String, ScalarDefinition)]): List[StructField] = {
    scalarDefinitions.map{
      case (name, definition) =>
        definition match {
          case UintScalar(keyed, _, _) => (name, keyed, IntegerType)
          case BooleanScalar(keyed, _, _) => (name, keyed, BooleanType)
          case StringScalar(keyed, _, _) => (name, keyed, StringType)
        }
    }.map{
      case (name, keyed, parquetType) =>
        keyed match {
          case true => StructField(name, MapType(StringType, parquetType), nullable = true)
          case false => StructField(name, parquetType, nullable = true)
        }
    }
  }

  def filterHistogramDefinitions(definitions: Map[String, HistogramDefinition], useWhitelist: Boolean = false): List[(String, HistogramDefinition)] = {
    definitions.toList.filter(_._2 match {
      case _: LinearHistogram => true
      case _: ExponentialHistogram => true
      case _: EnumeratedHistogram => true
      case _: BooleanHistogram => true
      case _: FlagHistogram => true // FIXME: hack to allow `A11Y_INSTANTIATED_FLAG` through until we can update it
      case _: CategoricalHistogram => true
      case _ => false
    }).filter(
      entry => !useWhitelist || histogramsWhitelist.contains(entry._2.originalName)
    ).sortBy(_._1)
  }

  val HistogramSchema = MapType(IntegerType, IntegerType, true)
  val CategoricalHistogramSchema = MapType(StringType, IntegerType, true)

  def buildHistogramSchema(histogramDefinitions: List[(String, HistogramDefinition)]): List[StructField] = {
    histogramDefinitions.map{
      case (name, definition) =>
        definition match {
          case _: CategoricalHistogram => (name, definition, CategoricalHistogramSchema)
          case _ => (name, definition, HistogramSchema)
        }
    }.map{
      case (name, definition, schemaType) =>
        definition.keyed match {
          case true => StructField(name, MapType(StringType, schemaType), nullable = true)
          case false => StructField(name, schemaType, nullable = true)
        }
    }
  }

  def buildPluginNotificationUserActionSchema = StructType(List(
    StructField("allow_now", IntegerType, nullable = true),
    StructField("allow_always", IntegerType, nullable = true),
    StructField("block", IntegerType, nullable = true)
  ))

  def buildSchema(userPrefs: List[UserPref], scalarDefinitions: List[(String, ScalarDefinition)], histogramDefinitions: List[(String, HistogramDefinition)]): StructType = {
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

      StructField("memory_mb", IntegerType, nullable = true), // environment/system/memoryMB
      StructField("apple_model_id", StringType, nullable = true), // environment/system/appleModelId

      // Bug 1431198 - Windows 8 only
      StructField("antivirus", ArrayType(StringType, containsNull=false), nullable=true), // environment/system/sec/antivirus
      StructField("antispyware", ArrayType(StringType, containsNull=false), nullable=true), // environment/system/sec/antispyware
      StructField("firewall", ArrayType(StringType, containsNull=false), nullable=true), // environment/system/sec/firewall

      // TODO: use proper 'date' type for date columns.
      StructField("profile_creation_date", LongType, nullable = true), // environment/profile/creationDate
      StructField("profile_reset_date", LongType, nullable = true), // environment/profile/resetDate
      StructField("subsession_start_date", StringType, nullable = true), // info/subsessionStartDate
      StructField("subsession_length", LongType, nullable = true), // info/subsessionLength
      StructField("subsession_counter", IntegerType, nullable = true), // info/subsessionCounter
      StructField("profile_subsession_counter", IntegerType, nullable = true), // info/profileSubsessionCounter
      StructField("creation_date", StringType, nullable = true), // creationDate
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

      // Bug 1406238
      StructField("e10s_multi_processes", LongType, nullable = true), // environment/settings/e10sMultiProcesses

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
      StructField("default_search_engine_data_load_path", StringType, nullable = true), // environment/settings/defaultSearchEngineData/loadPath
      StructField("default_search_engine_data_origin", StringType, nullable = true), // environment/settings/defaultSearchEngineData/origin
      StructField("default_search_engine_data_submission_url", StringType, nullable = true), // environment/settings/defaultSearchEngineData/submissionURL
      StructField("default_search_engine", StringType, nullable = true), // environment/settings/defaultSearchEngine

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
      // Legacy/disabled addon and configuration settings per Bug 1390814. Please note that |disabled_addons_ids|
      // may go away in the future.
      StructField("disabled_addons_ids", ArrayType(StringType, containsNull = false), nullable = true), // One per item in payload.addonDetails.XPI
      StructField("active_theme", buildAddonSchema, nullable = true), // environment.addons.theme
      StructField("blocklist_enabled", BooleanType, nullable = true), // environment.settings.blocklistEnabled
      StructField("addon_compatibility_check_enabled", BooleanType, nullable = true), // environment.settings.addonCompatibilityCheckEnabled
      StructField("telemetry_enabled", BooleanType, nullable = true), // environment.settings.telemetryEnabled

      // TODO: Deprecate and eventually remove this field, preferring the top-level
      //       user_pref_* fields for easy schema evolution.
      StructField("user_prefs", buildOldUserPrefsSchema, nullable = true), // environment.settings.userPrefs

      StructField("events", ArrayType(Events.buildEventSchema, containsNull = false), nullable = true), // payload.processes.parent.events

      // bug 1339655
      StructField("ssl_handshake_result_success", IntegerType, nullable = true),
      StructField("ssl_handshake_result_failure", IntegerType, nullable = true),
      StructField("ssl_handshake_result", MapType(StringType, IntegerType), nullable = true), // SSL_HANDSHAKE_RESULT

      // bug 1353114 - payload.simpleMeasurements.*
      StructField("active_ticks", IntegerType, nullable = true),
      StructField("main", IntegerType, nullable = true),
      StructField("first_paint", IntegerType, nullable = true),
      StructField("session_restored", IntegerType, nullable = true),
      StructField("total_time", IntegerType, nullable = true),

      // bug 1362520 - plugin notifications
      StructField("plugins_notification_shown", IntegerType, nullable = true),
      StructField("plugins_notification_user_action", buildPluginNotificationUserActionSchema, nullable = true),
      StructField("plugins_infobar_shown", IntegerType, nullable = true),
      StructField("plugins_infobar_block", IntegerType, nullable = true),
      StructField("plugins_infobar_allow", IntegerType, nullable = true),
      StructField("plugins_infobar_dismissed", IntegerType, nullable = true),

      // bug 1366253 - active experiments
      StructField("experiments", MapType(StringType, StringType), nullable = true), // experiment id->branchname

      StructField("search_cohort", StringType, nullable = true),

      // bug 1366838 - Quantum Release Criteria
      StructField("gfx_compositor", StringType, nullable = true),
      StructField("quantum_ready", BooleanType, nullable = true),

      StructField("gc_max_pause_ms_main_above_150", LongType, nullable = true),
      StructField("gc_max_pause_ms_main_above_250", LongType, nullable = true),
      StructField("gc_max_pause_ms_main_above_2500", LongType, nullable = true),

      StructField("gc_max_pause_ms_content_above_150", LongType, nullable = true),
      StructField("gc_max_pause_ms_content_above_250", LongType, nullable = true),
      StructField("gc_max_pause_ms_content_above_2500", LongType, nullable = true),

      StructField("cycle_collector_max_pause_main_above_150", LongType, nullable = true),
      StructField("cycle_collector_max_pause_main_above_250", LongType, nullable = true),
      StructField("cycle_collector_max_pause_main_above_2500", LongType, nullable = true),

      StructField("cycle_collector_max_pause_content_above_150", LongType, nullable = true),
      StructField("cycle_collector_max_pause_content_above_250", LongType, nullable = true),
      StructField("cycle_collector_max_pause_content_above_2500", LongType, nullable = true),

      StructField("input_event_response_coalesced_ms_main_above_150", LongType, nullable = true),
      StructField("input_event_response_coalesced_ms_main_above_250", LongType, nullable = true),
      StructField("input_event_response_coalesced_ms_main_above_2500", LongType, nullable = true),

      StructField("input_event_response_coalesced_ms_content_above_150", LongType, nullable = true),
      StructField("input_event_response_coalesced_ms_content_above_250", LongType, nullable = true),
      StructField("input_event_response_coalesced_ms_content_above_2500", LongType, nullable = true),

      StructField("ghost_windows_main_above_1", LongType, nullable = true),
      StructField("ghost_windows_content_above_1", LongType, nullable = true)
    ) ++ buildUserPrefsSchema(userPrefs)
      ++ buildScalarSchema(scalarDefinitions)
      ++ buildHistogramSchema(histogramDefinitions))
  }
}
