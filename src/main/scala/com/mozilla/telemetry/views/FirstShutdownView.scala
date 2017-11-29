package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, Days, format}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, JValue}
import org.rogach.scallop._

import scala.util.{Success, Try}

object FirstShutdownView {

  def schemaVersion: String = "v4"
  def jobName: String = "first_shutdown"

  val histogramsWhitelist = MainSummaryView.histogramsWhitelist
  val userPrefsList = MainSummaryView.userPrefsList

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    val channel = opt[String]("channel", descr = "Only process data from the given channel", required = false)
    val appVersion = opt[String]("version", descr = "Only process data from the given app version", required = false)
    val allHistograms = opt[Boolean]("all-histograms", descr = "Flag to use all histograms", required = false)
    val docType = opt[String]("doc-type", descr = "DocType of pings conforming to main ping schema", required=false, default=Some("first_shutdown"))
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

      val histogramDefinitions = MainSummaryView.filterHistogramDefinitions(
        Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _, includeCategorical = true),
        useWhitelist = !conf.allHistograms())

      val schema = MainSummaryView.buildSchema(userPrefsList, scalarDefinitions, histogramDefinitions)
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
          messageToRow(m, userPrefsList, scalarDefinitions, histogramDefinitions) match {
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

        // Then write to S3 using the given fields as path name partitions. Overwrites
        // existing data.
        partitioned.write.partitionBy("sample_id").mode("overwrite").parquet(s3path)

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

  def asInt(v: JValue): Integer = v match {
    case JInt(x) => x.toInt
    case _ => null
  }

  // Convert the given Heka message containing a "main" ping
  // to a map containing just the fields we're interested in.
  def messageToRow(message: Message, userPrefs: List[UserPref], scalarDefinitions: List[(String, ScalarDefinition)], histogramDefinitions: List[(String, HistogramDefinition)]): Option[Row] = {
    try {
      val fields = message.fieldsAsMap

      // Don't compute the expensive stuff until we need it. We may skip a record
      // due to missing required fields.
      lazy val submission = parse(message.payload.getOrElse(fields.getOrElse("submission", "{}")).asInstanceOf[String])

      lazy val addons = parse(fields.getOrElse("environment.addons", "{}").asInstanceOf[String])
      lazy val addonDetails = submission \ "payload" \ "addonDetails"
      lazy val application = submission \ "application"
      lazy val build = parse(fields.getOrElse("environment.build", "{}").asInstanceOf[String])
      lazy val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
      lazy val partner = parse(fields.getOrElse("environment.partner", "{}").asInstanceOf[String])
      lazy val settings = parse(fields.getOrElse("environment.settings", "{}").asInstanceOf[String])
      lazy val system = parse(fields.getOrElse("environment.system", "{}").asInstanceOf[String])
      lazy val info = submission \ "payload" \ "info"
      lazy val simpleMeasures = submission \ "payload" \ "simpleMeasurements"

      lazy val histograms = MainPing.ProcessTypes.map{
        _ match {
          case "parent" => "parent" -> submission \ "payload" \ "histograms"
          case p => p -> submission \ "payload" \ "processes" \ p \ "histograms"
        }
      }.toMap

      lazy val keyedHistograms = MainPing.ProcessTypes.map{
        _ match {
          case "parent" => "parent" -> submission \ "payload" \ "keyedHistograms"
          case p => p -> submission \ "payload" \ "processes" \ p \ "keyedHistograms"
        }
      }.toMap

      lazy val scalars = MainPing.ProcessTypes.map{
        p => p -> submission \ "payload" \ "processes" \ p \ "scalars"
      }.toMap

      lazy val keyedScalars = MainPing.ProcessTypes.map{
        p => p -> submission \ "payload" \ "processes" \ p \ "keyedScalars"
      }.toMap

      lazy val weaveConfigured = MainPing.booleanHistogramToBoolean(histograms("parent") \ "WEAVE_CONFIGURED")
      lazy val weaveDesktop = MainPing.enumHistogramToCount(histograms("parent") \ "WEAVE_DEVICE_COUNT_DESKTOP")
      lazy val weaveMobile = MainPing.enumHistogramToCount(histograms("parent") \ "WEAVE_DEVICE_COUNT_MOBILE")

      lazy val events = ("dynamic" :: MainPing.ProcessTypes).map {
        p => p -> submission \ "payload" \ "processes" \ p \ "events"
      }

      lazy val experiments = parse(fields.getOrElse("environment.experiments", "{}").asInstanceOf[String])

      val sslHandshakeResultKeys = (0 to 671).map(_.toString)

      // Messy list of known enum values for POPUP_NOTIFICATION_STATS.
      val popupNotificationStatsKeys = (0 to 8).union(10 to 11).union(20 to 28).union(30 to 31).map(_.toString)

      val pluginNotificationUserActionKeys = (0 to 2).map(_.toString)

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
        system \ "memoryMB" match {
          case JInt(x) => x.toInt
          case _ => null
        },
        system \ "appleModelId" match {
          case JString(x) => x
          case _ => null
        },
        profile \ "creationDate" match {
          case JInt(x) => x.toLong
          case _ => null
        },
        profile \ "resetDate" match {
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
        info \ "subsessionCounter" match {
          case JInt(x) => x.toInt
          case _ => null
        },
        info \ "profileSubsessionCounter" match {
          case JInt(x) => x.toInt
          case _ => null
        },
        submission \ "creationDate" match {
          case JString(x) => x
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
        settings \ "e10sMultiProcesses" match {
          case JInt(x) => x.toLong
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
        settings \ "defaultSearchEngineData" \ "loadPath" match {
          case JString(x) => x
          case _ => null
        },
        settings \ "defaultSearchEngineData" \ "origin" match {
          case JString(x) => x
          case _ => null
        },
        settings \ "defaultSearchEngineData" \ "submissionURL" match {
          case JString(x) => x
          case _ => null
        },
        settings \ "defaultSearchEngine" match {
          case JString(x) => x
          case _ => null
        },
        hsum(histograms("parent") \ "DEVTOOLS_TOOLBOX_OPENED_COUNT"),
        fields.getOrElse("Date", None) match {
          case x: String => x
          case _ => null
        },
        MainPing.histogramToMean(histograms("parent") \ "PLACES_BOOKMARKS_COUNT").orNull,
        MainPing.histogramToMean(histograms("parent") \ "PLACES_PAGES_COUNT").orNull,
        hsum(histograms("parent") \ "PUSH_API_NOTIFY"),
        hsum(histograms("parent") \ "WEB_NOTIFICATION_SHOWN"),

        MainPing.keyedEnumHistogramToMap(keyedHistograms("parent") \ "POPUP_NOTIFICATION_STATS",
          popupNotificationStatsKeys).orNull,

        MainPing.getSearchCounts(keyedHistograms("parent") \ "SEARCH_COUNTS").orNull,

        getActiveAddons(addons \ "activeAddons").orNull,
        getDisabledAddons(addons \ "activeAddons", addonDetails \ "XPI").orNull,
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
        getOldUserPrefs(settings \ "userPrefs").orNull,

        Option(events.flatMap {case (p, e) => Events.getEvents(e, p)}).filter(!_.isEmpty).orNull,

        // bug 1339655
        MainPing.enumHistogramBucketCount(histograms("parent") \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys.head).orNull,
        MainPing.enumHistogramSumCounts(histograms("parent") \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys.tail),
        MainPing.enumHistogramToMap(histograms("parent") \ "SSL_HANDSHAKE_RESULT", sslHandshakeResultKeys),

        // bug 1382002 - use scalar version when available.
        Try(MainPing.getScalarByName(scalars, scalarDefinitions, "scalar_parent_browser_engagement_active_ticks")) match {
          case Success(x: Integer) => x
          case _ => asInt (simpleMeasures \ "activeTicks")
        },

        // bug 1353114 - payload.simpleMeasurements.*
        asInt(simpleMeasures \ "main"),

        // Use scalar version when available.
        Try(MainPing.getScalarByName(scalars, scalarDefinitions, "scalar_parent_timestamps_first_paint")) match {
          case Success(x: Integer) => x
          case _ => asInt (simpleMeasures \ "firstPaint")
        },

        // bug 1353114 - payload.simpleMeasurements.*
        asInt(simpleMeasures \ "sessionRestored"),
        asInt(simpleMeasures \ "totalTime"),

        // bug 1362520 - plugin notifications
        hsum(histograms("parent") \ "PLUGINS_NOTIFICATION_SHOWN"),
        MainPing.enumHistogramToRow(histograms("parent") \ "PLUGINS_NOTIFICATION_USER_ACTION", pluginNotificationUserActionKeys),
        hsum(histograms("parent") \ "PLUGINS_INFOBAR_SHOWN"),
        hsum(histograms("parent") \ "PLUGINS_INFOBAR_BLOCK"),
        hsum(histograms("parent") \ "PLUGINS_INFOBAR_ALLOW"),
        hsum(histograms("parent") \ "PLUGINS_INFOBAR_DISMISSED"),

        // bug 1366253 - active experiments
        getExperiments(experiments).orNull,

        settings \ "searchCohort" match {
          case JString(x) => x
          case _ => null
        },

        // bug 1366838 - Quantum Release Criteria
        system \ "gfx" \ "features" \ "compositor" match {
          case JString(x) => x
          case _ => null
        },

        getQuantumReady(
          settings \ "e10sEnabled",
          addons \ "activeAddons",
          addons \ "theme"
        ).orNull,

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
      )

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
}
