package streams

import awscala.s3.Bucket
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import org.json4s.JsonAST.{JInt, JNothing, JObject, JString, JValue, JBool}
import org.json4s.jackson.JsonMethods.parse
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.heka.{HekaFrame, Message}
import telemetry.parquet.ParquetFile

case class MainSummary(prefix: String) extends DerivedStream{
  override def filterPrefix: String = prefix
  override def streamName: String = "telemetry"
  def streamVersion: String = "v1"

  // Convert the given Heka message containing a "main" ping
  // to a map containing just the fields we're interested in.
  def messageToMap(message: Message): Option[Map[String,Any]] = {
    val fields = HekaFrame.fields(message)

    // Don't compute the expensive stuff until we need it. We may skip a record
    // due to missing simple fields.
    lazy val application = parse(fields.getOrElse("application", "{}").asInstanceOf[String])
    lazy val build = parse(fields.getOrElse("environment.build", "{}").asInstanceOf[String])
    lazy val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
    lazy val partner = parse(fields.getOrElse("environment.partner", "{}").asInstanceOf[String])
    lazy val settings = parse(fields.getOrElse("environment.settings", "{}").asInstanceOf[String])
    lazy val system = parse(fields.getOrElse("environment.system", "{}").asInstanceOf[String])
    lazy val info = parse(fields.getOrElse("payload.info", "{}").asInstanceOf[String])
    lazy val histograms = parse(fields.getOrElse("payload.histograms", "{}").asInstanceOf[String])

    lazy val weaveConfigured = booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
    lazy val weaveDesktop = enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
    lazy val weaveMobile = enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")

    val map = Map[String, Any](
        "documentId" -> (fields.getOrElse("documentId", None) match {
          case x: String => x
          // documentId is required, and must be a string. If either
          // condition is not satisfied, we skip this record.
          case _ => return None
        }),
        "clientId" -> (fields.getOrElse("clientId", None) match {
          case x: String => x
          case _ => return None // required
        }),
        "sampleId" -> (fields.getOrElse("sampleId", None) match {
          case x: Long => x
          case x: Double => x.toLong
          case _ => return None // required
        }),
        "submissionDate" -> (fields.getOrElse("submissionDate", None) match {
          case x: String => x
          case _ => return None // required
        }),
        "timestamp" -> message.timestamp, // required
        "channel" -> (fields.getOrElse("appUpdateChannel", None) match {
          case x: String => x
          case _ => ""
        }),
        "normalizedChannel" -> (fields.getOrElse("normalizedChannel", None) match {
          case x: String => x
          case _ => ""
        }),
        "country" -> (fields.getOrElse("geoCountry", None) match {
          case x: String => x
          case _ => ""
        }),
        "version" -> (fields.getOrElse("appVersion", None) match {
          case x: String => x
          case _ => ""
        }),
        "profileCreationDate" -> ((profile \ "creationDate") match {
          case x: JInt => x.num.toLong
          case _ => null
        }),
        "syncConfigured" -> weaveConfigured.getOrElse(null),
        "syncCountDesktop" -> weaveDesktop.getOrElse(null),
        "syncCountMobile" -> weaveMobile.getOrElse(null),
        "subsessionStartDate" -> ((info \ "subsessionStartDate") match {
          case JString(x) => x
          case _ => null
        }),
        "subsessionLength" -> ((info \ "subsessionLength") match {
          case x: JInt => x.num.toLong
          case _ => null
        }),
        "distributionId" -> ((partner \ "distributionId") match {
          case JString(x) => x
          case _ => null
        }),
        "e10sEnabled" -> ((settings \ "e10sEnabled") match {
          case JBool(x) => x
          case _ => null
        }),
        "e10sCohort" -> ((settings \ "e10sCohort") match {
          case JString(x) => x
          case _ => null
        }),
        "os" -> ((system \ "os" \ "name") match {
          case JString(x) => x
          case _ => null
        }),
        "osVersion" -> ((system \ "os" \ "version") match {
          case JString(x) => x
          case _ => null
        }),
        "osServicepackMajor" -> ((system \ "os" \ "servicePackMajor") match {
          case JString(x) => x
          case _ => null
        }),
        "osServicepackMinor" -> ((system \ "os" \ "servicePackMinor") match {
          case JString(x) => x
          case _ => null
        }),
        "appBuildId" -> ((application \ "buildId") match {
          case JString(x) => x
          case _ => null
        }),
        "appDisplayVersion" -> ((application \ "displayVersion") match {
          case JString(x) => x
          case _ => null
        }),
        "appName" -> ((application \ "name") match {
          case JString(x) => x
          case _ => null
        }),
        "appVersion" -> ((application \ "version") match {
          case JString(x) => x
          case _ => null
        }),
        "envBuildId" -> ((build \ "buildId") match {
          case JString(x) => x
          case _ => null
        }),
        "envBuildVersion" -> ((build \ "version") match {
          case JString(x) => x
          case _ => null
        }),
        "envBuildArch" -> ((build \ "architecture") match {
          case JString(x) => x
          case _ => null
        })
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
      val churnMessages = sc.parallelize(groups, groups.size)
        .flatMap(x => x)
        .flatMap{ case obj =>
          val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3: " + obj.key))
          for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message }
        .flatMap{ case message => messageToMap(message) }
        .repartition(100) // TODO: partition by sampleId
        .foreachPartition{ case partitionIterator =>
          val schema = buildSchema
          val records = for {
            record <- partitionIterator
            saveable <- buildRecord(record, schema)
          } yield saveable

          while(!records.isEmpty) {
            val localFile = ParquetFile.serialize(records, schema)
            uploadLocalFileToS3(localFile, s"$streamVersion/submission_date_s3=$currentDay")
          }
        }
    }
  }

  private def buildSchema: Schema = {
    // Type for encapsulating search counts
    val searchCountsType = SchemaBuilder
      .record("SearchCounts").fields()
      .name("engine").`type`().stringType().noDefault() // Name of the search engine
      .name("source").`type`().stringType().noDefault() // Source of the search (urlbar, etc)
      .name("count").`type`().longType().noDefault() // Number of searches
      .endRecord()

    SchemaBuilder
      .record("MainSummary").fields
      .name("documentId").`type`().stringType().noDefault() // id
      .name("clientId").`type`().stringType().noDefault() // clientId
      .name("sampleId").`type`().intType().noDefault() // Fields[sampleId]
      .name("channel").`type`().stringType().noDefault() // appUpdateChannel
      .name("normalizedChannel").`type`().stringType().noDefault() // normalizedChannel
      .name("country").`type`().stringType().noDefault() // geoCountry
      .name("os").`type`().stringType().noDefault() // environment/system/os/name
      .name("osVersion").`type`().nullable().stringType().noDefault() // environment/system/os/version
      .name("osServicepackMajor").`type`().nullable().stringType().noDefault() // environment/system/os/servicePackMajor
      .name("osServicepackMinor").`type`().nullable().stringType().noDefault() // environment/system/os/servicePackMinor

      // TODO: use proper 'date' type for date columns.
      .name("profileCreationDate").`type`().nullable().intType().noDefault() // environment/profile/creationDate
      .name("subsessionStartDate").`type`().nullable().stringType().noDefault() // info/subsessionStartDate
      .name("subsessionLength").`type`().nullable().intType().noDefault() // info/subsessionLength
      .name("distributionId").`type`().nullable().stringType().noDefault() // environment/partner/distributionId
      .name("submissionDate").`type`().stringType().noDefault()
      // See bug 1232050
      .name("syncConfigured").`type`().nullable().booleanType().noDefault() // WEAVE_CONFIGURED
      .name("syncCountDesktop").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_DESKTOP
      .name("syncCountMobile").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_MOBILE
      .name("appBuildId").`type`().stringType().noDefault() // application/buildId
      .name("appDisplayVersion").`type`().nullable().stringType().noDefault() // application/displayVersion
      .name("appName").`type`().stringType().noDefault() // application/name
      .name("appVersion").`type`().stringType().noDefault() // application/version
      .name("timestamp").`type`().longType().noDefault() // server-assigned timestamp when record was received

      .name("envBuildId").`type`().nullable().stringType().noDefault() // environment/build/buildId
      .name("envBuildVersion").`type`().nullable().stringType().noDefault() // environment/build/version
      .name("envBuildArch").`type`().nullable().stringType().noDefault() // environment/build/architecture

      // See bug 1251259
      .name("e10sEnabled").`type`().nullable().booleanType().noDefault() // environment/settings/e10sEnabled
      .name("e10sCohort").`type`().nullable().stringType().noDefault() // environment/settings/e10sCohort
      .name("locale").`type`().nullable().stringType().noDefault() // environment/settings/locale

      .name("activeExperimentId").`type`().nullable().stringType().noDefault() // environment/addons/activeExperiment/id
      .name("activeExperimentBranch").`type`().nullable().stringType().noDefault() // environment/addons/activeExperiment/branch
      .name("reason").`type`().nullable().stringType().noDefault() // info/reason

      .name("timezoneOffset").`type`().nullable().intType().noDefault() // info/timezoneOffset

      // Different types of hangs:
      .name("pluginHangs").`type`().nullable().intType().noDefault() // SUBPROCESS_CRASHES_WITH_DUMP / pluginhang
      .name("abortsPlugin").`type`().nullable().intType().noDefault() // SUBPROCESS_ABNORMAL_ABORT / plugin
      .name("abortsContent").`type`().nullable().intType().noDefault() // SUBPROCESS_ABNORMAL_ABORT / content
      .name("abortsGmplugin").`type`().nullable().intType().noDefault() // SUBPROCESS_ABNORMAL_ABORT / gmplugin
      .name("crashesdetectedPlugin").`type`().nullable().intType().noDefault() // SUBPROCESS_CRASHES_WITH_DUMP / plugin
      .name("crashesdetectedContent").`type`().nullable().intType().noDefault() // SUBPROCESS_CRASHES_WITH_DUMP / content
      .name("crashesdetectedGmplugin").`type`().nullable().intType().noDefault() // SUBPROCESS_CRASHES_WITH_DUMP / gmplugin
      .name("crashSubmitAttemptMain").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_ATTEMPT / main-crash
      .name("crashSubmitAttemptContent").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_ATTEMPT / content-crash
      .name("crashSubmitAttemptPlugin").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_ATTEMPT / plugin-crash
      .name("crashSubmitSuccessMain").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_SUCCESS / main-crash
      .name("crashSubmitSuccessContent").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_SUCCESS / content-crash
      .name("crashSubmitSuccessPlugin").`type`().nullable().intType().noDefault() // PROCESS_CRASH_SUBMIT_SUCCESS / plugin-crash


      .name("activeAddons").`type`().nullable().intType().noDefault() // number of keys in environment/addons/activeAddons

      // See https://github.com/mozilla-services/data-pipeline/blob/master/hindsight/modules/fx/ping.lua#L82
      .name("flashVersion").`type`().nullable().stringType().noDefault() // latest installable version of flash plugin.
      .name("vendor").`type`().nullable().stringType().noDefault() // info/vendor
      .name("isDefaultBrowser").`type`().nullable().booleanType().noDefault() // environment/settings/isDefaultBrowser
      .name("defaultSearchEngineDataName").`type`().nullable().stringType().noDefault() // environment/settings/defaultSearchEngineData/name

      // Search counts
      .name("searchCounts").`type`().optional().array().items(searchCountsType) // split up and organize the SEARCH_COUNTS keyed histogram
      .endRecord
  }

  // Check if a json value contains a number greater than zero.
  def gtZero(v: JValue): Boolean = {
    v match {
      case x: JInt => x.num.toInt > 0
      case _ => false
    }
  }

  // Given histogram h, return true if it has a value in the "true" bucket,
  // or false if it has a value in the "false" bucket, or None otherwise.
  def booleanHistogramToBoolean(h: JValue): Option[Boolean] = {
    (gtZero(h \ "values" \ "1"), gtZero(h \ "values" \ "0")) match {
      case (true, _) => Some(true)
      case (_, true) => Some(false)
      case _ => None
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  // Find the largest numeric bucket that contains a value greater than zero.
  def enumHistogramToCount(h: JValue): Option[Long] = {
    (h \ "values") match {
      case JNothing => None
      case JObject(x) => {
        var topBucket = -1
        for {
          (k, v) <- x
          b <- toInt(k) if b > topBucket && gtZero(v)
        } topBucket = b

        if (topBucket >= 0) {
          Some(topBucket)
        } else {
          None
        }
      }
      case _ => {
        None
      }
    }
  }

  def buildRecord(fields: Map[String,Any], schema: Schema): Option[GenericRecord] ={
    val root = new GenericRecordBuilder(schema)
    for ((k, v) <- fields) root.set(k, v)
    Some(root.build)
  }
}
