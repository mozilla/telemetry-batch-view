package com.mozilla.telemetry.views

import com.github.nscala_time.time.Imports._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.math.abs
import scala.reflect.ClassTag
import com.mozilla.telemetry.parquet.ParquetFile
import com.mozilla.telemetry.avro
import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils._

protected class ClientIterator(it: Iterator[(String, Map[String, Any])], maxHistorySize: Int = 1000) extends Iterator[List[Map[String, Any]]] {
  // Less than 1% of clients in a sampled dataset over a 3 months period has more than 1000 fragments.
  var buffer = ListBuffer[Map[String, Any]]()
  var currentKey =
    if (it.hasNext) {
      val (key, value) = it.next()
      buffer += value
      key
    } else {
      null
    }

  override def hasNext() = buffer.nonEmpty

  override def next(): List[Map[String, Any]] = {
    while (it.hasNext) {
      val (key, value) = it.next()

      if (key == currentKey && buffer.size == maxHistorySize) {
        // Trim long histories
        val result = buffer.toList
        buffer.clear

        // Fast forward to next client
        while (it.hasNext) {
          val (k, v) = it.next()
          if (k != currentKey) {
            buffer += v
            currentKey = k
            return result
          }
        }

        return result
      } else if (key == currentKey) {
        buffer += value
      } else {
        val result = buffer.toList
        buffer.clear
        buffer += value
        currentKey = key
        return result
      }
    }

    val result = buffer.toList
    buffer.clear
    result
  }
}

object LongitudinalView {
  val jobName = "longitudinal"

  // Column name prefix used to prevent name clashing between scalars and other
  // columns.
  private val ScalarColumnNamePrefix = "scalar_"

  // In theory, we shouldn't really need to define this types. However, on Spark 2.0
  // and Avro 1.7.x this is the only sane way to have Arrays with null elements.
  // See https://github.com/mozilla/telemetry-batch-view/pull/124#issuecomment-254218256
  // for context.
  private val UintScalarSchema = SchemaBuilder
    .record("uint_scalar").fields()
      .name("value").`type`().optional().longType()
    .endRecord()

  private val BoolScalarSchema = SchemaBuilder
    .record("bool_scalar").fields()
      .name("value").`type`().optional().booleanType()
    .endRecord()

  private val StringScalarSchema = SchemaBuilder
    .record("string_scalar").fields()
      .name("value").`type`().optional().stringType()
    .endRecord()

  private class ClientIdPartitioner(size: Int) extends Partitioner {
    def numPartitions: Int = size

    def getPartition(key: Any): Int = key match {
      case (clientId: String, startDate: String, counter: Int) => abs(clientId.hashCode) % size
      case _ => throw new Exception("Invalid key")
    }
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date. Defaults to 6 months before `to`.", required = false)
    val to = opt[String]("to", descr = "To submission date", required = true)
    val outputBucket = opt[String]("bucket", descr = "bucket", required = true)
    val includeOptin = opt[String]("optin-only", descr = "Boolean, whether to include opt-in data only. Defaults to false.", required = false)
    val channels = opt[String]("channels", descr = "Comma separated string, which channels to include. Defaults to all channels.", required = false)
    val samples = opt[String]("samples", descr = "Comma separated string, which samples to include. Defaults to 42.", required = false)
    val telemetrySource = opt[String]("source", descr = "Source for Dataset.from_source. Defaults to telemetry-sample.", required = false)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val fmt = DateTimeFormat.forPattern("yyyyMMdd")

    val to = opts.to()
    val from = opts.from.get match {
      case Some(f) => f
      case _ => fmt.print(fmt.parseDateTime(to).minusMonths(6))
    }

    val channels = opts.channels.get.map(_.split(","))

    val samples = opts.samples.get match {
        case Some(s) => s.split(",")
        case _ => Array("42")
    }

    val telemetrySource = opts.telemetrySource.get match {
        case Some(ts) => ts
        case _ => "telemetry-sample"
    }

    val spark = getOrCreateSparkSession(jobName)
    implicit val sc = spark.sparkContext

    val messages = Dataset(telemetrySource)
      .where("sourceName") {
        case "telemetry" => true
      }.where("sourceVersion") {
        case "4" => true
      }.where("docType") {
        case "main" => true
      }.where("submissionDate") {
        case date if date <= to && date >= from => true
      }.where("sampleId") {
        case sample if samples.contains(sample) => true
      }.where("appUpdateChannel") {
        case channel if channels.map(_.contains(channel)).getOrElse(true) => true
      }

    run(opts: Opts, messages)
    sc.stop()
  }

  private def run(opts: Opts, messages: RDD[Message]): Unit = {
    val prefix = s"${jobName}/v${opts.to()}"
    val outputBucket = opts.outputBucket()
    val lockfileName = "RUNNING"

    val includeOptin = opts.includeOptin.get match {
        case Some(v) => v.toBoolean // fail loudly on invalid string
        case _ => false
    }

    require(S3Store.isPrefixEmpty(outputBucket, prefix), s"s3://${outputBucket}/${prefix} already exists!")

    // Bug 1321424 -- add a lockfile into the folder to prevent duplicate jobs from running
    val lockfile = new java.io.File(lockfileName)
    lockfile.createNewFile()
    S3Store.uploadFile(lockfile, outputBucket, prefix, lockfileName)

    // Sort submissions in descending order
    implicit val ordering = Ordering[(String, String, Int)].reverse
    val clientMessages = messages
      .flatMap {
        (message) =>
          val fields = message.fieldsAsMap
          val payload = message.payload.getOrElse(fields.getOrElse("submission", "{}")) match {
            case p: String => parse(p) \ "payload"
            case _ => JObject()
          }
          for {
            clientId <- fields.get("clientId").asInstanceOf[Option[String]]
            json <- fields.get("payload.info").asInstanceOf[Option[String]]

            info = parse(json)
            JString(startDate) <- (info \ "subsessionStartDate").toOption
            JInt(counter) <- (info \ "profileSubsessionCounter").toOption
          } yield ((clientId, startDate, counter.toInt),
                   fields + ("payload" -> compact(render(payload))) - "submission")
      }
      .repartitionAndSortWithinPartitions(new ClientIdPartitioner(480))
      .map { case (key, value) => (key._1, value) }

    /* One file per partition is generated at the end of the job. We want to have
       few big files but at the same time we need a high enough degree of parallelism
       to keep all workers busy. Since the cluster typically used for this job has
       8 workers and 20 executors, 320 partitions provide a good compromise. */

    val histogramDefinitions = Histograms.definitions(includeOptin)

    // Only show parent scalars
    val scalarDefinitions = Scalars.definitions(includeOptin).filter(_._2.process == Some("parent"))

    val partitionCounts = clientMessages
      .mapPartitions{ case it =>
        val clientIterator = new ClientIterator(it)
        val schema = buildSchema(histogramDefinitions, scalarDefinitions)

        val allRecords = for {
          client <- clientIterator
          record = buildRecord(client, schema, histogramDefinitions, scalarDefinitions)
        } yield record

        var ignoredCount = 0
        var processedCount = 0
        val records = allRecords.map(r => r match {
          case Some(record) =>
            processedCount += 1
            r
          case None =>
            ignoredCount += 1
            r
        }).flatten

        while(records.nonEmpty) {
          // Block size has to be increased to pack more than a couple hundred profiles
          // within the same row group.
          val localFile = new java.io.File(ParquetFile.serialize(records, schema, 8).toUri())
          S3Store.uploadFile(localFile, outputBucket, prefix)
          localFile.delete()
        }

        List((processedCount + ignoredCount, ignoredCount)).toIterator
      }

    val counts = partitionCounts.reduce( (x, y) => (x._1 + y._1, x._2 + y._2))
    println("Clients seen: %d".format(counts._1))
    println("Clients ignored: %d".format(counts._2))

    // NOTE: This line must be after the reduce above due to lazy eval, or else this will execute before any real work
    S3Store.deleteKey(outputBucket,  s"${prefix}/${lockfileName}")
  }

  private def buildSchema(histogramDefinitions: Map[String, HistogramDefinition], scalarDefinitions: Map[String, ScalarDefinition]): Schema = {
    // The $PROJECT_ROOT/scripts/generate-ping-schema.py script can be used to
    // generate these SchemaBuilder definitions, and the output of that script is
    // combined with data from https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/telemetry
    // to get the results below. See the schemas at https://github.com/mozilla-services/mozilla-pipeline-schemas/

    // Note that field names have to be defined with underscore_notation to allow
    // the dataset to be read from Presto.
    val buildType = SchemaBuilder
      .record("build").fields()
        .name("application_id").`type`().optional().stringType()
        .name("application_name").`type`().optional().stringType()
        .name("architecture").`type`().optional().stringType()
        .name("architectures_in_binary").`type`().optional().stringType()
        .name("build_id").`type`().optional().stringType()
        .name("version").`type`().optional().stringType()
        .name("vendor").`type`().optional().stringType()
        .name("platform_version").`type`().optional().stringType()
        .name("xpcom_abi").`type`().optional().stringType()
        .name("hotfix_version").`type`().optional().stringType()
      .endRecord()
    val settingsType = SchemaBuilder
      .record("settings").fields()
        .name("addon_compatibility_check_enabled").`type`().optional().booleanType()
        .name("blocklist_enabled").`type`().optional().booleanType()
        .name("is_default_browser").`type`().optional().booleanType()
        .name("default_search_engine").`type`().optional().stringType()
        .name("default_search_engine_data").`type`().optional()
          .record("default_search_engine_data").fields()
            .name("name").`type`().optional().stringType()
            .name("load_path").`type`().optional().stringType()
            .name("submission_url").`type`().optional().stringType()
          .endRecord()
        .name("search_cohort").`type`().optional().stringType()
        .name("e10s_enabled").`type`().optional().booleanType()
        .name("e10s_cohort").`type`().optional().stringType()
        .name("telemetry_enabled").`type`().optional().booleanType()
        .name("locale").`type`().optional().stringType()
        .name("update").`type`().optional()
          .record("update").fields()
            .name("channel").`type`().optional().stringType()
            .name("enabled").`type`().optional().booleanType()
            .name("auto_download").`type`().optional().booleanType()
          .endRecord()
        .name("user_prefs").`type`().optional().map().values().stringType()
      .endRecord()
    val partnerType = SchemaBuilder
      .record("partner").fields()
        .name("distribution_id").`type`().optional().stringType()
        .name("distribution_version").`type`().optional().stringType()
        .name("partner_id").`type`().optional().stringType()
        .name("distributor").`type`().optional().stringType()
        .name("distributor_channel").`type`().optional().stringType()
        .name("partner_names").`type`().optional().array().items().stringType()
      .endRecord()
    val systemType = SchemaBuilder
      .record("system").fields()
        .name("memory_mb").`type`().optional().intType()
        .name("virtual_max_mb").`type`().optional().stringType()
        .name("is_wow64").`type`().optional().booleanType()
      .endRecord()
    val systemCpuType = SchemaBuilder
      .record("system_cpu").fields()
        .name("cores").`type`().optional().intType()
        .name("count").`type`().optional().intType()
        .name("vendor").`type`().optional().stringType()
        .name("family").`type`().optional().intType()
        .name("model").`type`().optional().intType()
        .name("stepping").`type`().optional().intType()
        .name("l2cache_kb").`type`().optional().intType()
        .name("l3cache_kb").`type`().optional().intType()
        .name("extensions").`type`().optional().array().items().stringType()
        .name("speed_mhz").`type`().optional().intType()
      .endRecord()
    val systemDeviceType = SchemaBuilder
      .record("system_device").fields()
        .name("model").`type`().optional().stringType()
        .name("manufacturer").`type`().optional().stringType()
        .name("hardware").`type`().optional().stringType()
        .name("is_tablet").`type`().optional().booleanType()
      .endRecord()
    val systemOsType = SchemaBuilder
      .record("system_os").fields()
        .name("name").`type`().optional().stringType()
        .name("version").`type`().optional().stringType()
        .name("kernel_version").`type`().optional().stringType()
        .name("service_pack_major").`type`().optional().intType()
        .name("service_pack_minor").`type`().optional().intType()
        .name("windows_build_number").`type`().optional().intType()
        .name("windows_ubr").`type`().optional().intType()
        .name("install_year").`type`().optional().intType()
        .name("locale").`type`().optional().stringType()
      .endRecord()
    val systemHddType = SchemaBuilder
      .record("system_hdd").fields()
        .name("profile").`type`().optional()
          .record("hdd_profile").fields()
            .name("model").`type`().optional().stringType()
            .name("revision").`type`().optional().stringType()
          .endRecord()
        .name("binary").`type`().optional()
          .record("binary").fields()
            .name("model").`type`().optional().stringType()
            .name("revision").`type`().optional().stringType()
          .endRecord()
        .name("system").`type`().optional()
          .record("hdd_system").fields()
            .name("model").`type`().optional().stringType()
            .name("revision").`type`().optional().stringType()
          .endRecord()
      .endRecord()
    val systemGfxType = SchemaBuilder
      .record("system_gfx").fields()
        .name("d2d_enabled").`type`().optional().booleanType()
        .name("d_write_enabled").`type`().optional().booleanType()
        .name("adapters").`type`().optional().array().items()
          .record("adapter").fields()
            .name("description").`type`().optional().stringType()
            .name("vendor_id").`type`().optional().stringType()
            .name("device_id").`type`().optional().stringType()
            .name("subsys_id").`type`().optional().stringType()
            .name("ram").`type`().optional().intType()
            .name("driver").`type`().optional().stringType()
            .name("driver_version").`type`().optional().stringType()
            .name("driver_date").`type`().optional().stringType()
            .name("gpu_active").`type`().optional().booleanType()
         .endRecord()
        .name("monitors").`type`().optional().array().items()
          .record("monitor").fields()
            .name("screen_width").`type`().optional().intType()
            .name("screen_height").`type`().optional().intType()
            .name("refresh_rate").`type`().optional().stringType()
            .name("pseudo_display").`type`().optional().booleanType()
            .name("scale").`type`().optional().doubleType()
          .endRecord()
      .endRecord()

    val activeAddonsType = SchemaBuilder
      .map().values()
        .record("active_addon").fields()
          .name("blocklisted").`type`().optional().booleanType()
          .name("description").`type`().optional().stringType()
          .name("name").`type`().optional().stringType()
          .name("user_disabled").`type`().optional().booleanType()
          .name("app_disabled").`type`().optional().booleanType()
          .name("version").`type`().optional().stringType()
          .name("scope").`type`().optional().intType()
          .name("type").`type`().optional().stringType()
          .name("foreign_install").`type`().optional().booleanType()
          .name("has_binary_components").`type`().optional().booleanType()
          .name("install_day").`type`().optional().longType()
          .name("update_day").`type`().optional().longType()
          .name("signed_state").`type`().optional().intType()
          .name("is_system").`type`().optional().booleanType()
        .endRecord()
    val themeType = SchemaBuilder
      .record("theme").fields()
        .name("id").`type`().optional().stringType()
        .name("blocklisted").`type`().optional().booleanType()
        .name("description").`type`().optional().stringType()
        .name("name").`type`().optional().stringType()
        .name("user_disabled").`type`().optional().booleanType()
        .name("app_disabled").`type`().optional().booleanType()
        .name("version").`type`().optional().stringType()
        .name("scope").`type`().optional().intType()
        .name("foreign_install").`type`().optional().booleanType()
        .name("has_binary_components").`type`().optional().booleanType()
        .name("install_day").`type`().optional().longType()
        .name("update_day").`type`().optional().longType()
      .endRecord()
    val activePluginsType = SchemaBuilder
      .array().items()
        .record("active_plugin").fields()
          .name("name").`type`().optional().stringType()
          .name("version").`type`().optional().stringType()
          .name("description").`type`().optional().stringType()
          .name("blocklisted").`type`().optional().booleanType()
          .name("disabled").`type`().optional().booleanType()
          .name("clicktoplay").`type`().optional().booleanType()
          .name("mime_types").`type`().optional().array().items().stringType()
          .name("update_day").`type`().optional().longType()
        .endRecord()
    val activeGMPluginsType = SchemaBuilder
      .map().values()
        .record("active_gmp_plugins").fields()
          .name("version").`type`().optional().stringType()
          .name("user_disabled").`type`().optional().booleanType()
          .name("apply_background_updates").`type`().optional().intType()
        .endRecord()
    val activeExperimentType = SchemaBuilder
      .record("active_experiment").fields()
        .name("id").`type`().optional().stringType()
        .name("branch").`type`().optional().stringType()
      .endRecord()

    val simpleMeasurementsType = SchemaBuilder
      .record("simple_measurements").fields()
        .name("active_ticks").`type`().optional().longType()
        .name("profile_before_change").`type`().optional().longType()
        .name("select_profile").`type`().optional().longType()
        .name("session_restore_init").`type`().optional().longType()
        .name("first_load_uri").`type`().optional().longType()
        .name("uptime").`type`().optional().longType()
        .name("total_time").`type`().optional().longType()
        .name("saved_pings").`type`().optional().longType()
        .name("start").`type`().optional().longType()
        .name("startup_session_restore_read_bytes").`type`().optional().longType()
        .name("pings_overdue").`type`().optional().longType()
        .name("first_paint").`type`().optional().longType()
        .name("shutdown_duration").`type`().optional().longType()
        .name("session_restored").`type`().optional().longType()
        .name("startup_window_visible_write_bytes").`type`().optional().longType()
        .name("startup_crash_detection_end").`type`().optional().longType()
        .name("startup_session_restore_write_bytes").`type`().optional().longType()
        .name("startup_crash_detection_begin").`type`().optional().longType()
        .name("startup_interrupted").`type`().optional().longType()
        .name("after_profile_locked").`type`().optional().longType()
        .name("delayed_startup_started").`type`().optional().longType()
        .name("main").`type`().optional().longType()
        .name("create_top_level_window").`type`().optional().longType()
        .name("session_restore_initialized").`type`().optional().longType()
        .name("maximal_number_of_concurrent_threads").`type`().optional().longType()
        .name("startup_window_visible_read_bytes").`type`().optional().longType()
      .endRecord()

    val histogramType = SchemaBuilder
      .record("histogram").fields()
        .name("values").`type`().array().items().intType().noDefault()
        .name("sum").`type`().longType().noDefault()
      .endRecord()

    val builder = SchemaBuilder
      .record("Submission").fields()
        .name("client_id").`type`().stringType().noDefault()
        .name("os").`type`().optional().stringType()
        .name("normalized_channel").`type`().stringType().noDefault()
        .name("submission_date").`type`().optional().array().items().stringType()
        .name("sample_id").`type`().optional().array().items().doubleType()
        .name("size").`type`().optional().array().items().doubleType()
        .name("geo_country").`type`().optional().array().items().stringType()
        .name("geo_city").`type`().optional().array().items().stringType()
        .name("dnt_header").`type`().optional().array().items().stringType()
        .name("async_plugin_init").`type`().optional().array().items().booleanType()
        .name("flash_version").`type`().optional().array().items().stringType()
        .name("previous_build_id").`type`().optional().array().items().stringType()
        .name("previous_session_id").`type`().optional().array().items().stringType()
        .name("previous_subsession_id").`type`().optional().array().items().stringType()
        .name("profile_subsession_counter").`type`().optional().array().items().intType()
        .name("profile_creation_date").`type`().optional().array().items().stringType()
        .name("profile_reset_date").`type`().optional().array().items().stringType()
        .name("reason").`type`().optional().array().items().stringType()
        .name("revision").`type`().optional().array().items().stringType()
        .name("session_id").`type`().optional().array().items().stringType()
        .name("session_length").`type`().optional().array().items().longType()
        .name("session_start_date").`type`().optional().array().items().stringType()
        .name("subsession_counter").`type`().optional().array().items().intType()
        .name("subsession_id").`type`().optional().array().items().stringType()
        .name("subsession_length").`type`().optional().array().items().longType()
        .name("subsession_start_date").`type`().optional().array().items().stringType()
        .name("timezone_offset").`type`().optional().array().items().intType()
        .name("build").`type`().optional().array().items(buildType)
        .name("partner").`type`().optional().array().items(partnerType)
        .name("settings").`type`().optional().array().items(settingsType)
        .name("system").`type`().optional().array().items(systemType)
        .name("system_cpu").`type`().optional().array().items(systemCpuType)
        .name("system_device").`type`().optional().array().items(systemDeviceType)
        .name("system_os").`type`().optional().array().items(systemOsType)
        .name("system_hdd").`type`().optional().array().items(systemHddType)
        .name("system_gfx").`type`().optional().array().items(systemGfxType)
        .name("active_addons").`type`().optional().array().items(activeAddonsType)
        .name("theme").`type`().optional().array().items(themeType)
        .name("active_plugins").`type`().optional().array().items(activePluginsType)
        .name("active_gmp_plugins").`type`().optional().array().items(activeGMPluginsType)
        .name("active_experiment").`type`().optional().array().items(activeExperimentType)
        .name("persona").`type`().optional().array().items().stringType()
        .name("thread_hang_activity").`type`().optional().array().items().map().values(histogramType)
        .name("thread_hang_stacks").`type`().optional().array().items().map().values().map().values(histogramType)
        .name("simple_measurements").`type`().optional().array().items(simpleMeasurementsType)

    histogramDefinitions.foreach{ case (k, value) =>
      val key = k.toLowerCase
      value match {
        case h: FlagHistogram if !h.keyed =>
          builder.name(key).`type`().optional().array().items().booleanType()
        case h: FlagHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().booleanType()
        case h: CountHistogram if !h.keyed =>
          builder.name(key).`type`().optional().array().items().intType()
        case h: CountHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().intType()
        case h: EnumeratedHistogram if !h.keyed =>
          builder.name(key).`type`().optional().array().items().array().items().intType()
        case h: EnumeratedHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().array().items().intType()
        case h: BooleanHistogram if !h.keyed =>
          builder.name(key).`type`().optional().array().items().array().items().intType()
        case h: BooleanHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().array().items().intType()
        case h: LinearHistogram if !h.keyed =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case h: LinearHistogram =>
          builder.name(key).`type`().optional().map.values().array().items(histogramType)
        case h: ExponentialHistogram if !h.keyed =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case h: ExponentialHistogram =>
          builder.name(key).`type`().optional().map.values().array().items(histogramType)
        case _ =>
          throw new Exception("Unrecognized histogram type")
      }
    }

    scalarDefinitions.foreach{ case (key, value) =>
      value match {
        case s: UintScalar if !s.keyed =>
          // TODO: After migrating to Spark 2.1.0, change this and the next
          // definitions to builder.name(key).`type`().optional().items().nullable().theTypeDef()
          // Context: https://github.com/mozilla/telemetry-batch-view/pull/124#issuecomment-254218256
          builder.name(key).`type`().optional().array().items(UintScalarSchema)
        case s: UintScalar =>
          builder.name(key).`type`().optional().map().values().array().items(UintScalarSchema)
        case s: BooleanScalar if !s.keyed =>
          builder.name(key).`type`().optional().array().items(BoolScalarSchema)
        case s: BooleanScalar =>
          builder.name(key).`type`().optional().map().values().array().items(BoolScalarSchema)
        case s: StringScalar if !s.keyed =>
          builder.name(key).`type`().optional().array().items(StringScalarSchema)
        case s: StringScalar =>
          builder.name(key).`type`().optional().map().values().array().items(StringScalarSchema)
        case _ =>
          throw new Exception("Unrecognized scalar type")
      }
    }

    builder.endRecord()
  }

  private def vectorizeHistogram_[T:ClassTag](name: String,
                                              payloads: List[Map[String, RawHistogram]],
                                              flatten: RawHistogram => T,
                                              default: T): java.util.Collection[T] = {
    val buffer = ListBuffer[T]()
    for (histograms <- payloads) {
      histograms.get(name) match {
        case Some(histogram) =>
          buffer += flatten(histogram)
        case None =>
          buffer += default
      }
    }
    buffer.asJava
  }

  private def vectorizeHistogram[T:ClassTag](name: String,
                                             definition: HistogramDefinition,
                                             payloads: List[Map[String, RawHistogram]],
                                             histogramSchema: Schema): java.util.Collection[Any] =
    definition match {
      case _: FlagHistogram =>
        // A flag histograms is represented with a scalar.
        vectorizeHistogram_(name, payloads, h => h.values("0") == 0, false)

      case _: BooleanHistogram =>
        // A boolean histograms is represented with an array of two integers.
        def flatten(h: RawHistogram): java.util.Collection[Int] = List(h.values.getOrElse("0", 0), h.values.getOrElse("1", 0)).asJavaCollection
        vectorizeHistogram_(name, payloads, flatten, List(0, 0).asJavaCollection)

      case _: CountHistogram =>
        // A count histograms is represented with a scalar.
        vectorizeHistogram_(name, payloads, h => h.values.getOrElse("0", 0), 0)

      case definition: EnumeratedHistogram =>
        // An enumerated histograms is represented with an array of N integers.
        def flatten(h: RawHistogram): java.util.Collection[Int] = {
          val values = Array.fill(definition.nValues + 1){0}
          h.values.foreach{case (key, value) =>
            values(key.toInt) = value
          }
          values.toList.asJavaCollection
        }

        vectorizeHistogram_(name, payloads, flatten, Array.fill(definition.nValues + 1){0}.toList.asJavaCollection)

      case definition: LinearHistogram =>
        // Exponential and linear histograms are represented with a struct containing
        // an array of N integers (values field) and the sum of entries (sum field).
        val buckets = Histograms.linearBuckets(definition.low, definition.high, definition.nBuckets)

        def flatten(h: RawHistogram): GenericData.Record = {
          val values = Array.fill(buckets.length){0}
          h.values.foreach{ case (key, value) =>
            val index = buckets.indexOf(key.toInt)
            values(index) = value
          }

          val record = new GenericData.Record(histogramSchema)
          record.put("values", values.toList.asJavaCollection)
          record.put("sum", h.sum)
          record
        }

        val empty = {
          val record = new GenericData.Record(histogramSchema)
          record.put("values", Array.fill(buckets.length){0}.toList.asJavaCollection)
          record.put("sum", 0)
          record
        }

        vectorizeHistogram_(name, payloads, flatten, empty)

      case definition: ExponentialHistogram =>
        val buckets = Histograms.exponentialBuckets(definition.low, definition.high, definition.nBuckets)
        def flatten(h: RawHistogram): GenericData.Record = {
          val values = Array.fill(buckets.length){0}
          h.values.foreach{ case (key, value) =>
            val index = buckets.indexOf(key.toInt)
            values(index) = value
          }

          val record = new GenericData.Record(histogramSchema)
          record.put("values", values.toList.asJavaCollection)
          record.put("sum", h.sum)
          record
        }

        val empty = {
          val record = new GenericData.Record(histogramSchema)
          record.put("values", Array.fill(buckets.length){0}.toList.asJavaCollection)
          record.put("sum", 0)
          record
        }

        vectorizeHistogram_(name, payloads, flatten, empty)
    }

  private def JSON2Avro(jsonField: String, jsonPath: List[String], avroField: String,
                        payloads: List[Map[String, Any]],
                        root: GenericRecordBuilder,
                        schema: Schema) {
    implicit val formats = DefaultFormats

    val records = payloads.map{ case (x) =>
      var record = parse(x.getOrElse(jsonField, return).asInstanceOf[String])
      for (key <- jsonPath) {
        record = record \ key
      }
      if (record == JNothing) return
      record
    }

    val fieldSchema = getElemType(schema, avroField)
    val fieldValues = records.map{ case (x) =>
      avro.JSON2Avro.parse(fieldSchema, x)
    }.map {
      case Some(x) => x
      case None => return
    }

    assert(payloads.length == fieldValues.length)
    root.set(avroField, fieldValues.asJava)
  }

  def getStandardHistogramSchema(schema: Schema): Schema = {
    getElemType(schema, "fx_tab_switch_total_ms")
  }

  def getElemType(schema: Schema, field: String): Schema = {
    schema.getField(field).schema.getTypes.get(1).getElementType
  }

  private def keyedHistograms2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema, histogramDefinitions: Map[String, HistogramDefinition]) {
    val histogramsList = payloads.map(Histograms.stripKeyedHistograms)

    val uniqueKeys = histogramsList.flatMap(x => x.keys).distinct.toSet

    val validKeys = for {
      key <- uniqueKeys
      definition <- histogramDefinitions.get(key)
    } yield (key, definition)

    val histogramSchema = getStandardHistogramSchema(schema)

    for ((key, definition) <- validKeys) {
      val keyedHistogramsList = histogramsList.map{x =>
        x.get(key) match {
          case Some(v) => v
          case _ => Map[String, RawHistogram]()
        }
      }

      val uniqueLabels = keyedHistogramsList.flatMap(x => x.keys).distinct.toSet
      val vectorized = for {
        label <- uniqueLabels
        vector = vectorizeHistogram(label, definition, keyedHistogramsList, histogramSchema)
      } yield (label, vector)

      root.set(key.toLowerCase, vectorized.toMap.asJava)
    }
  }

  private def histograms2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema, histogramDefinitions: Map[String, HistogramDefinition]) {
    val histogramsList = payloads.map(Histograms.stripHistograms)

    val uniqueKeys = histogramsList.flatMap(x => x.keys).distinct.toSet

    val validKeys = for {
      key <- uniqueKeys
      definition <- histogramDefinitions.get(key)
    } yield (key, definition)

    val histogramSchema = getStandardHistogramSchema(schema)

    for ((key, definition) <- validKeys) {
      root.set(key.toLowerCase, vectorizeHistogram(key, definition, histogramsList, histogramSchema))
    }
  }

  private def vectorizeScalar_[T:ClassTag](name: String,
                                           payloads: List[Map[String, AnyVal]],
                                           flatten: AnyVal => T,
                                           default: T): java.util.Collection[T] = {
    val buffer = ListBuffer[T]()
    for (scalars <- payloads) {
      scalars.get(name) match {
        case Some(scalar) =>
          buffer += flatten(scalar)
        case None =>
          buffer += default
      }
    }
    buffer.asJava
  }

  private def vectorizeScalar[T:ClassTag](name: String,
                                          definition: ScalarDefinition,
                                          payloads: List[Map[String, AnyVal]]): java.util.Collection[Any] = {
    definition match {
      case _: UintScalar =>
        vectorizeScalar_(
          name, payloads,
          s => {
            val record = new GenericData.Record(UintScalarSchema)
            record.put("value", s.asInstanceOf[BigInt])
            record
           },
          new GenericData.Record(UintScalarSchema))

      case _: BooleanScalar =>
        vectorizeScalar_(
          name, payloads,
          s => {
            val record = new GenericData.Record(BoolScalarSchema)
            record.put("value", s.asInstanceOf[Boolean])
            record
          },
          new GenericData.Record(BoolScalarSchema))

      case _: StringScalar =>
        vectorizeScalar_(
          name, payloads,
          s => {
            val record = new GenericData.Record(StringScalarSchema)
            record.put("value", s.asInstanceOf[String])
            record
          },
          new GenericData.Record(StringScalarSchema))
    }
  }

  private def scalars2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, scalarDefinitions: Map[String, ScalarDefinition]) {
    implicit val formats = DefaultFormats

    // Get a list of the scalars in the ping.
    val scalarsList = payloads.map{ case (x) =>
      val payload = parse(x.getOrElse("payload", return).asInstanceOf[String])
      MainPing.ProcessTypes.map{ process =>
        (payload \ "processes" \ process \ "scalars").toOption match {
          case Some(scalars) => process -> scalars.extract[Map[String, AnyVal]]
          case _ => process -> Map[String, AnyVal]()
        }
      } toMap
    }

    // this allows us to avoid iterating
    // through every payload on every scalar
    val byProcessScalarList = MainPing.ProcessTypes.map{ process =>
      process -> scalarsList.map(_(process))
    } toMap

    for {
      (key, definition) <- scalarDefinitions.toList
      scalar_payloads = byProcessScalarList(definition.process.get)
      if !definition.keyed
    }{
      root.set(key, vectorizeScalar(definition.originalName, definition, scalar_payloads))
    }
  }

  private def keyedScalars2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, scalarDefinitions: Map[String, ScalarDefinition]) {
    implicit val formats = DefaultFormats

    val scalarsList = payloads.map{ case (x) =>
      val payload = parse(x.getOrElse("payload", return).asInstanceOf[String])
      MainPing.ProcessTypes.map{ process =>
        (payload \ "processes" \ process \ "keyedScalars").toOption match {
          case Some(scalars) => process -> scalars.extract[Map[String, Map[String, AnyVal]]]
          case _ => process -> Map[String, Map[String, AnyVal]]()
        }
      } toMap
    }

    // this allows us to avoid iterating
    // through every payload on every scalar
    val byProcessScalarList = MainPing.ProcessTypes.map{ process =>
      process -> scalarsList.map(_(process))
    } toMap

    for {
      (key, definition) <- scalarDefinitions.toList
      scalar_payloads = byProcessScalarList(definition.process.get)
      if definition.keyed
    }{
      // only includes keys that
      // show up for this process
      val keyedScalarsList = scalar_payloads.map{x =>
        x.get(definition.originalName) match {
          case Some(v) => v
          case _ => Map[String, AnyVal]()
        }
      }

      val uniqueLabels = keyedScalarsList.flatMap(x => x.keys).distinct.toSet
      val vectorized = for {
        label <- uniqueLabels
        vector = vectorizeScalar(label, definition, keyedScalarsList)
      } yield (label, vector)

      root.set(key, vectorized.toMap.asJava)
    }
  }

  private def sessionStartDates2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    def extractTimestamps(field: String): java.util.Collection[String] = {
      payloads.flatMap{ _.get("payload.info") }
        .flatMap{ x => parse(x.asInstanceOf[String]) \ field match {
          case JString(value) =>
            Some(normalizeISOTimestamp(value))
          case _ =>
            None
        }
      }.asJavaCollection
    }

    root.set("subsession_start_date", extractTimestamps("subsessionStartDate"))
    root.set("session_start_date", extractTimestamps("sessionStartDate"))
  }

  private def profileDates2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    val creationValues = payloads.map{ case (x) =>
      parse(x.getOrElse("environment.profile", return).asInstanceOf[String]) \ "creationDate" match {
        case JInt(value) => normalizeEpochTimestamp(value)
        case _ => ""
      }
    }
    root.set("profile_creation_date", creationValues.asJavaCollection)

    val resetValues = payloads.map{ case (x) =>
      parse(x.getOrElse("environment.profile", return).asInstanceOf[String]) \ "resetDate" match {
        case JInt(value) => normalizeEpochTimestamp(value)
        case _ => ""
      }
    }
    root.set("profile_reset_date", resetValues.asJavaCollection)
  }

  private def value2Avro[T:ClassTag](field: String, avroField: String, default: T,
                                     mapFunction: Any => Any,
                                     payloads: List[Map[String, Any]],
                                     root: GenericRecordBuilder,
                                     schema: Schema) {
    implicit val formats = DefaultFormats

    val values = payloads.map{ case (x) =>
      val value = x.getOrElse(field, default).asInstanceOf[T]
      mapFunction(value)
    }

    // only set the keys if there are valid entries

    if (values.exists( value => value != default )) {
      root.set(avroField, values.asJava)
    }
  }

  def getFirst[T](key: String, payloads: Seq[Map[String, Any]]): Option[T] = {
    payloads
      .collectFirst{ case m if m contains key => m(key) }
      .map(_.asInstanceOf[T])
  }

  private def buildRecord(history: Iterable[Map[String, Any]], schema: Schema, histogramDefinitions: Map[String, HistogramDefinition], scalarDefinitions: Map[String, ScalarDefinition]): Option[GenericRecord] = {
    // De-dupe records
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
      .set("client_id", getFirst[String]("clientId", sorted).orNull)
      .set("os", getFirst[String]("os", sorted).orNull)
      .set("normalized_channel", getFirst[String]("normalizedChannel", sorted).orNull)

    try {
      value2Avro("sampleId",       "sample_id",       0.0, x => x, sorted, root, schema)
      value2Avro("Size",           "size",            0.0, x => x, sorted, root, schema)
      value2Avro("geoCountry",     "geo_country",     "", x => x, sorted, root, schema)
      value2Avro("geoCity",        "geo_city",        "", x => x, sorted, root, schema)
      value2Avro("DNT",            "dnt_header",      "", x => x, sorted, root, schema)
      value2Avro("submissionDate", "submission_date", "", x => normalizeYYYYMMDDTimestamp(x.asInstanceOf[String]), sorted, root, schema)
      JSON2Avro("environment.build",          List[String](),                   "build", sorted, root, schema)
      JSON2Avro("environment.partner",        List[String](),                   "partner", sorted, root, schema)
      JSON2Avro("environment.settings",       List[String](),                   "settings", sorted, root, schema)
      JSON2Avro("environment.system",         List[String](),                   "system", sorted, root, schema)
      JSON2Avro("environment.system",         List("cpu"),                      "system_cpu", sorted, root, schema)
      JSON2Avro("environment.system",         List("device"),                   "system_device", sorted, root, schema)
      JSON2Avro("environment.system",         List("os"),                       "system_os", sorted, root, schema)
      JSON2Avro("environment.system",         List("hdd"),                      "system_hdd", sorted, root, schema)
      JSON2Avro("environment.system",         List("gfx"),                      "system_gfx", sorted, root, schema)
      JSON2Avro("environment.addons",         List("activeAddons"),             "active_addons", sorted, root, schema)
      JSON2Avro("environment.addons",         List("theme"),                    "theme", sorted, root, schema)
      JSON2Avro("environment.addons",         List("activePlugins"),            "active_plugins", sorted, root, schema)
      JSON2Avro("environment.addons",         List("activeGMPlugins"),          "active_gmp_plugins", sorted, root, schema)
      JSON2Avro("environment.addons",         List("activeExperiment"),         "active_experiment", sorted, root, schema)
      JSON2Avro("environment.addons",         List("persona"),                  "persona", sorted, root, schema)
      JSON2Avro("payload.simpleMeasurements", List[String](),                   "simple_measurements", sorted, root, schema)
      JSON2Avro("payload.info",               List("asyncPluginInit"),          "async_plugin_init", sorted, root, schema)
      JSON2Avro("payload.info",               List("previousBuildId"),          "previous_build_id", sorted, root, schema)
      JSON2Avro("payload.info",               List("previousSessionId"),        "previous_session_id", sorted, root, schema)
      JSON2Avro("payload.info",               List("previousSubsessionId"),     "previous_subsession_id", sorted, root, schema)
      JSON2Avro("payload.info",               List("profileSubsessionCounter"), "profile_subsession_counter", sorted, root, schema)
      JSON2Avro("payload.info",               List("reason"),                   "reason", sorted, root, schema)
      JSON2Avro("payload.info",               List("revision"),                 "revision", sorted, root, schema)
      JSON2Avro("payload.info",               List("sessionId"),                "session_id", sorted, root, schema)
      JSON2Avro("payload.info",               List("sessionLength"),            "session_length", sorted, root, schema)
      JSON2Avro("payload.info",               List("subsessionCounter"),        "subsession_counter", sorted, root, schema)
      JSON2Avro("payload.info",               List("subsessionId"),             "subsession_id", sorted, root, schema)
      JSON2Avro("payload.info",               List("subsessionLength"),         "subsession_length", sorted, root, schema)
      JSON2Avro("payload.info",               List("timezoneOffset"),           "timezone_offset", sorted, root, schema)
      histograms2Avro(sorted, root, schema, histogramDefinitions)
      keyedHistograms2Avro(sorted, root, schema, histogramDefinitions)
      scalars2Avro(sorted, root, scalarDefinitions)
      keyedScalars2Avro(sorted, root, scalarDefinitions)
      sessionStartDates2Avro(sorted, root, schema)
      profileDates2Avro(sorted, root, schema)
    } catch {
      case e : Throwable =>
        // Ignore buggy clients
        return None
    }

    Some(root.build)
  }
}
