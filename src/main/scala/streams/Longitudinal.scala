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

private class ClientIdPartitioner(size: Int) extends Partitioner{
  def numPartitions: Int = size
  def getPartition(key: Any): Int = key match {
    case (clientId: String, startDate: String, counter: Int) => abs(clientId.hashCode) % size
    case _ => throw new Exception("Invalid key")
  }
}

class ClientIterator(it: Iterator[Tuple2[String, Map[String, Any]]], maxHistorySize: Int = 1000) extends Iterator[List[Map[String, Any]]]{
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

  override def hasNext() = !buffer.isEmpty

  override def next(): List[Map[String, Any]] = {
    while (it.hasNext) {
      val (key, value) = it.next()

      if (key == currentKey && buffer.size == maxHistorySize) {
        // Trim long histories
        val result = buffer.toList
        buffer.clear

        // Fast forward to next client
        while (it.hasNext) {
          val (k, _) = it.next()
          if (k != currentKey) {
            buffer += value
            return result
          }
        }

        return result
      } else if (key == currentKey){
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

case class Longitudinal() extends DerivedStream {
  override def streamName: String = "telemetry-sample"
  override def filterPrefix: String = "telemetry/4/main/*/*/*/42/"

  override def transform(sc: SparkContext, bucket: Bucket, summaries: RDD[ObjectSummary], from: String, to: String) {
    val prefix = s"v$to"

    if (!isS3PrefixEmpty(prefix)) {
      println(s"Warning: prefix $prefix already exists on S3!")
      return
    }

    // Sort submissions in descending order
    implicit val ordering = Ordering[Tuple3[String, String, Int]].reverse
    val groups = DerivedStream.groupBySize(summaries.collect().toIterator)
    val clientMessages = sc.parallelize(groups, groups.size)
      .flatMap(x => x)
      .flatMap{ case obj =>
        val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3: %s".format(obj.key)))
        for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message }
      .flatMap{ case message =>
        val fields = HekaFrame.fields(message)
        for {
          clientId <- fields.get("clientId").asInstanceOf[Option[String]]
          json <- fields.get("payload.info").asInstanceOf[Option[String]]
          info = parse(json)
          tmp = for {
            JString(startDate) <- info \ "subsessionStartDate"
            JInt(counter) <- info \ "profileSubsessionCounter"
          } yield (startDate, counter)
          (startDate, counter) <- tmp.headOption
        } yield ((clientId, startDate, counter.toInt), fields)
      }
      .repartitionAndSortWithinPartitions(new ClientIdPartitioner(480))
      .map{case (key, value) => (key._1, value)}

    /* One file per partition is generated at the end of the job. We want to have
       few big files but at the same time we need a high enough degree of parallelism
       to keep all workers busy. Since the cluster typically used for this job has
       8 workers and 20 executors, 320 partitions provide a good compromise. */

    val partitionCounts = clientMessages
      .mapPartitions{ case it =>
        val clientIterator = new ClientIterator(it)
        val schema = buildSchema

        val allRecords = for {
          client <- clientIterator
          record = buildRecord(client, schema)
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

        while(!records.isEmpty) {
          // Block size has to be increased to pack more than a couple hundred profiles
          // within the same row group.
          val localFile = ParquetFile.serialize(records, schema, 8)
          uploadLocalFileToS3(localFile, prefix)
        }

        List((processedCount + ignoredCount, ignoredCount)).toIterator
    }

    val counts = partitionCounts.reduce( (x, y) => (x._1 + y._1, x._2 + y._2))
    println("Clients seen: %d".format(counts._1))
    println("Clients ignored: %d".format(counts._2))
  }

  private def buildSchema: Schema = {
    // The $PROJECT_ROOT/scripts/generate-ping-schema.py script can be used to
    // generate these SchemaBuilder definitions, and the output of that script is
    // combined with data from http://gecko.readthedocs.org/en/latest/toolkit/components/telemetry/telemetry
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
        .name("default_search_engine_data").`type`().optional().record("default_search_engine_data").fields()
          .name("name").`type`().optional().stringType()
          .name("load_path").`type`().optional().stringType()
          .name("submission_url").`type`().optional().stringType()
        .endRecord()
        .name("search_cohort").`type`().optional().stringType()
        .name("e10s_enabled").`type`().optional().booleanType()
        .name("telemetry_enabled").`type`().optional().booleanType()
        .name("locale").`type`().optional().stringType()
        .name("update").`type`().optional().record("update").fields()
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
        .name("locale").`type`().optional().stringType()
      .endRecord()
    val systemHddType = SchemaBuilder
      .record("system_hdd").fields()
        .name("profile").`type`().optional().record("hdd_profile").fields()
          .name("model").`type`().optional().stringType()
          .name("revision").`type`().optional().stringType()
        .endRecord()
        .name("binary").`type`().optional().record("binary").fields()
          .name("model").`type`().optional().stringType()
          .name("revision").`type`().optional().stringType()
        .endRecord()
        .name("system").`type`().optional().record("hdd_system").fields()
          .name("model").`type`().optional().stringType()
          .name("revision").`type`().optional().stringType()
        .endRecord()
      .endRecord()
    val systemGfxType = SchemaBuilder
      .record("system_gfx").fields()
        .name("d2d_enabled").`type`().optional().booleanType()
        .name("d_write_enabled").`type`().optional().booleanType()
        .name("adapters").`type`().optional().array().items().record("adapter").fields()
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
        .name("monitors").`type`().optional().array().items().record("monitor").fields()
          .name("screen_width").`type`().optional().intType()
          .name("screen_height").`type`().optional().intType()
          .name("refresh_rate").`type`().optional().stringType()
          .name("pseudo_display").`type`().optional().booleanType()
          .name("scale").`type`().optional().doubleType()
        .endRecord()
      .endRecord()

    val activeAddonsType = SchemaBuilder
      .map().values().record("active_addon").fields()
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
      .array().items().record("active_plugin").fields()
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
      .map().values().record("active_gmp_plugins").fields()
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
        .name("os").`type`().stringType().noDefault()
        .name("normalized_channel").`type`().stringType().noDefault()
        .name("submission_date").`type`().optional().array().items().stringType()
        .name("sample_id").`type`().optional().array().items().doubleType()
        .name("size").`type`().optional().array().items().doubleType()
        .name("geo_country").`type`().optional().array().items().stringType()
        .name("geo_city").`type`().optional().array().items().stringType()
        .name("dnt_header").`type`().optional().array().items().stringType()
        .name("addons").`type`().optional().array().items().stringType()
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

    Histograms.definitions.foreach{ case (k, value) =>
      val key = k.toLowerCase
      value match {
        case h: FlagHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().booleanType()
        case h: FlagHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().booleanType()
        case h: CountHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().intType()
        case h: CountHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().intType()
        case h: EnumeratedHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().array().items().intType()
        case h: EnumeratedHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().array().items().intType()
        case h: BooleanHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().array().items().intType()
        case h: BooleanHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().array().items().intType()
        case h: LinearHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case h: LinearHistogram =>
          builder.name(key).`type`().optional().map.values().array().items(histogramType)
        case h: ExponentialHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items(histogramType)
        case h: ExponentialHistogram =>
          builder.name(key).`type`().optional().map.values().array().items(histogramType)
        case _ =>
          throw new Exception("Unrecognized histogram type")
      }
    }

    builder.endRecord()
  }

  private def vectorizeHistogram_[T:ClassTag](name: String,
                                              payloads: List[Map[String, RawHistogram]],
                                              flatten: RawHistogram => T,
                                              default: T): Array[T] = {
    val buffer = ListBuffer[T]()
    for (histograms <- payloads) {
      histograms.get(name) match {
        case Some(histogram) =>
          buffer += flatten(histogram)
        case None =>
          buffer += default
      }
    }
    buffer.toArray
  }

  private def vectorizeHistogram[T:ClassTag](name: String,
                                             definition: HistogramDefinition,
                                             payloads: List[Map[String, RawHistogram]],
                                             histogramSchema: Schema): Array[Any] =
    definition match {
      case _: FlagHistogram =>
        // A flag histograms is represented with a scalar.
        vectorizeHistogram_(name, payloads, h => h.values("0") > 0, false)

      case _: BooleanHistogram =>
        // A boolean histograms is represented with an array of two integers.
        def flatten(h: RawHistogram): Array[Int] = Array(h.values.getOrElse("0", 0), h.values.getOrElse("1", 0))
        vectorizeHistogram_(name, payloads, flatten, Array(0, 0))

      case _: CountHistogram =>
        // A count histograms is represented with a scalar.
        vectorizeHistogram_(name, payloads, h => h.values.getOrElse("0", 0), 0)

      case definition: TimeHistogram =>
        val buckets = definition.ranges
        def flatten(h: RawHistogram): GenericData.Record = {
          val values = Array.fill(buckets.length){0}
          h.values.foreach{ case (key, value) =>
            val index = buckets.indexOf(key.toInt)
            values(index) = value
          }

          val record = new GenericData.Record(histogramSchema)
          record.put("values", values)
          record.put("sum", h.sum)
          record
        }

        val empty = {
          val record = new GenericData.Record(histogramSchema)
          record.put("values", Array.fill(buckets.length){0})
          record.put("sum", 0)
          record
        }

        vectorizeHistogram_(name, payloads, flatten, empty)

      case definition: EnumeratedHistogram =>
        // An enumerated histograms is represented with an array of N integers.
        def flatten(h: RawHistogram): Array[Int] = {
          val values = Array.fill(definition.nValues + 1){0}
          h.values.foreach{case (key, value) =>
            values(key.toInt) = value
          }
          values
        }

        vectorizeHistogram_(name, payloads, flatten, Array.fill(definition.nValues + 1){0})

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
          record.put("values", values)
          record.put("sum", h.sum)
          record
        }

        val empty = {
          val record = new GenericData.Record(histogramSchema)
          record.put("values", Array.fill(buckets.length){0})
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
          record.put("values", values)
          record.put("sum", h.sum)
          record
        }

        val empty = {
          val record = new GenericData.Record(histogramSchema)
          record.put("values", Array.fill(buckets.length){0})
          record.put("sum", 0)
          record
        }

        val x: Array[GenericData.Record] = vectorizeHistogram_(name, payloads, flatten, empty)
        x.asInstanceOf[Array[Any]]
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

    val fieldSchema = schema.getField(avroField).schema().getTypes()(1).getElementType()
    val fieldValues = records.map{ case (x) =>
      avro.JSON2Avro.parse(fieldSchema, x)
    }

    root.set(avroField, fieldValues.flatten.toArray)
  }

  private def keyedHistograms2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    val histogramsList = payloads.map{ case (x) =>
      val json = x.getOrElse("payload.keyedHistograms", return).asInstanceOf[String]
      parse(json).extract[Map[String, Map[String, RawHistogram]]]
    }

    val uniqueKeys = histogramsList.flatMap(x => x.keys).distinct.toSet

    val validKeys = for {
      key <- uniqueKeys
      definition <- Histograms.definitions.get(key)
    } yield (key, definition)

    val histogramSchema = schema.getField("gc_ms").schema().getTypes()(1).getElementType()

    for ((key, definition) <- validKeys) {
      val keyedHistogramsList = histogramsList.map{x =>
        x.get(key) match {
          case Some(x) => x
          case _ => Map[String, RawHistogram]()
        }
      }

      val uniqueLabels = keyedHistogramsList.flatMap(x => x.keys).distinct.toSet
      val vector = vectorizeHistogram(key, definition, keyedHistogramsList, histogramSchema)
      val vectorized = for {
        label <- uniqueLabels
        vector = vectorizeHistogram(label, definition, keyedHistogramsList, histogramSchema)
      } yield (label, vector)

      root.set(key.toLowerCase, vectorized.toMap.asJava)
    }
  }

  private def histograms2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    val histogramsList = payloads.map{ case (x) =>
      val json = x.getOrElse("payload.histograms", return).asInstanceOf[String]
      parse(json).extract[Map[String, RawHistogram]]
    }

    val uniqueKeys = histogramsList.flatMap(x => x.keys).distinct.toSet

    val validKeys = for {
      key <- uniqueKeys
      definition <- Histograms.definitions.get(key)
    } yield (key, definition)

    val histogramSchema = schema.getField("gc_ms").schema().getTypes()(1).getElementType()

    for ((key, definition) <- validKeys) {
      root.set(key.toLowerCase, vectorizeHistogram(key, definition, histogramsList, histogramSchema))
    }
  }

  private def threadHangStats2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    var ranges = Array[Int]()
    val threadHangStatsList = payloads.map{ case (payload) =>
      val json = payload.getOrElse("payload.threadHangStats", return).asInstanceOf[String]
      val body = parse(json)
      body match {
        // for some reason, when there are no thread hangs, it's an object rather than an array - we only want to accept the array
        case _ : JArray => null
        case _ => return
      }
      val threadHangStats = body.extract[Array[Map[String, JValue]]]
      for (thread <- threadHangStats) {
        ranges = thread.getOrElse("activity", return).extract[Map[String, JValue]]
                       .getOrElse("ranges", return).extract[List[BigInt]]
                       .map(x => x.toInt).toArray
      }
      threadHangStats
    }

    val definition = TimeHistogram(ranges)
    val histogramSchema = schema.getField("gc_ms").schema().getTypes()(1).getElementType()

    val threadHangActivityList = threadHangStatsList.map{ case (threadHangStats) =>
      val threadHangActivity = threadHangStats.map{ case thread =>
        val name = thread.getOrElse("name", return).extract[String]
        val activity = thread.getOrElse("activity", return).extract[RawHistogram]
        val histograms = vectorizeHistogram("activity", definition, List(Map("activity" -> activity)), histogramSchema)
        val histogram = histograms(0).asInstanceOf[GenericData.Record]
        (name -> histogram)
      }.toMap
      mapAsJavaMap(threadHangActivity)
    } toArray

    val threadHangStackMapList = threadHangStatsList.map{ case (threadHangStats) =>
      val threadHangStackMap = threadHangStats.map{ case thread =>
        val name = thread.getOrElse("name", return).extract[String]
        val hangs = thread.getOrElse("hangs", return).extract[Array[Map[String, JValue]]]
        val stackMapping = hangs.map{ case (hang) =>
          val stack = hang.getOrElse("stack", return).extract[Array[String]]
          val histogramEntry = hang.getOrElse("histogram", return).extract[RawHistogram]
          val histograms = vectorizeHistogram("hang", definition, List(Map("hang" -> histogramEntry)), histogramSchema)
          val histogram = histograms(0).asInstanceOf[GenericData.Record]
          (stack.mkString("\n") -> histogram)
        }.toMap
        (name -> mapAsJavaMap(stackMapping))
      }.toMap
      mapAsJavaMap(threadHangStackMap)
    } toArray

    root.set("thread_hang_activity", threadHangActivityList)
    root.set("thread_hang_stacks", threadHangStackMapList)
  }

  private def subsessionStartDate2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    val fieldValues = payloads.map{ case (x) =>
      var record = parse(x.getOrElse("payload.info", return).asInstanceOf[String]) \ "subsessionStartDate"
      record match {
        case JString(value) =>
          Utils.normalizeISOTimestamp(value)
        case _ =>
          return
      }
    }

    root.set("subsession_start_date", fieldValues.toArray)
  }

  private def profileDates2Avro(payloads: List[Map[String, Any]], root: GenericRecordBuilder, schema: Schema) {
    implicit val formats = DefaultFormats

    val creationValues = payloads.map{ case (x) =>
      var record = parse(x.getOrElse("environment.profile", return).asInstanceOf[String]) \ "creationDate"
      record match {
        case JInt(value) => Utils.normalizeEpochTimestamp(value)
        case _ => ""
      }
    }
    root.set("profile_creation_date", creationValues.toArray)

    val resetValues = payloads.map{ case (x) =>
      var record = parse(x.getOrElse("environment.profile", return).asInstanceOf[String]) \ "resetDate"
      record match {
        case JInt(value) => Utils.normalizeEpochTimestamp(value)
        case _ => ""
      }
    }
    root.set("profile_reset_date", resetValues.toArray)
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
      root.set(avroField, values.toArray)
    }
  }

  private def buildRecord(history: Iterable[Map[String, Any]], schema: Schema): Option[GenericRecord] = {
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
      .set("client_id", sorted(0)("clientId").asInstanceOf[String])
      .set("os", sorted(0)("os").asInstanceOf[String])
      .set("normalized_channel", sorted(0)("normalizedChannel").asInstanceOf[String])

    try {
      value2Avro("sampleId",          "sample_id",   0.0, x => x, sorted, root, schema)
      value2Avro("Size",              "size",        0.0, x => x, sorted, root, schema)
      value2Avro("geoCountry",        "geo_country", "",  x => x, sorted, root, schema)
      value2Avro("geoCity",           "geo_city",    "",  x => x, sorted, root, schema)
      value2Avro("DNT",               "dnt_header",  "",  x => x, sorted, root, schema)

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
      JSON2Avro("payload.info",               List("addons"),                   "addons", sorted, root, schema)
      JSON2Avro("payload.info",               List("asyncPluginInit"),          "async_plugin_init", sorted, root, schema)
      JSON2Avro("payload.info",               List("flashVersion"),             "flash_version", sorted, root, schema)
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
      keyedHistograms2Avro(sorted, root, schema)
      histograms2Avro(sorted, root, schema)
      threadHangStats2Avro(sorted, root, schema)

      subsessionStartDate2Avro(sorted, root, schema)
      value2Avro("submissionDate", "submission_date", "", x => Utils.normalizeYYYYMMDDTimestamp(x.asInstanceOf[String]), sorted, root, schema)
      profileDates2Avro(sorted, root, schema)
    } catch {
      case e : Throwable =>
        // Ignore buggy clients
        return None
    }

    Some(root.build)
  }
}
