package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.math.max
import scala.reflect.ClassTag
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.avro
import telemetry.heka.{HekaFrame, Message}
import telemetry.histograms._
import telemetry.parquet.ParquetFile

case class Longitudinal() extends DerivedStream {
  override def streamName: String = "telemetry-release"
  override def filterPrefix: String = "telemetry/4/main/*/*/*/*/*/42/"

  override def transform(sc: SparkContext, bucket: Bucket, summaries: RDD[ObjectSummary], from: String, to: String) {
    val prefix = s"generationDate=$to"

    if (!isS3PrefixEmpty(prefix)) {
      println(s"Warning: prefix $prefix already exists on S3!")
      return
    }

    val groups = DerivedStream.groupBySize(summaries.collect().toIterator)
    val clientMessages = sc.parallelize(groups, groups.size)
      .flatMap(x => x)
      .flatMap{ case obj =>
        val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3: %s".format(obj.key)))
        for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message }
      .flatMap{ case message =>
        val fields = HekaFrame.fields(message)
        val clientId = fields.get("clientId")

        clientId match {
          case Some(client: String) => List((client, fields))
          case _ => Nil
        }}
      .groupByKey()
      .coalesce(max((0.5*sc.defaultParallelism).toInt, 1), true)  // see https://issues.apache.org/jira/browse/PARQUET-222

    val partitionCounts = clientMessages
      .values
      .mapPartitions{ case clientIterator =>
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
          val localFile = ParquetFile.serialize(records, schema)
          uploadLocalFileToS3(localFile, prefix)
        }

        List((processedCount + ignoredCount, ignoredCount)).toIterator
    }

    val counts = partitionCounts.reduce( (x, y) => (x._1 + y._1, x._2 + y._2))
    println("Clients seen: %d".format(counts._1))
    println("Clients ignored: %d".format(counts._2))
  }

  private def buildSchema: Schema = {
    // The generate-ping-schema.py script can be used to generate these SchemaBuilder definitions, and the output of that script is combined with data
    // from http://gecko.readthedocs.org/en/latest/toolkit/components/telemetry/telemetry/environment.html to get the results below

    // See https://github.com/SamPenrose/data-pipeline/blob/v4-baseset-schema/schemas/test/application.schema.json commit 9993f0c0d3fc2e2dc16113070eb77554a72ce975
    val applicationType = SchemaBuilder
      .record("application").fields()
        .name("architecture").`type`().stringType().noDefault()
        .name("buildId").`type`().stringType().noDefault()
        .name("name").`type`().stringType().noDefault()
        .name("version").`type`().stringType().noDefault()
        .name("vendor").`type`().stringType().noDefault()
        .name("platformVersion").`type`().stringType().noDefault()
        .name("xpcomAbi").`type`().stringType().noDefault()
        .name("channel").`type`().optional().stringType()
      .endRecord()

    // See https://github.com/SamPenrose/data-pipeline/blob/v4-baseset-schema/schemas/test/environment.schema.json commit 9993f0c0d3fc2e2dc16113070eb77554a72ce975
    val buildType = SchemaBuilder
      .record("build").fields()
        .name("applicationId").`type`().stringType().noDefault()
        .name("applicationName").`type`().stringType().noDefault()
        .name("architecture").`type`().stringType().noDefault()
        .name("architecturesInBinary").`type`().optional().stringType()
        .name("buildId").`type`().stringType().noDefault()
        .name("version").`type`().stringType().noDefault()
        .name("vendor").`type`().stringType().noDefault()
        .name("platformVersion").`type`().stringType().noDefault()
        .name("xpcomAbi").`type`().stringType().noDefault()
        .name("hotfixVersion").`type`().optional().stringType()
      .endRecord()
    val settingsType = SchemaBuilder
      .record("settings").fields()
        .name("addonCompatibilityCheckEnabled").`type`().optional().booleanType()
        .name("blocklistEnabled").`type`().optional().booleanType()
        .name("isDefaultBrowser").`type`().optional().booleanType()
        .name("defaultSearchEngine").`type`().optional().stringType()
        .name("defaultSearchEngineData").`type`().optional().record("defaultSearchEngineData").fields()
          .name("name").`type`().optional().stringType()
          .name("loadPath").`type`().optional().stringType()
          .name("submissionURL").`type`().optional().stringType()
        .endRecord()
        .name("searchCohort").`type`().optional().stringType()
        .name("e10sEnabled").`type`().optional().booleanType()
        .name("telemetryEnabled").`type`().optional().booleanType()
        .name("locale").`type`().optional().stringType()
        .name("update").`type`().optional().record("update").fields()
          .name("channel").`type`().optional().stringType()
          .name("enabled").`type`().optional().booleanType()
          .name("autoDownload").`type`().optional().booleanType()
        .endRecord()
        .name("userPrefs").`type`().optional().map().values().stringType()
      .endRecord()
    val profileType = SchemaBuilder
      .record("profile").fields()
        .name("creationDate").`type`().optional().longType()
        .name("resetDate").`type`().optional().longType()
      .endRecord()
    val partnerType = SchemaBuilder
      .record("partner").fields()
        .name("distributionId").`type`().optional().stringType()
        .name("distributionVersion").`type`().optional().stringType()
        .name("partnerId").`type`().optional().stringType()
        .name("distributor").`type`().optional().stringType()
        .name("distributorChannel").`type`().optional().stringType()
        .name("partnerNames").`type`().optional().array().items().stringType()
      .endRecord()
    val systemType = SchemaBuilder
      .record("system").fields()
        .name("memoryMB").`type`().optional().intType()
        .name("virtualMaxMB").`type`().optional().stringType()
        .name("isWow64").`type`().optional().booleanType()
        .name("cpu").`type`().optional().record("cpu").fields()
          .name("cores").`type`().optional().intType()
          .name("count").`type`().optional().intType()
          .name("vendor").`type`().optional().stringType()
          .name("family").`type`().optional().intType()
          .name("model").`type`().optional().intType()
          .name("stepping").`type`().optional().intType()
          .name("l2cacheKB").`type`().optional().intType()
          .name("l3cacheKB").`type`().optional().intType()
          .name("extensions").`type`().optional().array().items().stringType()
          .name("speedMHz").`type`().optional().intType()
        .endRecord()
        .name("device").`type`().optional().record("device").fields()
          .name("model").`type`().optional().stringType()
          .name("manufacturer").`type`().optional().stringType()
          .name("hardware").`type`().optional().stringType()
          .name("isTablet").`type`().optional().booleanType()
        .endRecord()
        .name("os").`type`().optional().record("os").fields()
          .name("name").`type`().optional().stringType()
          .name("version").`type`().optional().stringType()
          .name("kernelVersion").`type`().optional().stringType()
          .name("servicePackMajor").`type`().optional().intType()
          .name("servicePackMinor").`type`().optional().intType()
          .name("locale").`type`().optional().stringType()
        .endRecord()
        .name("hdd").`type`().optional().record("hdd").fields()
          .name("hdd_profile").`type`().optional().record("hdd_profile").fields()
            .name("model").`type`().optional().stringType()
            .name("revision").`type`().optional().stringType()
          .endRecord()
          .name("binary").`type`().optional().record("binary").fields()
            .name("model").`type`().optional().stringType()
            .name("revision").`type`().optional().stringType()
          .endRecord()
          .name("hdd_system").`type`().optional().record("hdd_system").fields()
            .name("model").`type`().optional().stringType()
            .name("revision").`type`().optional().stringType()
          .endRecord()
        .endRecord()
        .name("gfx").`type`().optional().record("gfx").fields()
          .name("D2DEnabled").`type`().optional().booleanType()
          .name("DWriteEnabled").`type`().optional().booleanType()
          .name("adapters").`type`().optional().array().items().record("adapter").fields()
            .name("description").`type`().optional().stringType()
            .name("vendorID").`type`().optional().stringType()
            .name("deviceID").`type`().optional().stringType()
            .name("subsysID").`type`().optional().stringType()
            .name("RAM").`type`().optional().intType()
            .name("driver").`type`().optional().stringType()
            .name("driverVersion").`type`().optional().stringType()
            .name("driverDate").`type`().optional().stringType()
            .name("GPUActive").`type`().booleanType().noDefault()
          .endRecord()
          .name("monitors").`type`().optional().array().items().record("monitor").fields()
            .name("screenWidth").`type`().optional().intType()
            .name("screenHeight").`type`().optional().intType()
            .name("refreshRate").`type`().optional().stringType()
            .name("pseudoDisplay").`type`().optional().booleanType()
            .name("scale").`type`().optional().doubleType()
          .endRecord()
        .endRecord()
      .endRecord()

    val addonsType = SchemaBuilder
      .record("addons").fields()
        .name("activeAddons").`type`().optional().map().values().record("activeAddon").fields()
          .name("blocklisted").`type`().optional().booleanType()
          .name("description").`type`().optional().stringType()
          .name("name").`type`().optional().stringType()
          .name("userDisabled").`type`().optional().booleanType()
          .name("appDisabled").`type`().optional().booleanType()
          .name("version").`type`().optional().stringType()
          .name("scope").`type`().optional().intType()
          .name("type").`type`().optional().stringType()
          .name("foreignInstall").`type`().optional().booleanType()
          .name("hasBinaryComponents").`type`().optional().booleanType()
          .name("installDay").`type`().optional().longType()
          .name("updateDay").`type`().optional().longType()
          .name("signedState").`type`().optional().intType()
        .endRecord()
        .name("theme").`type`().optional().record("theme").fields()
          .name("id").`type`().optional().stringType()
          .name("blocklisted").`type`().optional().booleanType()
          .name("description").`type`().optional().stringType()
          .name("name").`type`().optional().stringType()
          .name("userDisabled").`type`().optional().booleanType()
          .name("appDisabled").`type`().optional().booleanType()
          .name("version").`type`().optional().stringType()
          .name("scope").`type`().optional().intType()
          .name("foreignInstall").`type`().optional().booleanType()
          .name("hasBinaryComponents").`type`().optional().booleanType()
          .name("installDay").`type`().optional().longType()
          .name("updateDay").`type`().optional().longType()
        .endRecord()
        .name("activePlugins").`type`().optional().array().items().record("activePlugin").fields()
          .name("name").`type`().optional().stringType()
          .name("version").`type`().optional().stringType()
          .name("description").`type`().optional().stringType()
          .name("blocklisted").`type`().optional().booleanType()
          .name("disabled").`type`().optional().booleanType()
          .name("clicktoplay").`type`().optional().booleanType()
          .name("mimeTypes").`type`().optional().array().items().stringType()
          .name("updateDay").`type`().optional().longType()
        .endRecord()
        .name("activeGMPlugins").`type`().optional().map().values().record("activeGMPlugin").fields()
          .name("version").`type`().optional().stringType()
          .name("userDisabled").`type`().optional().booleanType()
          .name("applyBackgroundUpdates").`type`().optional().booleanType()
        .endRecord()
        .name("activeExperiment").`type`().optional().record("activeExperiment").fields()
          .name("id").`type`().optional().stringType()
          .name("branch").`type`().optional().stringType()
        .endRecord()
        .name("persona").`type`().optional().stringType()
      .endRecord()

    val histogramType = SchemaBuilder
      .record("Histogram").fields()
        .name("values").`type`().array().items().longType().noDefault()
        .name("sum").`type`().longType().noDefault()
      .endRecord()

    val builder = SchemaBuilder
      .record("Submission").fields()
        .name("clientId").`type`().stringType().noDefault()
        .name("os").`type`().stringType().noDefault()
        .name("creationTimestamp").`type`().array().items().doubleType().noDefault()
        .name("application").`type`().optional().array().items(applicationType)
        .name("build").`type`().optional().array().items(buildType)
        .name("partner").`type`().optional().array().items(partnerType)
        .name("profile").`type`().optional().array().items(profileType)
        .name("settings").`type`().optional().array().items(settingsType)
        .name("system").`type`().optional().array().items(systemType)
        .name("addons").`type`().optional().array().items(addonsType)
    Histograms.definitions.foreach{ case (key, value) =>
      value match {
        case h: FlagHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().booleanType()
        case h: FlagHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().booleanType()
        case h: CountHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().longType()
        case h: CountHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().longType()
        case h: EnumeratedHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().array().items().longType()
        case h: EnumeratedHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().array().items().longType()
        case h: BooleanHistogram if h.keyed == false =>
          builder.name(key).`type`().optional().array().items().array().items().longType()
        case h: BooleanHistogram =>
          builder.name(key).`type`().optional().map().values().array().items().array().items().longType()
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
        def flatten(h: RawHistogram): Array[Long] = Array(h.values.getOrElse("0", 0L), h.values.getOrElse("1", 0L))
        vectorizeHistogram_(name, payloads, flatten, Array(0L, 0L))

      case _: CountHistogram =>
        // A count histograms is represented with a scalar.
        vectorizeHistogram_(name, payloads, h => h.values.getOrElse("0", 0L), 0L)

      case definition: EnumeratedHistogram =>
        // An enumerated histograms is represented with an array of N integers.
        def flatten(h: RawHistogram): Array[Long] = {
          val values = Array.fill(definition.nValues + 1){0L}
          h.values.foreach{case (key, value) =>
            values(key.toInt) = value
          }
          values
        }

        vectorizeHistogram_(name, payloads, flatten, Array.fill(definition.nValues + 1){0L})

      case definition: LinearHistogram =>
        // Exponential and linear histograms are represented with a struct containing
        // an array of N integers (values field) and the sum of entries (sum field).
        val buckets = Histograms.linearBuckets(definition.low, definition.high, definition.nBuckets)

        def flatten(h: RawHistogram): GenericData.Record = {
          val values = Array.fill(buckets.length){0L}
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
          record.put("values", Array.fill(buckets.length){0L})
          record.put("sum", 0)
          record
        }

        vectorizeHistogram_(name, payloads, flatten, empty)

      case definition: ExponentialHistogram =>
        val buckets = Histograms.exponentialBuckets(definition.low, definition.high, definition.nBuckets)
        def flatten(h: RawHistogram): GenericData.Record = {
          val values = Array.fill(buckets.length){0L}
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
          record.put("values", Array.fill(buckets.length){0L})
          record.put("sum", 0)
          record
        }

        vectorizeHistogram_(name, payloads, flatten, empty)
    }

  private def JSON2Avro(jsonField: String,
                        avroField: String,
                        payloads: List[Map[String, Any]],
                        root: GenericRecordBuilder,
                        schema: Schema) {
    implicit val formats = DefaultFormats

    val records = payloads.map{ case (x) =>
      parse(x.getOrElse(jsonField, return).asInstanceOf[String])
    }

    val fieldSchema = schema.getField(avroField).schema().getTypes()(1).getElementType()
    val field = records.map{ case (x) =>
      avro.JSON2Avro.parse(fieldSchema, x)
    }

    root.set(avroField, field.flatten.toArray)
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

    val histogramSchema = schema.getField("GC_MS").schema().getTypes()(1).getElementType()

    for ((key, value) <- validKeys) {
      val keyedHistogramsList = histogramsList.map{x =>
        x.get(key) match {
          case Some(x) => x
          case _ => Map[String, RawHistogram]()
        }
      }

      val uniqueLabels = keyedHistogramsList.flatMap(x => x.keys).distinct.toSet
      val vector = vectorizeHistogram(key, value, keyedHistogramsList, histogramSchema)
      val vectorized = for {
        label <- uniqueLabels
        vector = vectorizeHistogram(label, value, keyedHistogramsList, histogramSchema)
      } yield (label, vector)

      root.set(key, vectorized.toMap.asJava)
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

    val histogramSchema = schema.getField("GC_MS").schema().getTypes()(1).getElementType()

    for ((key, value) <- validKeys) {
      root.set(key, vectorizeHistogram(key, value, histogramsList, histogramSchema))
    }
  }

  private def buildRecord(history: Iterable[Map[String, Any]], schema: Schema): Option[GenericRecord] = {
    // Sort records by timestamp
    val sorted = history
      .toList
      .sortWith((x, y) => {
                 (x("creationTimestamp"), y("creationTimestamp")) match {
                   case (creationX: Double, creationY: Double) =>
                     creationX < creationY
                   case _ =>
                     return None  // Ignore 'unsortable' client
                 }
               })

    val root = new GenericRecordBuilder(schema)
      .set("clientId", sorted(0)("clientId").asInstanceOf[String])
      .set("os", sorted(0)("os").asInstanceOf[String])
      .set("creationTimestamp", sorted.map(x => x("creationTimestamp").asInstanceOf[Double]).toArray)

    try {
      JSON2Avro("application", "application", sorted, root, schema)
      JSON2Avro("environment.build", "build", sorted, root, schema)
      JSON2Avro("environment.partner", "partner", sorted, root, schema)
      JSON2Avro("environment.profile", "profile", sorted, root, schema)
      JSON2Avro("environment.settings", "settings", sorted, root, schema)
      JSON2Avro("environment.system", "system", sorted, root, schema)
      JSON2Avro("environment.addons", "addons", sorted, root, schema)
      keyedHistograms2Avro(sorted, root, schema)
      histograms2Avro(sorted, root, schema)
    } catch {
      //case _ : Throwable =>
      case e : Throwable =>
        // Ignore buggy clients
        return None
    }

    Some(root.build)
  }
}
