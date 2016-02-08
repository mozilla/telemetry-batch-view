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
    // See https://github.com/SamPenrose/data-pipeline/blob/v4-baseset-schema/schemas/test/environment.schema.json
    val buildType = SchemaBuilder
      .record("build").fields()
      .name("applicationId").`type`().optional().stringType()
      .name("architecture").`type`().optional().stringType()
      .name("architecturesInBinary").`type`().optional().stringType()
      .name("buildId").`type`().optional().stringType()
      .name("hotfixVersion").`type`().optional().stringType()
      .name("version").`type`().optional().stringType()
      .name("vendor").`type`().optional().stringType()
      .name("platformVersion").`type`().optional().stringType()
      .name("xpcomAbi").`type`().optional().stringType()
      .endRecord()

    val settingsType = SchemaBuilder
      .record("settings").fields()
      .name("blocklistEnabled").`type`().optional().booleanType()
      .name("isDefaultBrowser").`type`().optional().booleanType()
      .name("defaultSearchEngine").`type`().optional().booleanType()
      .name("e10sEnabled").`type`().optional().booleanType()
      .name("locale").`type`().optional().stringType()
      .name("telemetryEnabled").`type`().optional().booleanType()
      .endRecord()

    val systemType = SchemaBuilder
      .record("system").fields()
      .name("cpu").`type`().optional().record("cpu").fields()
        .name("count").`type`().optional().intType()
        .name("cores").`type`().optional().intType()
        .endRecord()
      .name("os").`type`().optional().record("os").fields()
        .name("name").`type`().optional().stringType()
        .name("locale").`type`().optional().stringType()
        .name("version").`type`().optional().stringType()
        .endRecord()
      .name("hdd").`type`().optional().record("hdd").fields()
        .name("profile").`type`().optional().record("profile").fields()
          .name("revision").`type`().optional().stringType()
          .name("model").`type`().optional().stringType()
          .endRecord()
        .endRecord()
        .name("binary").`type`().optional().`type`("profile")
        .name("system").`type`().optional().`type`("profile")
      .name("gfx").`type`().optional().record("gfx").fields()
        .name("adapters").`type`().optional().array().items().record("adapter").fields()
          .name("RAM").`type`().optional().intType()
          .name("description").`type`().optional().stringType()
          .name("deviceID").`type`().optional().stringType()
          .name("vendorID").`type`().optional().stringType()
          .name("GPUActive").`type`().optional().booleanType()
         .endRecord()
      .endRecord()
      .name("memoryMB").`type`().optional().intType()
    .endRecord()

    val builder = SchemaBuilder
      .record("Submission")
      .fields
      .name("clientId").`type`().stringType().noDefault()
      .name("os").`type`().stringType().noDefault()
      .name("creationTimestamp").`type`().array().items().doubleType().noDefault()
      .name("system").`type`().optional().array().items(systemType)
      .name("build").`type`().optional().array().items(buildType)
      .name("settings").`type`().optional().array().items(settingsType)

    val histogramType = SchemaBuilder
      .record("Histogram")
      .fields()
      .name("values").`type`().array().items().intType().noDefault()
      .name("sum").`type`().longType().noDefault()
      .endRecord()

    Histograms.definitions.foreach{ case (key, value) =>
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
      JSON2Avro("environment.system", "system", sorted, root, schema)
      JSON2Avro("environment.settings", "settings", sorted, root, schema)
      JSON2Avro("environment.build", "build", sorted, root, schema)
      keyedHistograms2Avro(sorted, root, schema)
      histograms2Avro(sorted, root, schema)
    } catch {
      case _ : Throwable =>
        // Ignore buggy clients
        return None
    }

    Some(root.build)
  }
}
