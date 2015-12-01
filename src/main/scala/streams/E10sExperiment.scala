package telemetry.streams

import awscala._
import awscala.s3._
import java.io.File
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.spark.{SparkContext, Partitioner}
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import scala.util.Random
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.heka.{HekaFrame, Message}
import telemetry.parquet.ParquetFile

private class SampleIdPartitioner extends Partitioner{
  def numPartitions: Int = 100
  def getPartition(key: Any): Int = key match {
    case (_, sampleId: Int) => sampleId
    case _ => throw new Exception("Invalid key")
  }
}

case class E10sExperiment(experimentId: String, prefix: String) extends DerivedStream {
  override def streamName: String = "telemetry"

  override def filterPrefix: String = prefix

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
        val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3"))
        for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message }
      .flatMap{ case message =>
        val fields = HekaFrame.fields(message)
        val clientId = fields.get("clientId")
        val sampleId = fields.get("sampleId")

        val addons = parse(fields.getOrElse("environment.addons", "{}").asInstanceOf[String])
        val id = addons \ "activeExperiment" \ "id"
        val branch = addons \ "activeExperiment" \ "branch"

        // Ignore the first session of the experiment
        val log = parse(fields.getOrElse("payload.log", "[]").asInstanceOf[String])
        val status = for {
          JArray(entries) <- log
          JArray(entry) <- entries
          JString(key) <- entry(0)
          if key == "EXPERIMENT_ACTIVATION"
          JString(status) <- entry(2)
          if status == "ACTIVATED"
          JString(exp) <- entry(3)
          if exp == experimentId
        } yield status

        (clientId, sampleId, id, branch) match {
          case (Some(client: String), Some(sample: Double), JString(id), JString(branch)) if id == experimentId && (branch == "control" || branch == "experiment") && status.isEmpty =>
            List(((client, sample.toInt), fields))
          case _ => Nil
        }
      }

    val representativeSubmissions = clientMessages
      .mapValues{case x => (x, 1)}
      .reduceByKey{ case ((p1, n1), (p2, n2)) =>
        // Every submission should have the same probability of being chosen
        val n = n1 + n2
        val rnd = new Random().nextFloat()
        if (n * rnd < n1) (p1, n) else (p2, n)
      }
      .mapValues{case x => x._1}
      .partitionBy(new SampleIdPartitioner())
      .values
      .foreachPartition{ case fieldsIterator =>
        val schema = buildSchema
        val records = for {
          fields <- fieldsIterator
          record <- buildRecord(fields, schema)
          } yield record

        while(!records.isEmpty) {
          val localFile = ParquetFile.serialize(records, schema)
          uploadLocalFileToS3(localFile, prefix)
        }
      }
  }

  private def buildSchema: Schema = {
    SchemaBuilder
      .record("Submission").fields
      .name("clientId").`type`().stringType().noDefault()
      .name("experimentBranch").`type`().stringType().noDefault()
      .name("creationTimestamp").`type`().stringType().noDefault()
      .name("submissionDate").`type`().stringType().noDefault()
      .name("documentId").`type`().stringType().noDefault()
      .name("sampleId").`type`().intType().noDefault()
      .name("simpleMeasurements").`type`().stringType().noDefault()
      .name("settings").`type`().stringType().noDefault()
      .name("addons").`type`().stringType().noDefault()
      .name("threadHangStats").`type`().stringType().noDefault()
      .name("histograms").`type`().stringType().noDefault()
      .name("keyedHistograms").`type`().stringType().noDefault()
      .name("childPayloads").`type`().stringType().noDefault()
      .endRecord
  }

  private def buildRecord(fields: Map[String, Any], schema: Schema): Option[GenericRecord] ={
    val addons = fields.getOrElse("environment.addons", "{}").asInstanceOf[String]
    val JString(branch) = parse(addons) \ "activeExperiment" \ "branch"

    val root = new GenericRecordBuilder(schema)
      .set("clientId", fields.getOrElse("clientId", ""))
      .set("experimentBranch", branch)
      .set("creationTimestamp", fields.getOrElse("creationTimestamp", ""))
      .set("submissionDate", fields.getOrElse("submissionDate", ""))
      .set("documentId", fields.getOrElse("documentId", ""))
      .set("sampleId", fields.getOrElse("sampleId", -1))
      .set("simpleMeasurements", fields.getOrElse("payload.simpleMeasurements", ""))
      .set("settings", fields.getOrElse("environment.settings", ""))
      .set("addons", addons)
      .set("threadHangStats", fields.getOrElse("payload.threadHangStats", ""))
      .set("histograms", fields.getOrElse("payload.histograms", ""))
      .set("keyedHistograms", fields.getOrElse("payload.keyedHistograms", ""))
      .set("childPayloads", fields.getOrElse("payload.childPayloads", "{}"))
      .build

    Some(root)
  }
}
