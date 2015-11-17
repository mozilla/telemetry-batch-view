package telemetry.streams

import awscala._
import awscala.s3._
import com.github.nscala_time.time.Imports._
import java.io.File
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.{DerivedStream, ObjectSummary, Partitioning}
import telemetry.heka.{HekaFrame, Message}
import telemetry.parquet.ParquetFile

case class E10sExperiment(experimentId: String, prefix: String) extends DerivedStream {
  override def streamName: String = "telemetry"

  override def filterPrefix: String = prefix

  override def transform(sc: SparkContext, bucket: Bucket, summaries: RDD[ObjectSummary], from: String, to: String) {
    val clientMessages = summaries
      .repartition(summaries.count().toInt)
      .flatMap{ case obj =>
        implicit val s3 = S3()
        val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3"))
        for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message
      }.flatMap{ case message =>
        val fields = HekaFrame.fields(message)
        val clientId = fields.get("clientId")
        val creationTimestamp = fields.get("creationTimestamp")

        (clientId, creationTimestamp) match {
          case (Some(id: String), Some(time: Double)) => List((id, (time, fields)))
          case _ => Nil
        }
      }

    val firstSessionTimestampByClient = clientMessages
      .mapValues(_._1)
      .reduceByKey((x, y) => if (x < y) x else y)
      .collectAsMap()

    val representativeSubmissions = clientMessages
       // TODO: re-enable
       // TODO: add check to see if derived stream has already been generated
       //.filter{case (clientId, payload) => payload._1 != firstSessionTimestampByClient(clientId)}
      .reduceByKey{ case (x, y) => x }
      .map{ case (clientId, payload) => payload._2 }
      .repartition(sc.defaultParallelism)
      .foreachPartition{ case fieldsIterator =>
        val schema = buildSchema
        val records = for {
          fields <- fieldsIterator
          record <- buildRecord(fields, schema)
          } yield record

        while(!records.isEmpty) {
          val localFile = ParquetFile.serialize(records, schema, chunked=true)
          uploadLocalFileToS3(localFile, s"generationDate=$to")
          new File(localFile).delete()
        }
      }
  }

  private def buildSchema: Schema = {
    SchemaBuilder
      .record("Submission").fields
      .name("clientId").`type`().stringType().noDefault()
      .name("creationTimestamp").`type`().stringType().noDefault()
      .name("submissionDate").`type`().stringType().noDefault()
      .name("documentId").`type`().stringType().noDefault()
      .name("sampleId").`type`().stringType().noDefault()
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
    val root = new GenericRecordBuilder(schema)
      .set("addons", fields.getOrElse("environment.addons", None) match {
             case addons: String =>
               parse(addons) \ "activeExperiment" \ "id" match {
                 case JString(id) if id == experimentId =>
                   addons
                 case _ =>
                   return None
               }
             case _ => return None
           })
      .set("clientId", fields.getOrElse("clientId", ""))
      .set("creationTimestamp", fields.getOrElse("creationTimestamp", ""))
      .set("submissionDate", fields.getOrElse("submissionDate", ""))
      .set("documentId", fields.getOrElse("documentId", ""))
      .set("sampleId", fields.getOrElse("sampleId", ""))
      .set("simpleMeasurements", fields.getOrElse("payload.simpleMeasurements", ""))
      .set("settings", fields.getOrElse("environment.settings", ""))
      .set("threadHangStats", fields.getOrElse("payload.threadHangStats", ""))
      .set("histograms", fields.getOrElse("payload.histograms", ""))
      .set("keyedHistograms", fields.getOrElse("payload.keyedHistograms", ""))
      .set("childPayloads", fields.getOrElse("payload.childPayloads", "{}"))
      .build

    Some(root)
  }
}
