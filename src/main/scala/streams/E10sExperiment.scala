package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.BatchDerivedStream
import telemetry.heka.{HekaFrame, Message}
import org.json4s._
import org.json4s.native.JsonMethods._

case class E10sExperiment(experimentId: String, prefix: String) extends BatchDerivedStream{
  def buildSchema: Schema = {
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

  def streamName: String = "telemetry"
  override def filterPrefix: String = prefix

  override def prefixGroup(key: String): String = {
    key.split("/").take(2).mkString("/")
  }

  def buildRecord(message: Message, schema: Schema): Option[GenericRecord] ={
    val fields = HekaFrame.fields(message)

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
