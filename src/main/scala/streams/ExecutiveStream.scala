package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.BatchDerivedStream
import telemetry.heka.{HekaFrame, Message}

object ExecutiveStream extends BatchDerivedStream{
  def buildSchema: Schema = {
    SchemaBuilder
      .record("System").fields
      .name("docType").`type`().stringType().noDefault()
      .name("submissionDate").`type`().stringType().noDefault()
      .name("activityTimestamp").`type`().doubleType().noDefault()
      .name("profileCreationTimestamp").`type`().doubleType().noDefault()
      .name("clientId").`type`().stringType().noDefault()
      .name("documentId").`type`().stringType().noDefault()
      .name("country").`type`().stringType().noDefault()
      .name("channel").`type`().stringType().noDefault()
      .name("os").`type`().stringType().noDefault()
      .name("osVersion").`type`().stringType().noDefault()
      .name("default").`type`().booleanType().noDefault()
      .name("buildId").`type`().stringType().noDefault()
      .name("app").`type`().stringType().noDefault()
      .name("version").`type`().stringType().noDefault()
      .name("vendor").`type`().stringType().noDefault()
      .name("reason").`type`().stringType().noDefault()
      .name("hours").`type`().doubleType().noDefault()
      .name("google").`type`().intType().noDefault()
      .name("yahoo").`type`().intType().noDefault()
      .name("bing").`type`().intType().noDefault()
      .name("other").`type`().intType().noDefault()
      .name("pluginHangs").`type`().intType().noDefault()
      .endRecord
  }

  def streamName: String = "telemetry-executive-summary"

  def buildRecord(message: Message, schema: Schema): Option[GenericRecord] ={
    val fields = HekaFrame.fields(message)

    val root = new GenericRecordBuilder(schema)
      .set("docType", fields.getOrElse("docType", None) match {
             case x: String => x
             case _ => return None
           })
      .set("submissionDate", fields.getOrElse("submissionDate", None) match {
             case x: String => x
             case _ => return None
           })
      .set("activityTimestamp", fields.getOrElse("activityTimestamp", None) match {
             case x: Double => x
             case _ => return None
           })
      .set("profileCreationTimestamp", fields.getOrElse("profileCreationTimestamp", None) match {
             case x: Double => x
             case _ => 0
           })
      .set("clientId", fields.getOrElse("clientId", None) match {
             case x: String => x
             case _ => return None
           })
      .set("documentId", fields.getOrElse("documentId", None) match {
             case x: String => x
             case _ => return None
           })
      .set("country", fields.getOrElse("country", None) match {
             case x: String => x
             case _ => ""
           })
      .set("channel", fields.getOrElse("channel", None) match {
             case x: String => x
             case _ => ""
           })
      .set("os", fields.getOrElse("os", None) match {
             case x: String => x
             case _ => ""
           })
      .set("osVersion", fields.getOrElse("osVersion", None) match {
             case x: String => x
             case _ => ""
           })
      .set("default", fields.getOrElse("default", None) match {
             case x: Boolean => x
             case _ => return None
           })
      .set("buildId", fields.getOrElse("buildId", None) match {
             case x: String => x
             case _ => ""
           })
      .set("app", fields.getOrElse("app", None) match {
             case x: String => x
             case _ => ""
           })
      .set("version", fields.getOrElse("version", None) match {
             case x: String => x
             case _ => ""
           })
      .set("vendor", fields.getOrElse("vendor", None) match {
             case x: String => x
             case _ => ""
           })
      .set("reason", fields.getOrElse("reason", None) match {
             case x: String => x
             case _ => ""
           })
      .set("hours", fields.getOrElse("hours", None) match {
             case x: Double => x
             case _ => return None
           })
      .set("google", fields.getOrElse("google", None) match {
             case x: Long => x
             case _ => 0
           })
      .set("yahoo", fields.getOrElse("yahoo", None) match {
             case x: Long => x
             case _ => 0
           })
      .set("bing", fields.getOrElse("bing", None) match {
             case x: Long => x
             case _ => 0
           })
      .set("other", fields.getOrElse("other", None) match {
             case x: Long => x
             case _ => 0
           })
      .set("pluginHangs", fields.getOrElse("pluginHangs", None) match {
             case x: Long => x
             case _ => 0
           })
      .build

    Some(root)
  }

  def main(args: Array[String]) = {
    // Used only for testing & debugging purposes
    implicit val s3 = S3()
    val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-data")
    val prefix = "telemetry-executive-summary-3/20151027/nightly"
    val keys = s3.objectSummaries(bucket, prefix).take(2).toIterator
    ExecutiveStream.transform(bucket, keys, prefix)
  }
}
