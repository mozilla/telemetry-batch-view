package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.SimpleDerivedStream
import telemetry.heka.{HekaFrame, Message}
import org.json4s.JsonAST.{JValue, JNothing}

case class Churn(prefix: String) extends SimpleDerivedStream{
  override def filterPrefix: String = prefix
  override def streamName: String = "telemetry"
  
  override def buildSchema: Schema = {
    SchemaBuilder
      .record("System").fields
      .name("clientId").`type`().stringType().noDefault()
      .name("sampleId").`type`().intType().noDefault()
      .name("channel").`type`().stringType().noDefault() // appUpdateChannel
      .name("normalizedChannel").`type`().stringType().noDefault() // normalizedChannel
      .name("country").`type`().stringType().noDefault() // geoCountry
      .name("profileCreationDate").`type`().intType().noDefault() // environment/profile/creationDate
      .name("submissionDate").`type`().stringType().noDefault()
      // See bug 1232050
      .name("syncConfigured").`type`().booleanType().noDefault() // WEAVE_CONFIGURED
//      .name("syncCountDesktop").`type`().intType().noDefault() // WEAVE_DEVICE_COUNT_DESKTOP
//      .name("syncCountMobile").`type`().intType().noDefault() // WEAVE_DEVICE_COUNT_MOBILE
      
      .name("version").`type`().stringType().noDefault() // appVersion
      .endRecord
  }

  def booleanHistogramToBoolean(h: JValue): Option[Boolean] = {
    if (h == JNothing) {
      return None
    }
    var one = h \ "1"
    if (one != JNothing && one.asInstanceOf[Int] > 0) {
      return Some(true)
    }
    var zero = h \ "0"
    if (zero != JNothing && zero.asInstanceOf[Int] > 0) {
      return Some(false)
    }
    
    return None
  }
  
  def enumHistogramToCount(h: JValue): Option[Long] = {
    Some(5)
  }

  override def buildRecord(message: Message, schema: Schema): Option[GenericRecord] ={
    val fields = HekaFrame.fields(message)
    val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
    val histograms = parse(fields.getOrElse("payload.histograms", "{}").asInstanceOf[String])
    
    val weaveConfigured = booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
//    val weaveDesktop = enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
//    val weaveMobile = enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")

    val root = new GenericRecordBuilder(schema)
      .set("clientId", fields.getOrElse("clientId", None) match {
             case x: String => x
             case _ => return None
           })
      .set("sampleId", fields.getOrElse("sampleId", None) match {
             case x: Long => x
             case _ => return None
           })
      .set("channel", fields.getOrElse("appUpdateChannel", None) match {
             case x: String => x
             case _ => ""
           })
      .set("normalizedChannel", fields.getOrElse("normalizedChannel", None) match {
             case x: String => x
             case _ => ""
           })
      .set("country", fields.getOrElse("geoCountry", None) match {
             case x: String => x
             case _ => ""
           })
      .set("version", fields.getOrElse("appVersion", None) match {
             case x: String => x
             case _ => ""
           })
      .set("submissionDate", fields.getOrElse("submissionDate", None) match {
             case x: String => x
             case _ => return None
           })
      .set("profileCreationDate", (profile \ "creationDate").asInstanceOf[Long])
      .set("syncConfigured", weaveConfigured match {
             case Some(x) => x
             case _ => return None
           })
      // TODO
//      .set("syncCountDesktop", weaveDesktop match {
//             case Some(x) => x
//             case _ => return None
//           })
//      .set("syncCountMobile", weaveMobile match {
//             case Some(x) => x
//             case _ => return None
//           })
      .build

    Some(root)
  }
}
