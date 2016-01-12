package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.SimpleDerivedStream
import telemetry.heka.{HekaFrame, Message}
import org.json4s.JsonAST.{JValue, JNothing, JInt, JObject}

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
      .name("profileCreationDate").`type`().nullable().intType().noDefault() // environment/profile/creationDate
      .name("submissionDate").`type`().stringType().noDefault()
      // See bug 1232050
      .name("syncConfigured").`type`().nullable().booleanType().noDefault() // WEAVE_CONFIGURED
      .name("syncCountDesktop").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_DESKTOP
      .name("syncCountMobile").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_MOBILE
      
      .name("version").`type`().stringType().noDefault() // appVersion
      .endRecord
  }

  // Given histogram h, return true if it has a value in the "true" bucket,
  // or false if it has a value in the "false" bucket, or None otherwise.
  def booleanHistogramToBoolean(h: JValue): Option[Boolean] = {
    val one = (h \ "values" \ "1") match {
      case x: JInt => x.num.toInt
      case _ => -1
    }
    var result: Option[Boolean] = None
    if (one > 0) {
      println("boolean true")
      result = Some(true)
    } else {
      val zero = (h \ "values" \ "0") match {
        case x: JInt => x.num.toInt
        case _ => -1
      }
      if (zero > 0) {
        println("boolean false")
        result = Some(false)
      }
    }
    result
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
        for ((k, v) <- x.asInstanceOf[Map[String,Any]]) {
          val bucket = toInt(k).getOrElse(-1)
          if (bucket > topBucket) {
            val count = v match {
              case x: Int => x
              case x: Long => x
              case x: JInt => x.num.toInt
              case _ => -1
            }
            if (count > 0) {
              topBucket = bucket
            }
          }
        }
        if (topBucket >= 0) {
          Some(topBucket)
        } else {
          None
        }
      }
      case _ => {
        println("Unexpected format for enum histogram")
        None
      }
    }
    None
  }

  override def buildRecord(message: Message, schema: Schema): Option[GenericRecord] ={
    val fields = HekaFrame.fields(message)
    val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
    val histograms = parse(fields.getOrElse("payload.histograms", "{}").asInstanceOf[String])
    
    val weaveConfigured = booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
    val weaveDesktop = enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
    val weaveMobile = enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")
    val root = new GenericRecordBuilder(schema)
      .set("clientId", fields.getOrElse("clientId", None) match {
             case x: String => x
             case _ => {
               println("skip: no clientid")
               return None
             }
           })
      .set("sampleId", fields.getOrElse("sampleId", None) match {
             case x: Long => x
             case x: Double => x.toLong
             case _ => {
               println("skip: no sampleid")
               return None
             }
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
             case _ => {
               println("skip: no subdate")
               return None
             }
           })
      .set("profileCreationDate", (profile \ "creationDate") match {
             case JNothing => null
             case x: JInt => x.num.toLong
             case _ => {
               println("profile creation date was not an int")
               null
             }
      })
      .set("syncConfigured", weaveConfigured match {
             case Some(x) => x
             case _ => null
           })
      .set("syncCountDesktop", weaveDesktop match {
             case Some(x) => x
             case _ => null
           })
      .set("syncCountMobile", weaveMobile match {
             case Some(x) => x
             case _ => null
           })
      .build
//    println("Matched one record")
    Some(root)
  }
}
