package telemetry.streams

import awscala._
import awscala.s3._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.{SimpleDerivedStream,Partitioning}
import telemetry.heka.{HekaFrame, Message}
import org.json4s.JsonAST.{JValue, JNothing, JInt, JObject}

case class Churn(prefix: String) extends SimpleDerivedStream{
  override def filterPrefix: String = prefix
  override def streamName: String = "telemetry"
  override val partitioning: Partitioning = {
    // Use a specific partitioning scheme for churn data. Data set is small, so we don't need many partitions.
    val churnSchema = """{
  "version": 2,
  "dimensions": [
    {
      "field_name": "submissionDate",
      "allowed_values": "*"
    }
  ]
}"""
    Partitioning(churnSchema)
  }
  
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
      .name("timestamp").`type`().longType().noDefault()
      .endRecord
  }

  // Check if a json value contains a number greater than zero.
  def gtZero(v: JValue): Boolean = {
    v match {
      case x: JInt => x.num.toInt > 0
      case _ => false
    }
  }

  // Given histogram h, return true if it has a value in the "true" bucket,
  // or false if it has a value in the "false" bucket, or None otherwise.
  def booleanHistogramToBoolean(h: JValue): Option[Boolean] = {
    (gtZero(h \ "values" \ "1"), gtZero(h \ "values" \ "0")) match {
      case (true, _) => Some(true)
      case (_, true) => Some(false)
      case _ => None
    }
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
        for {
          (k, v) <- x
          b <- toInt(k) if b > topBucket && gtZero(v)
        } topBucket = b

        if (topBucket >= 0) {
          Some(topBucket)
        } else {
          None
        }
      }
      case _ => {
        None
      }
    }
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
               // Skip: no client id
               return None
             }
           })
      .set("sampleId", fields.getOrElse("sampleId", None) match {
             case x: Long => x
             case x: Double => x.toLong
             case _ => {
               // Skip: no sample id
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
               // Skip: no submission date
               return None
             }
           })
      .set("profileCreationDate", (profile \ "creationDate") match {
             case JNothing => null
             case x: JInt => x.num.toLong
             case _ => {
               // Profile creation date was not an int
               null
             }
      })
      .set("syncConfigured", weaveConfigured.getOrElse(null))
      .set("syncCountDesktop", weaveDesktop.getOrElse(null))
      .set("syncCountMobile", weaveMobile.getOrElse(null))
      .set("timestamp", message.timestamp)
      .build
    Some(root)
  }
}
