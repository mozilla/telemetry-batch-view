package telemetry.streams

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.DerivedStream
import telemetry.heka.{HekaFrame, Message}

object ExampleStream extends DerivedStream{
  private val SEC_IN_HOUR = 60 * 60
  private val SEC_IN_DAY = SEC_IN_HOUR * 24

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def buildSchema: Schema = {
    SchemaBuilder
      .record("System").fields
      .name("docType").`type`().stringType().noDefault()
      .name("submissionDate").`type`().stringType().noDefault()
      .name("creationTimestamp").`type`().doubleType().noDefault()
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
      .endRecord
  }

  def buildRecord(message: Message, schema: Schema): Option[GenericRecord] ={
    val fields = HekaFrame.fields(message)

    val docType = fields.getOrElse("docType", None) match {
      case x: String if List("main", "crash").contains(x) => x
      case _ => return None
    }

    val environment_profile = fields("environment.profile") match {
      case x: String => parse(x)
      case _ => return None
    }

    val environment_system = fields("environment.system") match {
      case x: String => parse(x)
      case _ => return None
    }

    val environment_settings = fields("environment.settings") match {
      case x: String => parse(x)
      case _ => return None
    }

    val payload_info = fields("payload.info") match {
      case x: String => parse(x)
      case _ => return None
    }

    val keyedHistograms = fields("payload.keyedHistograms") match {
      case x: String => parse(x)
      case _ => return None
    }

    val searches = for {
      JObject(histogram) <- keyedHistograms \\ "SEARCH_COUNTS"
      JField(key, value) <- histogram
      engine = key match {
        case r"[gG]oogle.*" => "google"
        case r"[bB]ing.*" => "bing"
        case r"[yY]ahoo.*" => "yahoo"
        case _ => ""
      }
      count = value \\ "sum" match {
        case JInt(x) => x
        case _ => 0: BigInt
      }
      if engine != ""
    } yield (engine, count)

    val totalSearches = searches
      .groupBy(_._1)
      .map{case (k, v) => v.reduce((x, y) => (x._1, x._2 + y._2)) }

    val root = new GenericRecordBuilder(schema)
      .set("docType", docType)
      .set("submissionDate", fields.getOrElse("submissionDate", None) match {
             case x: String => x
             case _ => return None
           })
      .set("creationTimestamp", fields.getOrElse("creationTimestamp", None) match {
             case x: Double => x
             case _ => return None
           })
      .set("profileCreationTimestamp", environment_profile \\ "creationDate" match {
             case JInt(x) => (x * SEC_IN_DAY).toDouble * 1e9
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
      .set("country", fields.getOrElse("geoCountry", None) match {
             case x: String => x
             case _ => ""
           })
      .set("channel", fields.getOrElse("appUpdateChannel", None) match {
             case x: String => x
             case _ => ""
           })
      .set("os", fields.getOrElse("os", None) match {
             case x: String => x
             case _ => ""
           })
      .set("osVersion", environment_system \\ "os" \\ "version" match {
             case JString(x) => x
             case _ => ""
           })
      .set("default", environment_settings \\ "isDefaultBrowser" match {
             case JBool(x) => x
             case _ => ""
           })
      .set("buildId", fields.getOrElse("appBuildId", None) match {
             case x: String => x
             case _ => ""
           })
      .set("app", fields.getOrElse("appName", None) match {
             case x: String => x
             case _ => ""
           })
      .set("version", fields.getOrElse("appVersion", None) match {
             case x: String => x
             case _ => ""
           })
      .set("vendor", fields.getOrElse("appVendor", None) match {
             case x: String => x
             case _ => ""
           })
      .set("reason", fields.getOrElse("reason", None) match {
             case x: String => x
             case _ => ""
           })
      .set("hours", payload_info \\ "subsessionLength" match {
             case JInt(x) if (x > 0) && (x < 180 * SEC_IN_DAY) => x.toDouble / SEC_IN_HOUR
             case _ => 0
           })
      .set("google", totalSearches.getOrElse("google", 0))
      .set("yahoo", totalSearches.getOrElse("yahoo", 0))
      .set("bing", totalSearches.getOrElse("bing", 0))
      .build

    Some(root)
  }

  def main(args: Array[String]) = {
    simulateEvent("net-mozaws-prod-us-west-2-pipeline-data", "telemetry-2/20151001/telemetry/4/main/Firefox/nightly/44.0a1/20150924030231/20151002225145.480_ip-172-31-32-149")
  }
}
