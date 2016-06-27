package com.mozilla.telemetry.views

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import org.json4s.JsonAST.{JBool, JInt, JString}
import org.json4s.jackson.JsonMethods.parse
import org.rogach.scallop._
import com.mozilla.telemetry.parquet.ParquetFile
import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils._

object ChurnView {
  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = true)
    val to = opt[String]("to", descr = "To submission date", required = true)
    val outputBucket = opt[String]("bucket", descr = "bucket", required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val from = opts.from()
    val to = opts.to()

    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = formatter.parseDateTime(from)
    val toDate = formatter.parseDateTime(to)
    val daysCount = Days.daysBetween(fromDate, toDate).getDays()

    val sparkConf = new SparkConf().setAppName("Churn")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    implicit val sc = new SparkContext(sparkConf)
    val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

    for (i <- 0 to daysCount) {
      val currentDay = fromDate.plusDays(i).toString("yyyyMMdd")
      logger.info(s"Processing day: ${currentDay}")

      val messages = Dataset("telemetry")
        .where("sourceName") {
          case "telemetry" => true
        }.where("sourceVersion") {
          case "4" => true
        }.where("docType") {
          case "main" => true
        }.where("appName") {
          case "Firefox" => true
        }.where("submissionDate") {
          case date if date == currentDay => true
        }

      run(messages, opts.outputBucket(), currentDay)
    }
  }

  private def run(messages: RDD[Message], outputBucket: String, currentDay: String): Unit = {
    val clsName = uncamelize(this.getClass.getSimpleName.replace("$", ""))
    val prefix = s"${clsName}/v1/submission_date_s3=${currentDay}"

    require(S3Store.isPrefixEmpty(outputBucket, prefix), s"s3://${outputBucket}/${prefix} already exists!")

    messages
      .flatMap(messageToMap)
      .repartition(100) // TODO: partition by sampleId
      .foreachPartition { partitionIterator =>
        val schema = buildSchema
        val records = for {
          record <- partitionIterator
          saveable <- buildRecord(record, schema)
        } yield saveable

        while(records.nonEmpty) {
          val localFile = new java.io.File(ParquetFile.serialize(records, schema).toUri)
          S3Store.uploadFile(localFile, outputBucket, prefix)
          localFile.delete()
        }
    }
  }

  private def messageToMap(message: Message): Option[Map[String,Any]] = {
    val fields = message.fieldsAsMap

    // Don't compute the expensive stuff until we need it. We may skip a record
    // due to missing simple fields.
    lazy val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
    lazy val partner = parse(fields.getOrElse("environment.partner", "{}").asInstanceOf[String])
    lazy val settings = parse(fields.getOrElse("environment.settings", "{}").asInstanceOf[String])
    lazy val info = parse(fields.getOrElse("payload.info", "{}").asInstanceOf[String])
    lazy val histograms = parse(fields.getOrElse("payload.histograms", "{}").asInstanceOf[String])

    lazy val weaveConfigured = MainPing.booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
    lazy val weaveDesktop = MainPing.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
    lazy val weaveMobile = MainPing.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")

    val map = Map[String, Any](
      "clientId" -> (fields.getOrElse("clientId", None) match {
        case x: String => x
        // clientId is required, and must be a string. If either
        // condition is not satisfied, we skip this record.
        case _ => return None
      }),
      "sampleId" -> (fields.getOrElse("sampleId", None) match {
        case x: Long => x
        case x: Double => x.toLong
        case _ => return None // required
      }),
      "submissionDate" -> (fields.getOrElse("submissionDate", None) match {
        case x: String => x
        case _ => return None // required
      }),
      "timestamp" -> message.timestamp, // required
      "channel" -> (fields.getOrElse("appUpdateChannel", None) match {
        case x: String => x
        case _ => ""
      }),
      "normalizedChannel" -> (fields.getOrElse("normalizedChannel", None) match {
        case x: String => x
        case _ => ""
      }),
      "country" -> (fields.getOrElse("geoCountry", None) match {
        case x: String => x
        case _ => ""
      }),
      "version" -> (fields.getOrElse("appVersion", None) match {
        case x: String => x
        case _ => ""
      }),
      "profileCreationDate" -> ((profile \ "creationDate") match {
        case x: JInt => x.num.toLong
        case _ => null
      }),
      "syncConfigured" -> weaveConfigured.getOrElse(null),
      "syncCountDesktop" -> weaveDesktop.getOrElse(null),
      "syncCountMobile" -> weaveMobile.getOrElse(null),
      "subsessionStartDate" -> ((info \ "subsessionStartDate") match {
        case JString(x) => x
        case _ => null
      }),
      "subsessionLength" -> ((info \ "subsessionLength") match {
        case x: JInt => x.num.toLong
        case _ => null
      }),
      "distributionId" -> ((partner \ "distributionId") match {
        case JString(x) => x
        case _ => null
      }),
      "e10sEnabled" -> ((settings \ "e10sEnabled") match {
        case JBool(x) => x
        case _ => null
      }),
      "e10sCohort" -> ((settings \ "e10sCohort") match {
        case JString(x) => x
        case _ => null
      })
    )
    Some(map)
  }

  private def buildSchema: Schema = {
    SchemaBuilder
      .record("System").fields
      .name("clientId").`type`().stringType().noDefault()
      .name("sampleId").`type`().intType().noDefault()
      .name("channel").`type`().stringType().noDefault() // appUpdateChannel
      .name("normalizedChannel").`type`().stringType().noDefault() // normalizedChannel
      .name("country").`type`().stringType().noDefault() // geoCountry
      // TODO: use proper 'date' type for date columns.
      .name("profileCreationDate").`type`().nullable().intType().noDefault() // environment/profile/creationDate
      .name("subsessionStartDate").`type`().nullable().stringType().noDefault() // info/subsessionStartDate
      .name("subsessionLength").`type`().nullable().intType().noDefault() // info/subsessionLength
      .name("distributionId").`type`().nullable().stringType().noDefault() // environment/partner/distributionId
      .name("submissionDate").`type`().stringType().noDefault()
      // See bug 1232050
      .name("syncConfigured").`type`().nullable().booleanType().noDefault() // WEAVE_CONFIGURED
      .name("syncCountDesktop").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_DESKTOP
      .name("syncCountMobile").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_MOBILE

      .name("version").`type`().stringType().noDefault() // appVersion
      .name("timestamp").`type`().longType().noDefault() // server-assigned timestamp when record was received

      // See bug 1251259
      .name("e10sEnabled").`type`().nullable.booleanType().noDefault() // environment/settings/e10sEnabled
      .name("e10sCohort").`type`().nullable.stringType().noDefault() // environment/settings/e10sCohort
      .endRecord
  }

  private def buildRecord(fields: Map[String,Any], schema: Schema): Option[GenericRecord] ={
    val root = new GenericRecordBuilder(schema)
    for ((k, v) <- fields) root.set(k, v)
    Some(root.build)
  }
}

