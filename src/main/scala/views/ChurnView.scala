package telemetry.streams

import awscala.s3.{S3, Bucket}
import com.typesafe.config._
import org.apache.spark.SparkContext
import org.apache.spark.{SparkConf, SparkContext, Accumulator}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import telemetry.DerivedStream.s3
import telemetry.streams.main_summary.Utils
import telemetry.utils.Telemetry
import org.joda.time.{format, DateTime, Days}
import org.rogach.scallop._

object ChurnView {
  // configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val to = conf.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }
    val from = conf.from.get match {
      case Some(f) => fmt.parseDateTime(f)
      case _ => DateTime.now.minusDays(1)
    }

    // set up Spark
    val sparkConf = new SparkConf().setAppName("ChurnView")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    val appConf = ConfigFactory.load()
    val parquetBucket = appConf.getString("app.parquetBucket")

    for (offset <- 0 to Days.daysBetween(from, to).getDays()) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")

      val schema = buildSchema()
      val messages = Telemetry.getRecords(sc, currentDate, List("telemetry", "4", "main", "Firefox"))
      val rowRDD = messages.flatMap(messageToRow).repartition(100) // TODO: partition by sampleId
      val records = sqlContext.createDataFrame(rowRDD.coalesce(1), schema)
      records.write.mode(SaveMode.Overwrite).parquet(s"s3://$parquetBucket/churn/v1/submission_date_s3=$currentDateString")

      println("=======================================================================================")
      println(s"JOB COMPLETED SUCCESSFULLY FOR $currentDate")
      println("=======================================================================================")
    }
  }

  // Convert a message to a row containing the schema fields
  def messageToRow(message: Map[String, Any]): Option[Row] = {
    // Don't compute the expensive stuff until we;ve confirmed that all the simple fields are valid
    lazy val profile = parse(message.getOrElse("environment.profile", "{}").asInstanceOf[String])
    lazy val partner = parse(message.getOrElse("environment.partner", "{}").asInstanceOf[String])
    lazy val settings = parse(message.getOrElse("environment.settings", "{}").asInstanceOf[String])
    lazy val info = parse(message.getOrElse("payload.info", "{}").asInstanceOf[String])
    lazy val histograms = parse(message.getOrElse("payload.histograms", "{}").asInstanceOf[String])

    lazy val weaveConfigured = Utils.booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
    lazy val weaveDesktop = Utils.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
    lazy val weaveMobile = Utils.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")

    val row = Row( // the entries in this correspond to the schema in `buildSchema()`
      message.getOrElse("clientId", None) match {
        case x: String => x
        case _ => return None // required
      },
      message.getOrElse("sampleId", None) match {
        case x: Long => x
        case x: Double => x.toLong
        case _ => return None // required
      },
      message.getOrElse("submissionDate", None) match {
        case x: String => x
        case _ => return None // required
      },
      message.getOrElse("timestamp", None) match {
        case x: String => x
        case _ => return None // required
      },
      message.getOrElse("appUpdateChannel", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("normalizedChannel", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("geoCountry", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("appVersion", None) match {
        case x: String => x
        case _ => ""
      },
      (profile \ "creationDate") match {
        case x: JInt => x.num.toLong
        case _ => null
      },
      weaveConfigured.getOrElse(null),
      weaveDesktop.getOrElse(null),
      weaveMobile.getOrElse(null),
      (info \ "subsessionStartDate") match {
        case JString(x) => x
        case _ => null
      },
      (info \ "subsessionLength") match {
        case x: JInt => x.num.toLong
        case _ => null
      },
      (partner \ "distributionId") match {
        case JString(x) => x
        case _ => null
      },
      (settings \ "e10sEnabled") match {
        case JBool(x) => x
        case _ => null
      },
      (settings \ "e10sCohort") match {
        case JString(x) => x
        case _ => null
      }
    )
    Some(row)
  }

  def buildSchema(): StructType = {
    StructType(
      StructField("clientId",            StringType,  false) ::
      StructField("sampleId",            IntegerType, false) ::
      StructField("channel",             StringType,  false) :: // appUpdateChannel
      StructField("normalizedChannel",   StringType,  false) :: // normalizedChannel
      StructField("country",             StringType,  false) :: // geoCountry
      StructField("profileCreationDate", IntegerType, true) :: // environment/profile/creationDate
      StructField("subsessionStartDate", StringType,  true) :: // info/subsessionStartDate
      StructField("subsessionLength",    IntegerType, true) :: // info/subsessionLength
      StructField("distributionId",      StringType,  true) :: // environment/partner/distributionId
      StructField("submissionDate",      StringType,  false) ::

      // bug 1232050
      StructField("syncConfigured",      BooleanType, true) :: // WEAVE_CONFIGURED
      StructField("syncCountDesktop",    IntegerType, true) :: // WEAVE_DEVICE_COUNT_DESKTOP
      StructField("syncCountMobile",     IntegerType, true) :: // WEAVE_DEVICE_COUNT_MOBILE

      StructField("version",             StringType,  false) :: // appVersion
      StructField("timestamp",           LongType,    false) :: // server-assigned timestamp when record was received

      // bug 1251259
      StructField("e10sEnabled",         BooleanType, false) :: // environment/settings/e10sEnabled
      StructField("e10sCohort",          StringType,  false) :: // environment/settings/e10sCohort
      Nil
    )
  }
}
