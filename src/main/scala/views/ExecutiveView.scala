package telemetry.views

import awscala._
import awscala.s3._
import org.apache.spark.SparkContext
import org.apache.spark.{SparkConf, SparkContext, Accumulator}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.SimpleDerivedStream
import telemetry.heka.{HekaFrame, Message}
import telemetry.utils.Telemetry
import org.joda.time.{format, DateTime, Days}
import com.typesafe.config._
import org.rogach.scallop._

object ExecutiveView {
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
    val sparkConf = new SparkConf().setAppName("ExecutiveView")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    val appConf = ConfigFactory.load()
    val parquetBucket = appConf.getString("app.parquetBucket")

    val knownChannels = Set("nightly", "aurora", "beta", "release")
    for (offset <- 0 to Days.daysBetween(from, to).getDays()) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")

      val schema = buildSchema()

      // get a list of channel values that fall under "Other"
      // for example, "esr" and "OTHER" should both be considered part of "Other" rather than
      // having their own categories, while "nightly" and other known channels should have
      // their own categories
      val otherChannels = Telemetry.listOptions(currentDate, List[String]("*", "*", "*", "*"))
                                   .map(prefix => prefix.split("/").last)
                                   .filter(channel => !knownChannels(channel))
                                   .toSet

      // get a mapping from channel names to sets containing possible channel values
      val channelValueSets = knownChannels.map(channel => (channel -> Set(channel))).toMap + ("Other" -> otherChannels)

      for ((channel, channelValues) <- channelValueSets) {
        // get a combined RDD of all messages in the desired channel
        val messages = channelValues.map(channelValue =>
          Telemetry.getRecords(sc, currentDate, List[String]("*", "*", "*", "*", channelValue))
        ).reduce(_.union(_))

        val records = sqlContext.createDataFrame(messages.flatMap(messageToRow), schema)
        records.write.mode(SaveMode.Overwrite).parquet(s"s3://$parquetBucket/executive_stream/v3/submission_date_s3=$currentDateString/channel_s3=$channel")
      }

      println("=======================================================================================")
      println(s"JOB COMPLETED SUCCESSFULLY FOR $currentDate")
      println("=======================================================================================")
    }
  }

  def messageToRow(message: Map[String, Any]): Option[Row] ={
    val root = Row(
      message.getOrElse("docType", None) match {
        case x: String => x
        case _ => return None
      },
      message.getOrElse("submissionDate", None) match {
        case x: String => x
        case _ => return None
      },
      message.getOrElse("activityTimestamp", None) match {
        case x: Double => x
        case _ => return None
      },
      message.getOrElse("profileCreationTimestamp", None) match {
        case x: Double => x
        case _ => 0
      },
      message.getOrElse("clientId", None) match {
        case x: String => x
        case _ => return None
      },
      message.getOrElse("documentId", None) match {
        case x: String => x
        case _ => return None
      },
      message.getOrElse("country", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("channel", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("os", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("osVersion", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("default", None) match {
        case x: Boolean => x
        case _ => return None
      },
      message.getOrElse("buildId", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("app", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("version", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("vendor", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("reason", None) match {
        case x: String => x
        case _ => ""
      },
      message.getOrElse("hours", None) match {
        case x: Double => x
        case _ => 0
      },
      message.getOrElse("google", None) match {
        case x: Long => x
        case _ => 0
      },
      message.getOrElse("yahoo", None) match {
        case x: Long => x
        case _ => 0
      },
      message.getOrElse("bing", None) match {
        case x: Long => x
        case _ => 0
      },
      message.getOrElse("other", None) match {
        case x: Long => x
        case _ => 0
      },
      message.getOrElse("pluginHangs", None) match {
        case x: Long => x
        case _ => 0
      }
    )

    Some(root)
  }

  def buildSchema(): StructType = {
    StructType(
      StructField("docType",                  StringType,  false) ::
      StructField("submissionDate",           StringType,  false) ::
      StructField("activityTimestamp",        DoubleType,  false) ::
      StructField("profileCreationTimestamp", DoubleType,  false) ::
      StructField("clientId",                 StringType,  false) ::
      StructField("documentId",               StringType,  false) ::
      StructField("country",                  StringType,  false) ::
      StructField("channel",                  StringType,  false) ::
      StructField("os",                       StringType,  false) ::
      StructField("osVersion",                StringType,  false) ::
      StructField("default",                  BooleanType, false) ::
      StructField("buildId",                  StringType,  false) ::
      StructField("app",                      StringType,  false) ::
      StructField("version",                  StringType,  false) ::
      StructField("vendor",                   StringType,  false) ::
      StructField("reason",                   StringType,  false) ::
      StructField("hours",                    DoubleType,  false) ::
      StructField("google",                   IntegerType, false) ::
      StructField("yahoo",                    IntegerType, false) ::
      StructField("bing",                     IntegerType, false) ::
      StructField("other",                    IntegerType, false) ::
      StructField("pluginHangs",              IntegerType, false) ::
      Nil
    )
  }
}
