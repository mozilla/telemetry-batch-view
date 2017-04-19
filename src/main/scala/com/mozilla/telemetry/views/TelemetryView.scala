package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils._
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, format}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Days
import org.apache.spark.sql.{Row, SQLContext}
import org.rogach.scallop._

abstract class TelemetryView {
  def schemaVersion: String
  def jobName: String
  def docType: String
  def appName: String = "Firefox"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    val channel = opt[String]("channel", descr = "Only process data from the given channel", required = false)
    val appVersion = opt[String]("version", descr = "Only process data from the given app version", required = false)
    verify()
  }

  def messageToRows(message: Message): Option[List[Row]]
  def buildSchema: StructType

  def main(args: Array[String]): Unit = {
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

    // Set up Spark
    val sparkConf = new SparkConf().setAppName(jobName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    implicit val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("parquet.enable.summary-metadata", "false")


    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")
      val filterChannel = conf.channel.get
      val filterVersion = conf.appVersion.get

      println("=======================================================================================")
      println(s"BEGINNING JOB $jobName FOR $currentDateString")
      if (filterChannel.nonEmpty)
        println(s" Filtering for channel = '${filterChannel.get}'")
      if (filterVersion.nonEmpty)
        println(s" Filtering for version = '${filterVersion.get}'")

      val schema = buildSchema
      val ignoredCount = sc.accumulator(0, "Number of Records Ignored")
      val processedCount = sc.accumulator(0, "Number of Records Processed")
      val messages = Dataset("telemetry")
        .where("sourceName") {
          case "telemetry" => true
        }.where("sourceVersion") {
        case "4" => true
      }.where("docType") {
        case docType => true
      }.where("appName") {
        case appName => true
      }.where("submissionDate") {
        case date if date == currentDate.toString("yyyyMMdd") => true
      }.where("appUpdateChannel") {
        case channel => filterChannel.isEmpty || channel == filterChannel.get
      }.where("appVersion") {
        case v => filterVersion.isEmpty || v == filterVersion.get
      }.records(conf.limit.get)

      val rowRDD = messages.flatMap(m => {
        messageToRows(m) match {
          case Some(x) =>
            processedCount += 1
            x
          case _ =>
            ignoredCount += 1
            None
        }
      })

      val records = sqlContext.createDataFrame(rowRDD, schema)
      val s3prefix = s"$jobName/$schemaVersion/submission_date_s3=$currentDateString"
      val s3path = s"s3://${conf.outputBucket()}/$s3prefix"
      org.apache.commons.io.FileUtils.deleteQuietly(new java.io.File("test-output"))
      records.write.mode("error").parquet(s3path)
      S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")

      println(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      println("     RECORDS SEEN:    %d".format(ignoredCount.value + processedCount.value))
      println("     RECORDS IGNORED: %d".format(ignoredCount.value))
      println("=======================================================================================")

    }
    sc.stop()
  }
}

