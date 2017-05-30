package com.mozilla.telemetry.views

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days, format}
import org.rogach.scallop._

object ExperimentSummaryView {
  def schemaVersion: String = "v1"
  def jobName: String = "experiments"

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for main_summary data", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)
    // parse command line arguments
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val to = conf.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }
    val from = conf.from.get match {
      case Some(f) => fmt.parseDateTime(f)
      case _ => DateTime.now.minusDays(1)
    }

    val spark = getSparkSession()

    implicit val sc = spark.sparkContext

    val outputBucket = conf.outputBucket()
    val inputBucket = conf.inputBucket.get.getOrElse(outputBucket)

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")

      logger.info("=======================================================================================")
      logger.info(s"BEGINNING JOB $jobName $schemaVersion FOR $currentDateString")

      val input = s"s3://$inputBucket/main_summary/${MainSummaryView.schemaVersion}/submission_date_s3=$currentDateString"
      val s3prefix = s"$jobName/$schemaVersion/"
      val output = s"s3://$outputBucket/$s3prefix"

      writeExperiments(input, output, currentDateString, spark)

      logger.info(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      logger.info("=======================================================================================")
    }
    sc.stop()
  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(jobName)
      .master("local[*]")
      .getOrCreate()

    val parquetSize = (512 * 1024 * 1024).toLong
    spark.conf.set("parquet.block.size", parquetSize)
    spark.conf.set("dfs.blocksize", parquetSize)
    return spark
  }

  def writeExperiments(input: String, output: String, date: String, spark: SparkSession): Unit = {
    val mainSummary = spark.read.parquet(input)
    mainSummary
      .select(col("*"), explode(col("experiments")).as(Array("experiment_id", "experiment_branch")))
      .withColumn("submission_date_s3", lit(date))
      .repartition(col("experiment_id"))
      .write
      // Usage characteristics will most likely be "get all pings from an experiment for all days"
      .partitionBy("experiment_id", "submission_date_s3")
      .mode("error")
      .parquet(output)
  }
}

