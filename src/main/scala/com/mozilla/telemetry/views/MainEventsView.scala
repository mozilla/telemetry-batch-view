/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils.{S3Store, getOrCreateSparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.rogach.scallop._

object MainEventsView extends BatchJobBase {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

  def schemaVersion: String = "v1"

  def jobName: String = "events"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends BaseOpts(args) {
    val sampleId = opt[String]("sampleid", descr = "Sample ID to limit processing to", required = false)
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for main_summary data", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments

    // Set up Spark
    val spark = getOrCreateSparkSession("EventsView")
    implicit val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration

    // We want to end up with reasonably large parquet files on S3.
    val parquetSize = 512 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    // Don't write metadata files, because they screw up partition discovery.
    // This is fixed in Spark 2.0, see:
    //   https://issues.apache.org/jira/browse/SPARK-13207
    //   https://issues.apache.org/jira/browse/SPARK-15454
    //   https://issues.apache.org/jira/browse/SPARK-15895
    hadoopConf.set("parquet.enable.summary-metadata", "false")


    val outputBucket = conf.outputBucket()
    val inputBucket = conf.inputBucket.get.getOrElse(outputBucket)

    for (currentDateString <- datesBetween(conf.from(), conf.to.toOption)) {
      logger.info("=======================================================================================")
      logger.info(s"BEGINNING JOB $jobName $schemaVersion FOR $currentDateString")

      val mainSummary = spark.read.parquet(s"s3://$inputBucket/main_summary/v4/submission_date_s3=$currentDateString")
      val events = eventsFromMain(mainSummary, conf.sampleId.get)

      val s3prefix = s"$jobName/$schemaVersion/submission_date_s3=$currentDateString/doc_type=main"
      val s3path = s"s3://$outputBucket/$s3prefix"

      // Repartition the dataframe down before saving.
      val partitioned = events.repartition(1)

      // Then write to S3 using the given fields as path name partitions. If any
      // data already exists for the target day, replace it.
      partitioned.write.mode("overwrite").parquet(s3path)

      // Then remove the _SUCCESS file so we don't break Spark partition discovery.
      S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")

      logger.info(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      logger.info("=======================================================================================")
    }
    if (shouldStopContextAtEnd(spark)) { spark.stop() }
  }

  def eventsFromMain(mainSummaryData: DataFrame, sampleId: Option[String]): DataFrame = {
    val partialDataFrame = mainSummaryData
      .where("client_id is not null")
      .where("events is not null")

    val records = sampleId match {
      case Some(s) => partialDataFrame.where(s"sample_id = $s")
      case _ => partialDataFrame
    }

    // Explode the events entries
    val exploded = records
      .withColumn("events", explode(when(size(col("events")).gt(0), col("events"))))
      .withColumn("session_start_time", unix_timestamp(col("session_start_date"), "yyy-MM-dd'T'HH:mm:ss.0XXX") * 1000)

    exploded.selectExpr("document_id", "client_id", "normalized_channel", "country", "locale", "app_name",
      "app_version", "os", "os_version", "session_id", "subsession_id", "session_start_time", "timestamp", "sample_id",
      "experiments",
      // Flatten nested event fields.
      "events.timestamp as event_timestamp",
      "events.category as event_category",
      "events.method as event_method",
      "events.object as event_object",
      "events.string_value as event_string_value",
      "events.map_values as event_map_values",
      "events.map_values['telemetry_process'] as event_process")
  }
}
