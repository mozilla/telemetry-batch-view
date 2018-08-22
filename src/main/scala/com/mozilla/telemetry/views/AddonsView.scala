/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils.{S3Store, getOrCreateSparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.rogach.scallop._

object AddonsView extends BatchJobBase {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

  def schemaVersion: String = "v2"
  def jobName: String = "addons"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends BaseOpts(args) {
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for main_summary data", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments

    // Set up Spark
    val spark = getOrCreateSparkSession("AddonsView")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

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
    val inputBucket = conf.inputBucket.toOption.getOrElse(outputBucket)

    for (currentDateString <- datesBetween(conf.from(), conf.to.get)) {
      logger.info("=======================================================================================")
      logger.info(s"BEGINNING JOB $jobName $schemaVersion FOR $currentDateString")

      val mainSummary = spark.read.parquet(s"s3://$inputBucket/main_summary/${MainSummaryView.schemaVersion}/submission_date_s3=$currentDateString")
      val addons = addonsFromMain(mainSummary)

      val s3prefix = s"$jobName/$schemaVersion/submission_date_s3=$currentDateString"
      val s3path = s"s3://$outputBucket/$s3prefix"

      // Repartition the dataframe by sample_id before saving.
      val partitioned = addons.repartition(100, addons.col("sample_id"))

      // Then write to S3 using the given fields as path name partitions. If any
      // data already exists for the target day, replace it.
      partitioned.write.partitionBy("sample_id").mode("overwrite").parquet(s3path)

      // Then remove the _SUCCESS file so we don't break Spark partition discovery.
      S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")

      logger.info(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      logger.info("=======================================================================================")
    }

    if (shouldStopContextAtEnd(spark)) { spark.stop() }
  }

  def addonsFromMain(mainSummaryData: DataFrame): DataFrame = {
    val addonSchema = MainSummaryView.buildAddonSchema
    val records = mainSummaryData
      .select("document_id", "client_id", "sample_id", "subsession_start_date", "active_addons", "normalized_channel")
      .where("client_id is not null")

    // Explode the addon entries, including a null row for the record if there
    // were none (either null or an empty list). Reference:
    // http://stackoverflow.com/questions/39739072/spark-sql-how-to-explode-without-losing-null-values
    val exploded = records.withColumn("active_addons",
      explode(when(size(col("active_addons")).gt(0), col("active_addons"))
        .otherwise(array(lit(null).cast(addonSchema)))))

    exploded.selectExpr("document_id", "client_id", "sample_id", "subsession_start_date", "normalized_channel",
      // Flatten nested addon fields.
      "active_addons.addon_id as addon_id",
      "active_addons.blocklisted as blocklisted",
      "active_addons.name as name",
      "active_addons.user_disabled as user_disabled",
      "active_addons.app_disabled as app_disabled",
      "active_addons.version as version",
      "active_addons.scope as scope",
      "active_addons.type as type",
      "active_addons.foreign_install as foreign_install",
      "active_addons.has_binary_components as has_binary_components",
      "active_addons.install_day as install_day",
      "active_addons.update_day as update_day",
      "active_addons.signed_state as signed_state",
      "active_addons.is_system as is_system",
      "active_addons.is_web_extension as is_web_extension",
      "active_addons.multiprocess_compatible as multiprocess_compatible")
  }
}
