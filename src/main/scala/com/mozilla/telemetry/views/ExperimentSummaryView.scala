/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import java.time.{LocalDate, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.mozilla.telemetry.utils.{deletePrefix, getOrCreateSparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop._
import scalaj.http.Http

import scala.util.{Success, Try}

object ExperimentSummaryView extends BatchJobBase {
  def schemaVersion: String = "v1"
  def jobName: String = "experiments"
  def experimentsUrl: String = "https://normandy.services.mozilla.com/api/v1/recipe/"
  def experimentsUrlParams: List[(String, String)] = List(("format", "json"))
  def experimentsQualifyingAction: List[String] = List("preference-experiment", "opt-out-study")
  def excludedExperiments: List[String] = List(
    "pref-flip-screenshots-release-1369150",
    "pref-flip-search-composition-57-release-1413565",
    "pref-hotfix-tls-13-avast-rollback")
  private case class NormandyRecipeBranch(ratio: Int, slug: String, value: Any)
  private case class NormandyRecipeArguments(branches: List[NormandyRecipeBranch],
                                             isHighVolume: Option[Boolean],
                                             slug: Option[String],
                                             name: Option[String])
  private case class NormandyRecipe(id: Long,
                                    last_updated: OffsetDateTime,
                                    enabled: Boolean,
                                    action: String,
                                    arguments: NormandyRecipeArguments)
  case class NormandyException(private val message: String = "",
                               private val cause: Throwable = None.orNull) extends Exception(message, cause)

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends BaseOpts(args) {
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for main_summary data", required = false)
    val maxRecordsPerFile = opt[Int]("max-records-per-file",
      descr = "Max number of rows to write to output files before splitting",
      required = false,
      default=Some(500000))
    val experimentLimit = opt[Int](
      "experiment-limit",
      descr = "Number of experiments to limit results to",
      required = false)
    val sampleId = opt[String](
      "sample-id",
      descr = "Sample_id to restrict results to",
      required = false)
    val rowLimit = opt[Int](
      "row-limit",
      descr = "Number of rows to limit input to",
      required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val spark = getOrCreateSparkSession(jobName)

    val parquetSize = (512 * 1024 * 1024).toLong
    spark.conf.set("parquet.block.size", parquetSize)
    spark.conf.set("dfs.blocksize", parquetSize)

    val outputBucket = conf.outputBucket()
    val inputBucket = conf.inputBucket.getOrElse(outputBucket)
    val maxRecordsPerFile = conf.maxRecordsPerFile()
    val sampleId = conf.sampleId.toOption

    for (submissionDate <- datesBetween(conf.from(), conf.to.toOption)) {
      val currentDate = LocalDate
        .parse(submissionDate, BatchJobBase.DateFormatter)
        .atStartOfDay(ZoneOffset.UTC)
        .toOffsetDateTime

      logger.info("=======================================================================================")
      logger.info(s"BEGINNING JOB $jobName $schemaVersion FOR $submissionDate")

      val input = s"s3://$inputBucket/main_summary/v4/submission_date_s3=$submissionDate"
      val s3prefix = s"$jobName/$schemaVersion"
      val output = s"s3://$outputBucket/$s3prefix/"

      val experiments = getExperimentList(getExperimentRecipes(), currentDate, conf.experimentLimit.toOption)

      experiments.foreach(e => deletePreviousOutput(outputBucket, s3prefix, submissionDate, e))
      writeExperiments(input, output, submissionDate, experiments, spark, maxRecordsPerFile, sampleId)

      logger.info(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $submissionDate")
      logger.info("=======================================================================================")
    }

    if (shouldStopContextAtEnd(spark)) { spark.stop() }
  }

  def deletePreviousOutput(bucket: String, prefix: String, date: String, experiment: String): Unit = {
    val completePrefix = s"$prefix/experiment_id=$experiment/submission_date_s3=$date"
    deletePrefix(bucket, completePrefix)
  }

  def writeExperiments(input: String, output: String, date: String, experiments: List[String], spark: SparkSession,
                       maxRecordsPerFile: Int, sampleId: Option[String] = None, rowLimit: Option[Int] = None): Unit = {
    val mainSummary = spark.read.parquet(input)
    val sampleIdClause = sampleId.map(s => s"sample_id='$s'").getOrElse("TRUE")

    val mainSummaryFiltered = mainSummary
      .where("experiments IS NOT NULL AND size(experiments) > 0")
      .where(sampleIdClause)
      .dropDuplicates("document_id")

    val mainSummaryLimited = rowLimit.map(mainSummaryFiltered.limit(_)).getOrElse(mainSummaryFiltered)

    mainSummaryLimited
      .select(col("*"), explode(col("experiments")).as(Array("experiment_id", "experiment_branch")))
      .where(col("experiment_id").isin(experiments:_*))
      .withColumn("submission_date_s3", lit(date))
      .coalesce(10)
      .write
      // Usage characteristics will most likely be "get all pings from an experiment for all days"
      .partitionBy("experiment_id", "submission_date_s3")
      .mode("append")
      .option("maxRecordsPerFile", maxRecordsPerFile)
      .parquet(output)
  }

  def getExperimentRecipes(): JValue = parse(
    Http(experimentsUrl)
      .params(experimentsUrlParams)
      .timeout(connTimeoutMs = 5000, readTimeoutMs = 60000)
      .asString
      .body
  )

  def getExperimentList(json: JValue, date: OffsetDateTime, limit: Option[Int] = None): List[String] = {
    val experiments = json match {
      case JArray(x) => x.flatMap(v =>
        getNormalizedRecipe(v) match {
          case Some(r) if shouldProcessExperiment(r, date) =>
            List(r.arguments.slug.get)
          case _ =>
            List()
        }
      )
      case _ => throw NormandyException("Unexpected response format from experiment recipe server")
    }
    limit.map(experiments.take(_)).getOrElse(experiments)
  }


  def shouldProcessExperiment(r: NormandyRecipe, date: OffsetDateTime): Boolean = {
    experimentsQualifyingAction.contains(r.action) &&
    r.arguments.isHighVolume != Some(true) &&
    !excludedExperiments.contains(r.arguments.slug.get) &&
    (r.enabled || r.last_updated.isAfter(date)) // is this experiment enabled for this date?
  }

  // Custom json4s serializer for java time
  private object OffsetDateTimeSerializer extends CustomSerializer[OffsetDateTime](_ => (
    {
      case JString(s) => OffsetDateTime.parse(s)
    },
    {
      case date: OffsetDateTime => JString(date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
    })
  )

  private def getNormalizedRecipe(json: JValue): Option[NormandyRecipe] = {
    // Some normandy recipes stuff the slug in a "slug" field and some in a "name" field
    // Here we try to normalize this a bit
    implicit val formats = DefaultFormats ++ Seq(OffsetDateTimeSerializer)

    Try(json.extract[NormandyRecipe]) match {
      case Success(r) =>
        Try(r.arguments.slug.getOrElse(r.arguments.name.get)) match {
          case Success(slug) =>
            Some(r.copy(arguments=NormandyRecipeArguments(r.arguments.branches, r.arguments.isHighVolume, Some(slug),
              Some(slug))))
          case _ =>
            None
        }
      case _ =>
        None
    }
  }
}

