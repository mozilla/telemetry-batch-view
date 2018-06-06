/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import java.util.Date

import com.mozilla.telemetry.utils.{deletePrefix, getOrCreateSparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, Days, format}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop._
import scalaj.http.Http

import scala.util.{Success, Try}

object ExperimentSummaryView {
  def schemaVersion: String = "v1"
  def jobName: String = "experiments"
  def experimentsUrl: String = "https://normandy.services.mozilla.com/api/v1/recipe/"
  def experimentsUrlParams: List[(String, String)] = List(("format", "json"))
  def experimentsQualifyingAction: List[String] = List("preference-experiment", "opt-out-study")
  def excludedExperiments: List[String] = List("pref-flip-screenshots-release-1369150", "pref-flip-search-composition-57-release-1413565")
  private case class NormandyRecipeBranch(ratio: Int, slug: String, value: Any)
  private case class NormandyRecipeArguments(branches: List[NormandyRecipeBranch],
                                             slug: Option[String],
                                             name: Option[String])
  private case class NormandyRecipe(id: Long,
                                    last_updated: Date,
                                    enabled: Boolean,
                                    action: String,
                                    arguments: NormandyRecipeArguments)
  case class NormandyException(private val message: String = "",
                               private val cause: Throwable = None.orNull) extends Exception(message, cause)

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

    val spark = getOrCreateSparkSession(jobName)

    val parquetSize = (512 * 1024 * 1024).toLong
    spark.conf.set("parquet.block.size", parquetSize)
    spark.conf.set("dfs.blocksize", parquetSize)

    implicit val sc = spark.sparkContext

    val outputBucket = conf.outputBucket()
    val inputBucket = conf.inputBucket.get.getOrElse(outputBucket)

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")

      logger.info("=======================================================================================")
      logger.info(s"BEGINNING JOB $jobName $schemaVersion FOR $currentDateString")

      val input = s"s3://$inputBucket/main_summary/${MainSummaryView.schemaVersion}/submission_date_s3=$currentDateString"
      val s3prefix = s"$jobName/$schemaVersion"
      val output = s"s3://$outputBucket/$s3prefix/"

      val experiments = getExperimentList(getExperimentRecipes(), currentDate.toDate)

      experiments.foreach(e => deletePreviousOutput(outputBucket, s3prefix, currentDateString, e))
      writeExperiments(input, output, currentDateString, experiments, spark)

      logger.info(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      logger.info("=======================================================================================")
    }
    sc.stop()
  }

  def deletePreviousOutput(bucket: String, prefix: String, date: String, experiment: String): Unit = {
    val completePrefix = s"$prefix/experiment_id=$experiment/submission_date_s3=$date"
    deletePrefix(bucket, completePrefix)
  }

  def writeExperiments(input: String, output: String, date: String, experiments: List[String], spark: SparkSession): Unit = {
    val mainSummary = spark.read.parquet(input)

    mainSummary
      .where("experiments IS NOT NULL AND size(experiments) > 0")
      .dropDuplicates("document_id")
      .select(col("*"), explode(col("experiments")).as(Array("experiment_id", "experiment_branch")))
      .where(col("experiment_id").isin(experiments:_*))
      .withColumn("submission_date_s3", lit(date))
      .repartition(col("experiment_id"))
      .write
      // Usage characteristics will most likely be "get all pings from an experiment for all days"
      .partitionBy("experiment_id", "submission_date_s3")
      .mode("append")
      .parquet(output)
  }

  def getExperimentRecipes(): JValue = {
    parse(Http(experimentsUrl).params(experimentsUrlParams).asString.body)
  }

  def getExperimentList(json: JValue, date: Date): List[String] = json match {
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


  def shouldProcessExperiment(r: NormandyRecipe, date: Date): Boolean = {
    experimentsQualifyingAction.contains(r.action) &&
    !excludedExperiments.contains(r.arguments.slug.get) &&
    ((r.enabled == true) || r.last_updated.after(date)) // is this experiment enabled for this date?
  }

  private def getNormalizedRecipe(json: JValue): Option[NormandyRecipe] = {
    // Some normandy recipes stuff the slug in a "slug" field and some in a "name" field
    // Here we try to normalize this a bit
    implicit val formats = new DefaultFormats {
      override def dateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    }

    Try(json.extract[NormandyRecipe]) match {
      case Success(r) =>
        Try(r.arguments.slug.getOrElse(r.arguments.name.get)) match {
          case Success(slug) =>
            Some(r.copy(arguments=NormandyRecipeArguments(r.arguments.branches, Some(slug), Some(slug))))
          case _ =>
            None
        }
      case _ =>
        None
    }
  }
}

