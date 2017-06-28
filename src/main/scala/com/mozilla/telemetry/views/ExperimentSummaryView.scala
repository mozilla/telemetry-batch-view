package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils.S3Store
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days, format}
import org.rogach.scallop._
import org.json4s.jackson.JsonMethods._
import org.json4s._

import scalaj.http.Http
import scala.util.{Try, Success}

object ExperimentSummaryView {
  def schemaVersion: String = "v1"
  def jobName: String = "experiments"
  def experimentsUrl: String = "https://normandy.services.mozilla.com/api/v1/recipe/"
  def experimentsUrlParams = List(("latest_revision__action", "3"), ("format", "json"))
  def experimentsQualifyingAction = "preference-experiment"
  private case class NormandyRecipeBranch(ratio: Int, slug: String, value: Any)
  private case class NormandyRecipeArguments(branches: List[NormandyRecipeBranch], slug: String)
  private case class NormandyRecipe(id: Long, enabled: Boolean, action: String, arguments: NormandyRecipeArguments)
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
      val s3prefix = s"$jobName/$schemaVersion"
      val output = s"s3://$outputBucket/$s3prefix/"

      val experiments = getExperimentList(getExperimentRecipes())

      experiments.foreach(e => deletePreviousOutput(outputBucket, s3prefix, currentDateString, e))
      writeExperiments(input, output, currentDateString, experiments, spark)

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

  def deletePreviousOutput(bucket: String, prefix: String, date: String, experiment: String): Unit = {
    val completePrefix = s"$prefix/experiment_id=$experiment/submission_date_s3=$date"
    if (!S3Store.isPrefixEmpty(bucket, completePrefix)) {
      // not the most efficient way to do this, but this is all on AWS anyway..
      S3Store.listKeys(bucket, completePrefix).foreach(o => S3Store.deleteKey(bucket, o.key))
    }
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

  def getExperimentList(json: JValue): List[String] = {
    implicit val formats = DefaultFormats
    json match {
      case JArray(x) => x.flatMap(v =>
        Try(v.extract[NormandyRecipe]) match {
          case Success(r) => if (r.action == experimentsQualifyingAction) List(r.arguments.slug) else List()
          case _ => List()
        }
      )
      case _ => throw NormandyException("Unexpected response format from experiment recipe server")
    }
  }
}

