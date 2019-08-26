/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers._
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils.{getOrCreateSparkSession, blockIdFromString}
import org.apache.spark.sql.functions.{col, min, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf

import scala.util.{Failure, Success, Try}

object ExperimentAnalysisView extends BatchJobBase {
  private val defaultErrorAggregatesBucket = "net-mozaws-prod-us-west-2-pipeline-data"
  private val errorAggregatesPath = "experiment_error_aggregates/v1"
  private val jobName = "experiment_analysis"
  // This gives us ~120MB partitions with the columns we have now. We should tune this as we add more columns.
  private val rowsPerPartition = 25000
  private val defaultJackknifeBlocks = 100
  private val defaultBootstrapIterations = 1000

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  private val allowedColumns = List(
    "client_id",
    "experiment_id",
    "experiment_branch",
    // Add default histograms & scalars here
    "scalar_parent_browser_engagement_active_ticks",
    "scalar_parent_browser_engagement_max_concurrent_tab_count",
    "scalar_parent_browser_engagement_max_concurrent_window_count",
    "scalar_parent_browser_engagement_navigation_about_home",
    "scalar_parent_browser_engagement_navigation_about_newtab",
    "scalar_parent_browser_engagement_navigation_contextmenu",
    "scalar_parent_browser_engagement_navigation_searchbar",
    "scalar_parent_browser_engagement_navigation_urlbar",
    "scalar_parent_browser_engagement_restored_pinned_tabs_count",
    "scalar_parent_browser_engagement_tab_open_event_count",
    "scalar_parent_browser_engagement_total_uri_count",
    "scalar_parent_browser_engagement_unfiltered_uri_count",
    "scalar_parent_browser_engagement_unique_domains_count",
    "scalar_parent_browser_engagement_window_open_event_count",
    "histogram_content_time_to_first_click_ms",
    "histogram_content_time_to_first_interaction_ms",
    "histogram_content_time_to_first_key_input_ms",
    "histogram_content_time_to_first_mouse_move_ms",
    "histogram_content_time_to_first_scroll_ms",
    "histogram_content_time_to_non_blank_paint_ms",
    "histogram_content_time_to_response_start_ms",
    "histogram_parent_fx_tab_switch_total_e10s_ms"
  )

  implicit class ExperimentDataFrame(df: DataFrame) {
    def selectUsedColumns(extra: List[String] = List()): DataFrame = {
      val allowlist = allowedColumns ++ extra
      val columns = df.schema.map(_.name).filter(allowlist.contains(_))
      df.select(columns.head, columns.tail: _*)
    }
  }

  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    // TODO: change to s3 bucket/keys
    val inputLocation = opt[String]("input", descr = "Source for parquet data", required = true)
    val outputLocation = opt[String]("output", descr = "Destination for parquet data", required = true)
    val errorAggregatesBucket = opt[String](descr = "Bucket for error_aggregates data", required = false,
      default = Some(defaultErrorAggregatesBucket))
    val metric = opt[String]("metric", descr = "Run job on just this metric", required = false)
    val experiment = opt[String]("experiment", descr = "Run job on just this experiment", required = false)
    val date = opt[String]("date", descr = "Run date for this job (defaults to yesterday)", required = false)
    val jackknifeBlocks = opt[Int]("jackknifeBlocks",
      descr = "Number of jackknife blocks to use; set to 0 to disable base metrics", required = false,
      default = Some(defaultJackknifeBlocks))
    val bootstrapIterations = opt[Int]("bootstrapIterations",
      descr = "Number of bootstrap iterations to use for engagement metrics; set to 0 to disable engagement metrics",
      required = false,
      default = Some(defaultBootstrapIterations))
    verify()
  }

  def getSpark: SparkSession = {
    val spark = getOrCreateSparkSession(jobName)
    if (spark.sparkContext.defaultParallelism > 200) {
      spark.sqlContext.sql(s"set spark.sql.shuffle.partitions=${spark.sparkContext.defaultParallelism}")
    }
    spark.sparkContext.setLogLevel("INFO")
    spark
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)
    val sparkSession = getSpark

    val experimentData = sparkSession.read.option("mergeSchema", "true").parquet(conf.inputLocation())
    val date = getDate(conf)
    logger.info("=======================================================================================")
    logger.info(s"Starting $jobName for date $date")

    val experiments = getExperiments(conf, experimentData)
    sparkSession.stop()

    logger.info(s"List of experiments to process for $date is: $experiments")

    experiments.foreach{ e: String =>
      logger.info(s"Aggregating pings for experiment $e")

      val spark = getSpark

      val experimentsSummary = spark.read.option("mergeSchema", "true").parquet(conf.inputLocation())
        .where(col("experiment_id") === e)
        .where(col("submission_date_s3") <= date)

      val minDate = experimentsSummary
        .agg(min("submission_date_s3"))
        .first.getAs[String](0)

      val errorAggregates = Try(spark.read.parquet(s"s3://${conf.errorAggregatesBucket()}/$errorAggregatesPath")) match {
        case Success(df) => df.where(col("experiment_id") === e && col("submission_date_s3") >= minDate)
        case Failure(_) => spark.emptyDataFrame
      }

      val outputLocation = s"${conf.outputLocation()}/experiment_id=$e/date=$date"

      import spark.implicits._
      getExperimentMetrics(e, experimentsSummary, errorAggregates, conf)
        .toDF()
        .drop(col("experiment_id"))
        .repartition(1)
        .write.mode("overwrite").parquet(outputLocation)

      logger.info(s"Wrote aggregates to $outputLocation")

      if (shouldStopContextAtEnd(spark)) { spark.stop() }
    }
  }

  def getDate(conf: Conf): String =  {
   conf.date.toOption match {
      case Some(d) => d
      case _ => com.mozilla.telemetry.utils.yesterdayAsYYYYMMDD
    }
  }

  def getExperiments(conf: Conf, data: DataFrame): List[String] = {
    conf.experiment.toOption match {
      case Some(e) => List(e)
      case _ => data // get all experiments
        .where(col("submission_date_s3") === getDate(conf))
        .select("experiment_id")
        .distinct()
        .collect()
        .toList
        .map(r => r(0).asInstanceOf[String])
    }
  }

  def getMetrics(conf: Conf): List[(String, MetricDefinition)] = {
    conf.metric.toOption match {
      case Some(m) => {
        List((m, (Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _) ++ Scalars.definitions())(m.toLowerCase)))
      }
      case _ => {
        val histogramDefs = MainSummaryView.filterHistogramDefinitions(
          Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _),
          useAllowlist = true)
        val scalarDefs = Scalars.definitions(includeOptin = true).toList
        scalarDefs ++ histogramDefs
      }
    }
  }

  def getExperimentMetrics(experiment: String,
                           experimentsSummary: DataFrame,
                           errorAggregates: DataFrame, conf: Conf,
                           experimentMetrics: List[String] = List()): List[MetricAnalysis] = {
    val experimentsFiltered = experimentsSummary
      .where("experiment_branch is not NULL") // Bug 1463248
      .where("client_id is not NULL")
    val pingCount = experimentsFiltered.count()
    val numJackknifeBlocks = conf.jackknifeBlocks()
    val bootstrapIterations = conf.bootstrapIterations()

    val metrics: Seq[MetricAnalysis] = if (numJackknifeBlocks <= 0) {
      Seq.empty[MetricAnalysis]
    } else {

      val persisted = repartitionAndPersist(experimentsFiltered, pingCount, experiment, experimentMetrics, numJackknifeBlocks)

      val schemaFields = persisted.schema.map(_.name)

      val metricList = getMetrics(conf).filter {case (name, md) => schemaFields.contains(name)}

      val baseMetrics = metricList.flatMap {
        case (name: String, md: MetricDefinition) =>
          md match {
            case hd: HistogramDefinition =>
              new HistogramAnalyzer(name, hd, persisted, numJackknifeBlocks).analyze()
            case sd: ScalarDefinition =>
              ScalarAnalyzer.getAnalyzer(name, sd, persisted, numJackknifeBlocks).analyze()
            case _ => throw new UnsupportedOperationException("Unsupported metric definition type")
          }
      }

      val metadata = ExperimentAnalyzer.getExperimentMetadata(persisted).collect()

      persisted.unpersist()

      metadata ++ baseMetrics
    }

    val crashes = CrashAnalyzer.getExperimentCrashes(errorAggregates)

    val engagement = if (bootstrapIterations <= 0) {
      Seq.empty[MetricAnalysis]
    } else {
      ExperimentEngagementAnalyzer.getMetrics(experimentsFiltered, bootstrapIterations)
    }

    (metrics ++ engagement ++ crashes).toList
  }

  // Repartitions dataset if warranted, adds a jackknife block id, and persists the result for quicker access
  def repartitionAndPersist(experimentsSummary: DataFrame,
                            pingCount: Long,
                            experiment: String,
                            experimentMetrics: List[String],
                            numJackknifeBlocks: Int): DataFrame = {
    val calculatedPartitions = (pingCount / rowsPerPartition).toInt
    val numPartitions = calculatedPartitions max experimentsSummary.sparkSession.sparkContext.defaultParallelism
    val generateBlockId = udf(blockIdFromString(numJackknifeBlocks.toLong) _)
    experimentsSummary
      .selectUsedColumns(extra = experimentMetrics)
      .select(col("*"), generateBlockId(col("client_id")).alias("block_id"))
      .repartition(numPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }
}
