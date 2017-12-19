package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers._
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.functions.{col, min}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf

import scala.util.{Failure, Success, Try}

object ExperimentAnalysisView {
  def defaultErrorAggregatesBucket = "net-mozaws-prod-us-west-2-pipeline-data"
  def errorAggregatesPath = "error_aggregates/v2"
  def jobName = "experiment_analysis"
  def schemaVersion = "v1"
  // This gives us ~120MB partitions with the columns we have now. We should tune this as we add more columns.
  def rowsPerPartition = 25000
  val bootstrapLimit = 25000000 // Empirically tested to take ~1 hour with this many pings

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  private val columnsWhitelist = List(
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
    "histogram_content_time_to_response_start_ms"
  )

  implicit class ExperimentDataFrame(df: DataFrame) {
    def selectUsedColumns(extra: List[String] = List()): DataFrame = {
      val whitelist = columnsWhitelist ++ extra
      val columns = df.schema.map(_.name).filter(whitelist.contains(_))
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
    val bootstrapScalars = toggle(
      "bootstrapScalars",
      descrYes = "Run bootstrapped confidence intervals on scalars",
      default = None)
    val bootstrapHistograms = toggle(
      "bootstrapHistograms",
      descrYes = "Run bootstrapped confidence intervals on histograms (very slow)",
      default = Some(false))
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
        case Success(df) => df.where(col("experiment_id") === e && col("submission_date") >= minDate)
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
      spark.stop()
    }
  }

  def getDate(conf: Conf): String =  {
   conf.date.get match {
      case Some(d) => d
      case _ => com.mozilla.telemetry.utils.yesterdayAsYYYYMMDD
    }   
  }

  def getExperiments(conf: Conf, data: DataFrame): List[String] = {
    conf.experiment.get match {
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

  def getMetrics(conf: Conf, data: DataFrame) = {
    conf.metric.get match {
      case Some(m) => {
        List((m, (Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _) ++ Scalars.definitions())(m.toLowerCase)))
      }
      case _ => {
        val histogramDefs = MainSummaryView.filterHistogramDefinitions(
          Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _),
          useWhitelist = true)
        val scalarDefs = Scalars.definitions(includeOptin = true).toList
        scalarDefs ++ histogramDefs
      }
    }
  }

  def getExperimentMetrics(experiment: String,
                           experimentsSummary: DataFrame,
                           errorAggregates: DataFrame, conf: Conf,
                           experimentMetrics: List[String] = List()): List[MetricAnalysis] = {
    val pingCount = experimentsSummary.count()
    val persisted = repartitionAndPersist(experimentsSummary, pingCount, experiment, experimentMetrics)
    val bootstrapHistograms = conf.bootstrapHistograms()
    val bootstrapScalars = shouldBootstrapScalars(pingCount, conf)

    val schemaFields = experimentsSummary.schema.map(_.name)

    val metricList = getMetrics(conf, experimentsSummary).filter {case (name, md) => schemaFields.contains(name)}

    val metrics = metricList.flatMap {
      case (name: String, md: MetricDefinition) =>
        md match {
          case hd: HistogramDefinition =>
            new HistogramAnalyzer(name, hd, persisted, bootstrapHistograms).analyze()
          case sd: ScalarDefinition =>
            ScalarAnalyzer.getAnalyzer(name, sd, persisted, bootstrapScalars).analyze()
          case _ => throw new UnsupportedOperationException("Unsupported metric definition type")
        }
    }

    val crashes = CrashAnalyzer.getExperimentCrashes(errorAggregates)

    val metadata = ExperimentAnalyzer.getExperimentMetadata(persisted).collect()
    (metadata ++ metrics ++ crashes).toList
  }

  // Repartitions dataset if warranted, and persists the result for quicker access
  def repartitionAndPersist(experimentsSummary: DataFrame,
                            pingCount: Long,
                            experiment: String,
                            experimentMetrics: List[String]): DataFrame = {
    val calculatedPartitions = (pingCount / rowsPerPartition).toInt
    val numPartitions = calculatedPartitions max experimentsSummary.sparkSession.sparkContext.defaultParallelism
    experimentsSummary
      .selectUsedColumns(extra = experimentMetrics)
      .repartition(numPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  def shouldBootstrapScalars(pingCount: Long, conf: Conf): Boolean = {
    conf.bootstrapScalars.get.getOrElse(pingCount < bootstrapLimit)
  }
}
