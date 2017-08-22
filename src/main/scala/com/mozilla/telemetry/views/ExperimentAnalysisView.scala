package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers.{ExperimentAnalyzer, HistogramAnalyzer, MetricAnalysis, ScalarAnalyzer}
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.functions.col

object ExperimentAnalysisView {
  def schemaVersion: String = "v1"
  def jobName: String = "experiments_aggregates"
  def numParquetFiles: Int = 1

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for parquet data", required = false)
    val bucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = false)
    val inputLocation = opt[String]("input-location", descr = "Exact location for parquet data", required = false)
    val outputLocation = opt[String]("output-location", descr = "Exact location for output data", required = false)
    val metric = opt[String]("metric", descr = "Run job on just this metric", required = false)
    val experiment = opt[String]("experiment", descr = "Run job on just this experiment", required = false)
    val date = opt[String]("date", descr = "Run date for this job (defaults to yesterday)", required = false)

    requireOne(bucket, inputLocation)
    requireOne(bucket, outputLocation)
    conflicts(bucket, List(inputLocation, outputLocation))
    verify()
  }

  def getSpark: SparkSession = {
    val spark = getOrCreateSparkSession(jobName)
    spark.sparkContext.setLogLevel("INFO")
    spark
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)
    val sparkSession = getSpark

    val inputLocation = getInputLocation(conf)
    val baseOutputLocation = getOutputLocation(conf)

    val experimentData = sparkSession.read.parquet(inputLocation)
    val date = getDate(conf)
    logger.info("=======================================================================================")
    logger.info(s"Starting $jobName for date $date")

    val experiments = getExperiments(conf, experimentData)
    sparkSession.stop()

    logger.info(s"List of experiments to process for $date is: $experiments")

    experiments.foreach{ e: String =>
      logger.info(s"Aggregating pings for experiment $e")

      val spark = getSpark
      val data = spark.read.parquet(conf.inputLocation())
      val outputLocation = s"$baseOutputLocation/experiment_id=$e/date=$date"
      getExperimentMetrics(e, data, conf)
        .toDF()
        .drop(col("experiment_id"))
        .repartition(numParquetFiles)
        .write.mode("overwrite").parquet(outputLocation)

      logger.info(s"Wrote aggregates to $outputLocation")
      spark.stop()
    }
  }

  def getInputLocation(conf: Conf): String = {
    conf.bucket.get match {
      case Some(b) => s"s3://${conf.inputBucket.get.getOrElse(b)}/${ExperimentSummaryView.jobName}/${ExperimentSummaryView.schemaVersion}"
      case _ => conf.inputLocation()
    }
  }

  def getOutputLocation(conf: Conf): String = {
    conf.bucket.get match {
      case Some(b) => s"s3://$b/$jobName/$schemaVersion"
      case _ => conf.outputLocation()
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
        List((m, (Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _) ++ Scalars.definitions())(m.toUpperCase)))
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

  def getExperimentMetrics(experiment: String, data: DataFrame, conf: Conf): Dataset[MetricAnalysis] = {
    val metricList = getMetrics(conf, data)
    val experimentData = data.where(col("experiment_id") === experiment)

    val metrics = metricList.map {
      case (name: String, md: MetricDefinition) =>
        md match {
          case hd: HistogramDefinition =>
            new HistogramAnalyzer(name, hd, experimentData).analyze()
          case sd: ScalarDefinition =>
            ScalarAnalyzer.getAnalyzer(name, sd, experimentData).analyze()
          case _ => throw new UnsupportedOperationException("Unsupported metric definition type")
        }
    }
    val metadata = ExperimentAnalyzer.getExperimentMetadata(experimentData)
    (metadata :: metrics).reduce(_.union(_))
  }
}
