package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers.{ExperimentAnalyzer, HistogramAnalyzer, MetricAnalysis, ScalarAnalyzer}
import com.mozilla.telemetry.metrics._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.functions.col

object ExperimentAnalysisView {
  def schemaVersion: String = "v1"
  def jobName: String = "experiment_analysis"

  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    // TODO: change to s3 bucket/keys
    val inputLocation = opt[String]("input", descr = "Source for parquet data", required = true)
    val outputLocation = opt[String]("output", descr = "Destination for parquet data", required = true)
    val metric = opt[String]("metric", descr = "Run job on just this metric", required = false)
    val experiment = opt[String]("experiment", descr = "Run job on just this experiment", required = false)
    val date = opt[String]("date", descr = "Run date for this job (defaults to yesterday)", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(jobName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    val data = spark.read.parquet(conf.inputLocation())
    val date = getDate(conf)

    getExperiments(conf, data).foreach{ e: String => 
      val outputLocation = s"${conf.outputLocation()}/experiment_id=$e/date=$date"
      getExperimentMetrics(e, data, conf)
        .toDF()
        .drop(col("experiment_id"))
        .repartition(1)
        .write.mode("overwrite").parquet(outputLocation)
    }

    spark.stop()
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
      case (name: String, hd: HistogramDefinition) =>
        new HistogramAnalyzer(name, hd, experimentData).analyze()
      case (name: String, sd: ScalarDefinition) =>
        ScalarAnalyzer.getAnalyzer(name, sd, experimentData).analyze()
      case _ => throw new UnsupportedOperationException("Unsupported metric definition type")
    }
    val metadata = ExperimentAnalyzer.getExperimentMetadata(data)
    (metadata :: metrics).reduce(_.union(_))
  }
}
