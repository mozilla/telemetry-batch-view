package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers.{HistogramAnalyzer, ScalarAnalyzer}
import com.mozilla.telemetry.metrics._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.functions.col

object ExperimentAnalysisView {
  def schemaVersion: String = "v1"
  def jobName: String = "experiment_analysis"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    // TODO: change to s3 bucket/keys
    val inputLocation = opt[String]("input", descr = "Source for parquet data", required = true)
    val outputLocation = opt[String]("output", descr = "Destination for parquet data", required = true)
    val metric = opt[String]("metric", descr = "Run job on just this metric", required = false)
    val experiment = opt[String]("experiment", descr = "Run job on just this experiment", required = false)
    val date = opt[String]("date", descr = "Run date for this job (defaults to yesterday)", required = false)
    verify()
  }

  def main(args: Array[String]) {
    // Spark/job setup stuff
    val conf = new Conf(args)
    val date = conf.date.get match {
      case Some(d) => d
      case _ => com.mozilla.telemetry.utils.yesterdayAsYYYYMMDD
    }

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(jobName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    val data = spark.read.parquet(conf.inputLocation())

    val experiments = conf.experiment.get match {
      case Some(e) => List(e)
      // if there isn't a specific experiment passed in, run the job for all experiments that had pings on the date
      // we're running on
      case _ => data
        .where(col("submission_date_s3") === date)
        .select("experiment_id")
        .distinct()
        .collect()
        .toList
        .map(r => r(0).asInstanceOf[String])
    }

    val metricList: List[(String, MetricDefinition)] = conf.metric.get match {
      case Some(m) => List((m, (Histograms.definitions() ++ Scalars.definitions())(m.toUpperCase)))
      case _ => MainSummaryView.filterHistogramDefinitions(Histograms.definitions(), useWhitelist = true) ++
        Scalars.definitions(includeOptin = true).toList
    }

    experiments.foreach { e: String =>
      val experimentData = data.where(col("experiment_id") === e)
      val experimentResult = metricList.map {
        case (name: String, hd: HistogramDefinition) =>
          val columnName = MainSummaryView.getHistogramName(name.toLowerCase, "parent")
          new HistogramAnalyzer(columnName, hd, experimentData).analyze()
        case (name: String, sd: ScalarDefinition) =>
          val columnName = Scalars.getParquetFriendlyScalarName(name, "parent")
          ScalarAnalyzer.getAnalyzer(columnName, sd, experimentData).analyze()
        case _ => throw new UnsupportedOperationException("Unsupported metric definition type")
      }.reduce(_.union(_))

      val outputLocation = s"${conf.outputLocation()}/experiment_id=$e/date=$date"
      experimentResult.toDF
        .drop(col("experiment_id"))
        .repartition(1)
        .write.mode("overwrite").parquet(outputLocation)
    }

    spark.stop()
  }
}
