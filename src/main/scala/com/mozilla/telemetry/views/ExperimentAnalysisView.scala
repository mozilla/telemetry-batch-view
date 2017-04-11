package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers.MetricAnalyzer
import com.mozilla.telemetry.histograms.{HistogramDefinition, Histograms}
import com.mozilla.telemetry.scalars.{ScalarDefinition, Scalars}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.types._

object ExperimentAnalysisView {
  def schemaVersion: String = "v1"
  def jobName: String = "experiment_analysis"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    // TODO: change to s3 bucket/keys
    val inputLocation = opt[String]("input", descr = "Source for parquet data", required = true)
    val outputLocation = opt[String]("output", descr = "Destination for parquet data", required = true)
    val histo = opt[String]("histogram", descr = "Run job on just this histogram", required = false)
    val scalar = opt[String]("scalar", descr = "Run job on just this scalar", required = false)
    verify()
  }

  def main(args: Array[String]) {
    // Spark/job setup stuff
    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(jobName)
      .getOrCreate()

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // DEBUG -- remove before committing
    spark.sparkContext.setLogLevel("WARN")
    val parquetSize = 512 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    // Grab messages
    val rows = spark.read.parquet(conf.inputLocation())

    val histogramList = conf.histo.get match {
      case Some(h) => Map(h -> Histograms.definitions()(h.toUpperCase))
      case _ => Histograms.definitions()
    }
    val scalarList = conf.scalar.get match {
      case Some(s) => Map(s -> Histograms.definitions()(s.toUpperCase))
      case _ => Scalars.definitions()
    }

    val output = scalarList.flatMap {
      case(name: String, sd: ScalarDefinition) =>
        MetricAnalyzer.getAnalyzer(
          Scalars.getParquetFriendlyScalarName(name.toLowerCase, "parent"), sd, rows
        ).analyze()
    } ++
    histogramList.flatMap {
      case(name: String, hd: HistogramDefinition) =>
        MetricAnalyzer.getAnalyzer(
          name.toLowerCase, hd, rows
        ).analyze()
      case _ => List()
    }

    output.foreach(println)

    val o = spark.sparkContext.parallelize(output.toList)
    val df = spark.sqlContext.createDataFrame(o, buildOutputSchema)
    df.repartition(1).write.parquet(conf.outputLocation())
    spark.stop()
  }

  def buildStatisticSchema = StructType(List(
    StructField("comparison_branch", StringType, nullable = true),
    StructField("name", StringType, nullable = false),
    StructField("value", DoubleType, nullable = false),
    StructField("confidence_low", DoubleType, nullable = true),
    StructField("confidence_high", DoubleType, nullable = true),
    StructField("confidence_level", DoubleType, nullable = true),
    StructField("p_value", DoubleType, nullable = true)
  ))

  def buildHistogramPointSchema = StructType(List(
    StructField("pdf", DoubleType),
    StructField("count", LongType),
    StructField("label", StringType, nullable = true)
  ))

  def buildOutputSchema = StructType(List(
    StructField("experiment_name", StringType, nullable = false),
    StructField("experiment_branch", StringType, nullable = false),
    StructField("subgroup", StringType, nullable = true),
    StructField("n", LongType, nullable = false),
    StructField("metric_name", StringType, nullable = false),
    StructField("metric_type", StringType, nullable = false),
    StructField("histogram", MapType(LongType, buildHistogramPointSchema), nullable = false),
    StructField("statistics", ArrayType(buildStatisticSchema, containsNull = false), nullable = true)
  ))
}