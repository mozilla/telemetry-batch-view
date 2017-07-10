package com.mozilla.telemetry.views

import com.mozilla.telemetry.experiments.analyzers.{HistogramAnalyzer, ScalarAnalyzer}
import com.mozilla.telemetry.metrics._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.functions.col

object ExperimentAnalysisView {
  def SchemaVersion: String = "v1"
  def JobName: String = "experiments_aggregates"
  def NumParquetFiles: Int = 1

  // All non-scalars and non-numeric scalars are ignored
  def Blacklist = 
    "client_id" ::
    "document_id" ::
    "experiment_branch" ::
    "experiment_id" ::
    "install_year" ::
    "is_wow64" ::
    "memory_mb" ::
    "normalized_channel" ::
    "os_service_pack_major" ::
    "os_service_pack_minor" ::
    "profile_creation_date" ::
    "profile_subsession_counter" ::
    "sample_id" ::
    "submission_date_s3" ::
    "subsession_counter" ::
    "timestamp" ::
    "timezone_offset" ::
    "windows_build_number" ::
    "windows_ubr" :: Nil

  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val inputBucket = opt[String]("input", descr = "Source bucket for parquet data", required = false)
    val outputBucket = opt[String]("output", descr = "Destination bucket for parquet data", required = false)
    val inputLocation = opt[String]("input-location", descr = "Exact location for parquet data", required = false)
    val outputLocation = opt[String]("output-location", descr = "Exact location for output data", required = false)
    val metric = opt[String]("metric", descr = "Run job on just this metric", required = false)
    val experiment = opt[String]("experiment", descr = "Run job on just this experiment", required = false)
    val date = opt[String]("date", descr = "Run date for this job (defaults to yesterday)", required = false)

    requireOne(outputBucket, inputLocation)
    requireOne(outputBucket, outputLocation)
    conflicts(outputBucket, List(inputLocation, outputLocation))
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(JobName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    val inputLocation = getInputLocation(conf)
    val baseOutputLocation = getOutputLocation(conf)
    val data = spark.read.parquet(inputLocation)
    val date = getDate(conf)

    getExperiments(conf, data).foreach{ e: String => 
      val outputLocation = s"$baseOutputLocation/experiment_id=$e/date=$date"
      getExperimentMetrics(e, data, conf)
        .drop(col("experiment_id"))
        .repartition(NumParquetFiles)
        .write.mode("overwrite").parquet(outputLocation)
    }

    spark.stop()
  }

  def getInputLocation(conf: Conf): String = {
    conf.outputBucket.get match {
      case Some(b) => s"s3://${conf.inputBucket.get.getOrElse(b)}/${ExperimentSummaryView.jobName}/${ExperimentSummaryView.schemaVersion}"
      case _ => conf.inputLocation.get.get
    }
  }

  def getOutputLocation(conf: Conf): String => String = {
    conf.outputBucket.get match {
      case Some(b) => s"s3://$b/$JobName/$SchemaVersion"
      case _ => conf.outputLocation.get.get
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
        val histogramDefs = Histograms.definitions()
        val scalarDefs = Scalars.definitions()
        val otherDefs = getOtherDefinitions(data, scalarDefs.toList, histogramDefs.toList).toMap
        List((m, (histogramDefs ++ scalarDefs ++ otherDefs)(m.toUpperCase)))
      }
      case _ => {
        val histogramDefs = MainSummaryView.filterHistogramDefinitions(Histograms.definitions(), useWhitelist = true)
        val scalarDefs = Scalars.definitions(includeOptin = true).toList
        scalarDefs ++ histogramDefs ++ getOtherDefinitions(data, scalarDefs, histogramDefs)
      }
    }
  }

  def getExperimentMetrics(experiment: String, data: DataFrame, conf: Conf): DataFrame = {
    val metricList = getMetrics(conf, data)
    val experimentData = data.where(col("experiment_id") === experiment)

    metricList.map {
      case (name: String, hd: HistogramDefinition) =>
        val columnName = MainSummaryView.getHistogramName(name.toLowerCase, "parent")
        new HistogramAnalyzer(columnName, hd, experimentData).analyze()
      case (name: String, sd: DerivedScalarDefinition) =>
        ScalarAnalyzer.getAnalyzer(name, sd, experimentData).analyze()
      case (name: String, sd: ScalarDefinition) =>
        val columnName = Scalars.getParquetFriendlyScalarName(name, "parent")
        ScalarAnalyzer.getAnalyzer(columnName, sd, experimentData).analyze()
      case _ => throw new UnsupportedOperationException("Unsupported metric definition type")
    }.reduce(_.union(_)).toDF()
  }

  def getOtherDefinitions(df: DataFrame, scalarDefs: List[(String, ScalarDefinition)], histogramDefs: List[(String, HistogramDefinition)]): List[(String, MetricDefinition)] = {
    val excludedFields = 
      scalarDefs.map(_._1).toSet ++ 
      histogramDefs.map(_._1).toSet ++
      Blacklist.toSet

    df.schema.fields
      .filter(sf => !excludedFields.contains(sf.name))
      .map{ sf => sf.dataType match {
          case _: IntegerType => Some((sf.name, new UintDerivedScalar))
          case _: LongType => Some((sf.name, new LongDerivedScalar))
          case _: BooleanType => Some((sf.name, new BooleanDerivedScalar))
          case _ => None
        }
      }
      .flatten
      .toList
  }
}
