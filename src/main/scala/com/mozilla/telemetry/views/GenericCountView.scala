package com.mozilla.telemetry.views

import com.github.nscala_time.time.Imports._
import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.rogach.scallop._

object GenericCountView {

  val DefaultSubmissionDateCol = "submission_date_s3"
  val DefaultHllBits = 12
  val DefaultOutputFiles = 32

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String](
      "from",
      descr = "From submission date. Defaults to six months before yesterday with format YYYYMMDD.",
      required = false)
    val to = opt[String](
      "to",
      descr = "To submission date. Defaults to yesterday.",
      required = false)
    val inputTablename = opt[String](
      "tablename",
      descr = "Table to pull data from. Cannot be used with --files",
      required = false)
    val inputFiles = opt[String](
      "files",
      descr = "s3 location to pull data from. Cannot be used with --tablename",
      required = false)
    val submissionDateCol = opt[String](
      "submission-date-col",
      descr = "Name of the submission date column. Defaults to submission_date_s3",
      required = false,
      default = Some(DefaultSubmissionDateCol))
    val countCol = opt[String](
      "count-column",
      descr = "Column which will have distinct counts of per set of dimensions",
      required = true)
    val hllBits = opt[Int](
      "hll-bits",
      descr = "Number of bits to use for hll. 12 bits corresponds to an error of .0163. Defaults to 12",
      required = false,
      default = Some(DefaultHllBits))
    val selection = opt[String](
      "select",
      descr = "Select statement to retrieve data with; e.g. \"substr(subsession_start_date, 0, 10) as activity_date\"",
      required = true)
    val dimensions = opt[String](
      "grouping-columns",
      descr = "Columns along which counts will be made; e.g. \"activity_date\"",
      required = true)
    val where = opt[String](
      "where",
      descr = "Filter statement for the incoming data; e.g. \"client_id IS NOT NULL\"",
      required = false)
    val outputBucket = opt[String](
      "output",
      descr = "Destination output for parquet data. E.g. \"telemetry-parquet/client_count\"",
      required = true)
    val numParquetFiles = opt[Int](
      "num-parquet-files",
      descr = "Number of parquet files to output. Defaults to 32",
      required = false,
      default = Some(DefaultOutputFiles))
    val version = opt[String](
      "version",
      descr = "Version of the output data. Defaults to v<from><to>",
      required = false)
    requireOne(inputTablename, inputFiles)
    verify()
  }

  private val hllMerge = new HyperLogLogMerge

  private val fmt = DateTimeFormat.forPattern("yyyyMMdd")

  private def getFrom(conf: Conf): String = {
    conf.from.get match {
      case Some(t) => t
      case _ => fmt.print(fmt.parseDateTime(getTo(conf)).minusMonths(6))
    }
  }

  private def getTo(conf: Conf): String = {
    conf.to.get match {
      case Some(t) => t
      case _ => fmt.print(DateTime.now.minusDays(1))
    }
  }

  def aggregate(sc: SparkContext, conf: Conf): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    sqlContext.udf.register("hll_create", hllCreate _)

    // To avoid parsing a SQL select statement,
    // we register as a temp table and let spark do it
    val tempTableName = "genericClientCountTempTable"

    val df = conf.inputTablename.get match {
      case Some(t) => sqlContext.sql(s"SELECT * FROM $t")
      case _ => sqlContext.read.load(conf.inputFiles())
    }

    df.registerTempTable(tempTableName)

    val from = getFrom(conf)
    val to = getTo(conf)

    val where = conf.where.get match {
      case Some(f) => s"AND $f"
      case _ => ""
    }

    val selection = s"hll_create(${conf.countCol()}, ${conf.hllBits()}) as hll," + conf.selection()
    val submissionDateCol = conf.submissionDateCol()
    val dimensions = conf.dimensions().split(",")

    sqlContext.sql(s"SELECT $selection FROM $tempTableName")
      .where(s"$from <= $submissionDateCol and $submissionDateCol <= $to $where")
      .groupBy(dimensions.head, dimensions.tail:_*)
      .agg(hllMerge(col("hll")).as("hll"))
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val sparkConf = new SparkConf().setAppName("GenericCountJob")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)

    val partitions = conf.numParquetFiles()
    val from = getFrom(conf)
    val to = getTo(conf)

    val version = conf.version.get match {
      case Some(v) => v
      case _ => s"v$from$to"
    }

    aggregate(sc, conf)
      .repartition(partitions)
      .write
      .mode("overwrite")
      .parquet(s"s3://${conf.outputBucket()}/$version")

    sc.stop()
  }
}
