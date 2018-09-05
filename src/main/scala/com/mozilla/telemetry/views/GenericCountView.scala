/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mozilla.telemetry.utils.UDFs._
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop._

object GenericCountView extends BatchJobBase {

  val DefaultSubmissionDateCol = "submission_date_s3"
  val DefaultHllBits = 12
  val DefaultOutputFiles = 32
  val DefaultWriteMode = "overwrite"

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
    val outputPartition = opt[String](
      "output-partition",
      descr = "Partition of the output data",
      required = false)
    val writeMode = opt[String](
      "write-mode",
      descr = "Spark write mode. Defaults to overwrite",
      required = false,
      default = Some(DefaultWriteMode))
    requireOne(inputTablename, inputFiles)
    verify()
  }

  private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")

  private def getFrom(conf: Conf): String = {
    conf.from.toOption match {
      case Some(t) => t
      case _ => LocalDate.parse(getTo(conf), fmt).minusMonths(6).format(fmt)
    }
  }

  private def getTo(conf: Conf): String = {
    conf.to.get match {
      case Some(t) => t
      case _ => LocalDate.now(clock).minusDays(1).format(fmt)
    }
  }

  def aggregate(spark: SparkSession, conf: Conf): DataFrame = {
    // To avoid parsing a SQL select statement,
    // we register as a temp table and let spark do it
    val tempTableName = "genericClientCountTempTable"

    val df = conf.inputTablename.get match {
      case Some(t) => spark.sql(s"SELECT * FROM $t")
      case _ => spark.read.option("mergeSchema", "true").load(conf.inputFiles())
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

    spark.sql(s"SELECT $selection FROM $tempTableName")
      .where(s"$from <= $submissionDateCol and $submissionDateCol <= $to $where")
      .groupBy(dimensions.head, dimensions.tail:_*)
      .agg(HllMerge(col("hll")).as("hll"))
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val spark = getOrCreateSparkSession("Generic Count View Job")

    spark.registerUDFs

    val sparkPartitions = conf.numParquetFiles()
    val from = getFrom(conf)
    val to = getTo(conf)

    val version = conf.version.get match {
      case Some(v) => v
      case _ => s"v$from$to"
    }

    val partition = conf.outputPartition.get match {
      case Some(p) => s"/$p"
      case _ => ""
    }

    aggregate(spark, conf)
      .repartition(sparkPartitions)
      .write
      .mode(conf.writeMode())
      .parquet(s"s3://${conf.outputBucket()}/$version$partition")

    if (shouldStopContextAtEnd(spark)) { spark.stop() }
  }
}
