package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils.CollectList

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import com.github.nscala_time.time.Imports._
import org.rogach.scallop._

object GenericLongitudinalView {

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val inputTablename = opt[String](
      "tablename",
      descr = "Table to pull data from",
      required = true)
    val from = opt[String](
      "from",
       descr = "From submission date. Defaults to 6 months before `to`.",
       required = false)
    val to = opt[String](
      "to",
       descr = "To submission date",
       required = true)
    val where = opt[String](
      "where",
      descr = "Where SQL clause, filtering input data. E.g. \"normalized_channel = 'nightly'\"",
      required = false)
    val groupingColumn = opt[String](
      "grouping-column",
      descr = "Column to group data by. Defaults to client_id",
      required = false)
    val orderingColumns = opt[String](
      "ordering-columns",
      descr = "Columns to order the arrays by (comma separated). Defaults to submissionDateCol",
      required = false)
    val outputPath = opt[String](
      "output-path",
      descr = "Path where data will be outputted",
      required = true)
    val submissionDateCol = opt[String](
      "submission-date-col",
      descr = "Name of the submission date column. Defaults to submission_date_s3",
      required = false)
    val maxArrayLength = opt[Int](
      "max-array-length",
      descr = "Max length of any groups history. Defaults to no limit. Negatives are ignored",
      required = false)
    val numParquetFiles = opt[Int](
      "num-parquet-files",
      descr = "Number of parquet files to output. Defaults to the number of input files",
      required = false)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val fmt = DateTimeFormat.forPattern("yyyyMMdd")

    val inputTablename = opts.inputTablename()

    val to = opts.to()

    val from = opts.from.get match {
      case Some(f) => f
      case _ => fmt.print(fmt.parseDateTime(to).minusMonths(6))
    }

    val where = opts.where.get match {
      case Some(f) => s"AND $f"
      case _ => ""
    }

    val groupingColumn = opts.groupingColumn.get match {
      case Some(gc) => gc
      case _ => "client_id"
    }

    val submissionDateCol = opts.groupingColumn.get match {
      case Some(sd) => sd
      case _ => "submission_date_s3"
    }

    val orderingColumns = opts.orderingColumns.get match {
      case Some(oc) => oc.split(",").toList
      case _ => List(submissionDateCol)
    }

    val maxArrayLength = opts.maxArrayLength.get

    val outputPath = opts.outputPath()

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    val data = hiveContext.sql(s"""
      SELECT *
      FROM $inputTablename
      WHERE $from <= $submissionDateCol
        AND $submissionDateCol <= $to
        $where
    """)

    val numParquetFiles = opts.numParquetFiles.get match {
      case Some(n) => n
      case _ => data.rdd.getNumPartitions
    }

    group(data, groupingColumn, orderingColumns, maxArrayLength)
      .repartition(numParquetFiles)
      .write
      .parquet(s"s3://$outputPath")

    sc.stop()
  }

  def group(df: DataFrame, groupColumn: String = "client_id", orderColumns: List[String] = List("submission_date_s3"), maxLength: Option[Int] = None): DataFrame = {
    // We can't order on the grouping column
    assert(!orderColumns.contains(groupColumn))

    // Group and create new array columns
    val nonGroupColumns =
      df.schema.fieldNames
      .filter(_ != groupColumn)
      .toList

    val subSchema = df.schema(nonGroupColumns.toSet)
    val colNames = groupColumn :: nonGroupColumns
    val collectListUdf = new CollectList(subSchema, orderColumns, maxLength)
    val newCol = collectListUdf(nonGroupColumns.map(col(_)): _*)
    val aggregateColName = "aggcol"

    val aggregated =
      df.groupBy(groupColumn)
      .agg(newCol)
      .toDF(groupColumn, aggregateColName)

    aggregated.select(col(groupColumn) :: getChildren(aggregateColName, aggregated): _*)
  }

  def getChildren(colname: String, df: DataFrame) = {
    df.schema(colname)
      .dataType
      .asInstanceOf[StructType]
      .fields
      .map(x => col(s"$colname.${x.name}"))
      .toList
  }
}
