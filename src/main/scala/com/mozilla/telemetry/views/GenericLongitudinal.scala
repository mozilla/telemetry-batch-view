package com.mozilla.telemetry.views

import com.github.nscala_time.time.Imports._
import com.mozilla.telemetry.utils.{CollectList, getOrCreateSparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.rogach.scallop._

object GenericLongitudinalView {

  val DefaultWriteMode = "overwrite"

  class Opts(args: Array[String]) extends ScallopConf(args) {
    val inputTablename = opt[String](
      "tablename",
      descr = "Table to pull data from",
      required = true)
    val inputFiles = opt[String](
      "files",
      descr = "s3 location to pull data from. Cannot be used with --tablename",
      required = false)
    val from = opt[String](
      "from",
       descr = "From submission date. Defaults to 6 months before `to`.",
       required = false)
    val to = opt[String](
      "to",
       descr = "To submission date",
       required = true)
    val submissionDateCol = opt[String](
      "submission-date-col",
      descr = "Name of the submission date column. Defaults to submission_date_s3",
      required = false)
    val selection = opt[String](
      "select",
      descr = "Select statement to retrieve data with; e.g. \"substr(subsession_start_date, 0, 10) as activity_date\". " +
              "If none given, defaults to all input columns",
      required = false)
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
    val maxArrayLength = opt[Int](
      "max-array-length",
      descr = "Max length of any groups history. Defaults to no limit. Negatives are ignored",
      required = false)
    val outputPath = opt[String](
      "output-path",
      descr = "Path where data will be outputted",
      required = true)
    val numParquetFiles = opt[Int](
      "num-parquet-files",
      descr = "Number of parquet files to output. Defaults to the number of input files",
      required = false)
    val version = opt[String](
      "version",
      descr = "The version for the output. Defaults to v<from><to>",
      required = false)
    val writeMode = opt[String](
      "write-mode",
      descr = "Spark write mode. Defaults to overwrite",
      required = false,
      default = Some(DefaultWriteMode))
    requireOne(inputTablename, inputFiles)
    verify()
  }

  def getFrom(opts: Opts): String = {
    val fmt = DateTimeFormat.forPattern("yyyyMMdd")
    val to = opts.to()

    opts.from.get match {
      case Some(f) => f
      case _ => fmt.print(fmt.parseDateTime(to).minusMonths(6))
    }
  }

  def getInputTable(sqlContext: SQLContext, opts: Opts): DataFrame = {
    opts.inputTablename.get match {
      case Some(t) => sqlContext.sql(s"SELECT * FROM $t")
      case _ => sqlContext.read.load(opts.inputFiles())
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = getOrCreateSparkSession(this.getClass.getName, enableHiveSupport = true)
    val opts = new Opts(args)

    val numParquetFiles = opts.numParquetFiles.get match {
      case Some(n) => n
      case _ => getInputTable(spark.sqlContext, opts).rdd.getNumPartitions
    }

    val from = getFrom(opts)
    val to = opts.to()

    val version = opts.version.get match {
      case Some(v) => v
      case _ => s"v$from$to"
    }

    val outputPath = opts.outputPath()

    run(spark.sqlContext, opts)
      .repartition(numParquetFiles)
      .write
      .mode(opts.writeMode())
      .parquet(s"s3://$outputPath/$version")

    spark.stop()
  }

  def run(sqlContext: SQLContext, opts: Opts): DataFrame = {

    val df = getInputTable(sqlContext, opts)
    val tempTableName = "genericLongitudinalTempTable"
    df.registerTempTable(tempTableName)

    val selection = opts.selection.get match {
      case Some(s) => s
      case _ => "*"
    }

    val submissionDateCol = opts.submissionDateCol.get match {
      case Some(sd) => sd
      case _ => "submission_date_s3"
    }

    val from = getFrom(opts)
    val to = opts.to()

    val where = opts.where.get match {
      case Some(f) => s"AND $f"
      case _ => ""
    }

    val data = sqlContext.sql(s"""
      SELECT $selection
      FROM $tempTableName
      WHERE $from <= $submissionDateCol
        AND $submissionDateCol <= $to
        $where
    """)

    val groupingColumn = opts.groupingColumn.get match {
      case Some(gc) => gc
      case _ => "client_id"
    }

    val orderingColumns = opts.orderingColumns.get match {
      case Some(oc) => oc.split(",").toList
      case _ => List(submissionDateCol)
    }

    val maxArrayLength = opts.maxArrayLength.get

    group(data, groupingColumn, orderingColumns, maxArrayLength)
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
