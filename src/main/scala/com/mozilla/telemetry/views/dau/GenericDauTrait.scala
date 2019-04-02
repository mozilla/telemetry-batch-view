/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.dau

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS

import com.mozilla.telemetry.utils.{getOrCreateSparkSession, hadoopExists}
import com.mozilla.telemetry.utils.UDFs.{HllMerge, MozUDFs}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions => F}
import org.rogach.scallop.{ScallopConf, ScallopOption}

trait GenericDauTrait {
  val jobName: String
  def getGenericDauConf(args: Array[String]): GenericDauConf

  class BaseCliConf(args: Array[String]) extends ScallopConf(args) {
    val to: ScallopOption[String] = opt[String](
      "to",
      descr = "newest date for which to generate counts (inclusive)",
      required = true)
    val from: ScallopOption[String] = opt[String](
      "from",
      descr = "oldest date for which to generate counts, default is `--to` value (inclusive)",
      required = false)
    val bucket: ScallopOption[String] = opt[String](
      "bucket",
      default = Some("telemetry-parquet"),
      descr = "bucket where input and output data sets are stored",
      required = false)
    val bucketProto: ScallopOption[String] = opt[String](
      "bucket-protocol",
      default = Some("s3://"),
      descr = "hadoop compatible filesystem protocol to be used with --bucket",
      required = false)
  }

  def main(args: Array[String]) {
    val conf = getGenericDauConf(args)
    val spark = getOrCreateSparkSession(jobName)
    // date partition should be read with a string type to minimize string parsing
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    val input = spark
      .read
      // schema evolution detection disabled due to error on decimal type precision reduction
      // merge schemas to handle schema evolution
      //.option("mergeSchema", "true")
      // detect input date column
      .option("basePath", conf.inputBasePath)
      // read input dates by path
      .parquet(conf.inputPaths.filter(hadoopExists):_*)

    val result = aggregate(input, conf)

    // write one output date at a time to overwrite at the date level
    conf.outputDates.values.foreach { date =>
      result
        // get one day
        .where(s"${conf.outputDateColumn}='$date'")
        // drop old date column format
        .drop(conf.outputDateColumn)
        // write with new date column format
        .write
        .mode("overwrite")
        .parquet(s"${conf.outputBasePath}/${conf.outputDateColumn}=$date")
    }
  }

  private val SecondsPerDay: Int = 86400
  def aggregate(input: DataFrame, conf: GenericDauConf): DataFrame = {
    // register hll functions
    input.sparkSession.registerUDFs // for hll_create and hll_cardinality
    input.sparkSession.udf.register("hll_merge", HllMerge)

    // common window config
    val baseWindow = Window.orderBy(s"int_${conf.inputDateColumn}").partitionBy(conf.columns.map(F.col): _*)
    // window for calculating mau
    val mauWindow = baseWindow.rangeBetween(-conf.PrecedingDaysInMonth, Window.currentRow)
    // window for calculating smoothed_dau
    val smoothedDauWindow = baseWindow.rangeBetween(-conf.PrecedingDaysInWeek, Window.currentRow)
    val outputDates = conf.outputDates
    val outputDateUdf = F.udf((d: String)=>outputDates.getOrElse(d, null))

    val input_with_hll = input
      // handle conf.where
      .where(conf.where)
      // handle conf.columns
      .groupBy(conf.inputDateColumn, conf.columns: _*)
      // generate daily hlls
      .agg(F.expr(s"hll_merge(${conf.hllColumnExpression})").as(conf.hllColumn))

    // handle conf.top
    conf.top.foldRight(input_with_hll) { case ((column, limit), df) =>
      // use join to filter input by top "limit" "column" values (eg top 100 country values)
      df.join(
        input_with_hll
          // count by "column"
          .groupBy(column)
          .agg(F.expr(s"hll_cardinality(hll_merge(${conf.hllColumn}))").as("count"))
          // filter to top "limit" counts
          .orderBy(F.desc("count"))
          .limit(limit)
          // collect only "column" values
          .select(column),
        column
      )
    }
      // handle conf.minDauThreshold
      .where(conf.minDauThresholdWhere)
      // add temporary integer date column as days since epoch
      .withColumn(
      s"int_${conf.inputDateColumn}",
      F.floor(F.unix_timestamp(F.to_date(F.col(conf.inputDateColumn), conf.inputDatePattern)) / SecondsPerDay))
      // calculate mau hll
      .withColumn("mau", F.expr(s"hll_merge(${conf.hllColumn})") over mauWindow)
      // calculate cardinality separately to avoid AnalysisException
      .withColumn("mau", F.expr("hll_cardinality(mau)"))
      // calculate dau
      .withColumn("dau", F.expr(s"hll_cardinality(${conf.hllColumn})"))
      // don't calculate smoothed_dau for unnecessary
      .filter(F.col(conf.inputDateColumn).isin(conf.smoothedDauDates: _*))
      // calculate smoothed_dau
      .withColumn("smoothed_dau", F.expr("avg(dau)") over smoothedDauWindow)
      // drop temporary integer date column
      .drop(s"int_${conf.inputDateColumn}")
      // calculate er
      .withColumn("er", F.expr("smoothed_dau/mau"))
      // rename input date column to output date column
      .withColumnRenamed(conf.inputDateColumn, conf.outputDateColumn)
      // reformat input date format to output date format
      .withColumn(conf.outputDateColumn, outputDateUdf(F.col(conf.outputDateColumn)))
  }
}

/** Expanded configuration for GenericDauTrait.aggregate
  *
  * @param from oldest date in output date range (inclusive)
  * @param to newest date in output date range (inclusive)
  * @param inputBasePath path to an existing parquet table partitioned by "inputDateColumn"
  * @param outputBasePath path where output will be written, partitioned by "outputDateColumn"
  * @param additionalOutputColumns additional columns to include in output and break down dau by
  * @param countColumn optional column generate "hllColumn" from. if absent then "hllColumn" must be in input
  * @param hllBytes number of bytes to use when generating "hllColumn" from "countColumn"
  * @param hllColumn hll column used to generate counts
  * @param inputDateColumn name of date partition in input table
  * @param inputDatePattern java date pattern to use to interpret inputDateColumn
  * @param minDauThreshold optional filter for minimum value of dau in output (inclusive)
  * @param outputDateColumn name to use for date partition in output table
  * @param outputDatePattern java date pattern to use to interpret "outputDateColumn"
  * @param top for each (column,limit) pair filter input to top "limit" "column" values by hll count,
  *            such as ("country",100) for the top 100 countries by hll count,
  *            each "column" is included in output
  * @param where where clause by which to filter input, applies before "top"
  */
case class GenericDauConf(
                    // required params
                    from: String,
                    to: String,
                    inputBasePath: String,
                    outputBasePath: String,
                    // optional params
                    additionalOutputColumns: List[String] = Nil,
                    countColumn: Option[String] = None,
                    hllBytes: Int = 12,
                    hllColumn: String = "hll",
                    inputDateColumn: String = "submission_date",
                    inputDatePattern: String = "yyyyMMdd",
                    minDauThreshold: Option[Int] = None,
                    outputDateColumn: String = "submission_date_s3",
                    outputDatePattern: String = "yyyyMMdd",
                    top: List[(String,Int)] = Nil,
                    where: String = "TRUE"
                  ) {
  val PrecedingDaysInWeek: Int = 6
  val PrecedingDaysInMonth: Int = 27
  // parse date patterns
  private val inputDateFmt = DateTimeFormatter.ofPattern(inputDatePattern)
  private val outputDateFmt = DateTimeFormatter.ofPattern(outputDatePattern)
  // dates needed from input
  private val inputDates: List[String] = {
    val fromDate = LocalDate.parse(from, inputDateFmt)
    val toDate = LocalDate.parse(to, inputDateFmt)
    // 27 preceding days in input for calculating mau
    (0L to DAYS.between(fromDate.minusDays(PrecedingDaysInMonth), toDate))
      .map(toDate.minusDays(_).format(inputDateFmt)).toList
  }
  // convert inputDates to paths
  val inputPaths: List[String] = inputDates.map(date=>s"$inputBasePath/$inputDateColumn=$date")
  // dates used in smoothed_dau
  val smoothedDauDates: List[String] = inputDates.reverse.drop(PrecedingDaysInMonth-PrecedingDaysInWeek)
  // dates with a full mau
  val outputDates: Map[String,String] = inputDates
    .reverse
    .drop(27)
    .map(d=>d->LocalDate.parse(d, inputDateFmt).format(outputDateFmt))
    .toMap
  // automatically include columns in `top`
  val columns: List[String] = top.map(_._1) ::: additionalOutputColumns
  // convert option to where clause
  val minDauThresholdWhere: String = minDauThreshold
    .map(t=>s"hll_cardinality($hllColumn)>=$t")
    .getOrElse("TRUE")
  // convert countColumn to hll_create expression or hllColumn
  val hllColumnExpression: String = countColumn
    .map(c=>s"hll_create($c, $hllBytes)")
    .getOrElse(hllColumn)
}
