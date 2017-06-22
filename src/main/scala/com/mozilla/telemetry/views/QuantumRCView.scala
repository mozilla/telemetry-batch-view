package com.mozilla.telemetry.views

import com.github.nscala_time.time.Imports._
import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._
import com.mozilla.telemetry.utils.UDFs._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.rogach.scallop._

/** Quantum Release Criteria View
  *
  * The purpose of this dataset is to verify that Quantum
  * is improving Firefox. To see specifically what the
  * criteria are, see bug 1366831.
  *
  * We use HLLs to get client_counts for a set of
  * values across a set of dimensions.
  */
object QuantumRCView {

  val TotalCountColName = "total_clients"
  val DatasetPrefix = "quantum_rc"
  val Version = "v1"
  val NumFiles = 16
  val WriteMode = "overwrite"
  val WeekPartitionName = "week"

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val to = opt[String](
      "to",
      descr = "To submission date, inclusive. Defaults to yesterday.",
      required = false)
    val bucket = opt[String](
      "bucket",
      descr = "Destination bucket for parquet data.",
      required = true)
    verify()
  }

  private val fmt = DateTimeFormat.forPattern("yyyyMMdd")

  // Joda time does not have a nice way of getting the first Sunday of the
  // current week, so this is a workaround.
  private def getFirstDayOfWeek(other: DateTime) = other.dayOfWeek.get match {
    case 7 => other
    case _ => other.minusWeeks(1).withDayOfWeek(7)
  }

  private def getFrom(conf: Conf): String = {
    fmt.print(getFirstDayOfWeek(fmt.parseDateTime(getTo(conf))))
  }

  private def getTo(conf: Conf): String = {
    conf.to.get match {
      case Some(t) => t
      case _ => fmt.print(DateTime.now.minusDays(1))
    }
  }

  private val dimensions =
    "app_name"           ::
    "app_version"        ::
    "app_build_id"       ::
    "normalized_channel" ::
    "country"            ::
    "gfx_compositor"     ::
    "e10s_enabled"       ::
    "os"                 ::
    "env_build_arch"     ::
    "quantum_ready"      ::
    "experiment_id"      ::
    "experiment_branch"  ::
    Nil

  private val counts = Map(
    "long_main_gc_or_cc_pause" -> "gc_max_pause_ms_main_above_150 > 0 OR cycle_collector_max_pause_main_above_150 > 0",
    "long_content_gc_or_cc_pause" -> "gc_max_pause_ms_content_above_2500 > 0 OR cycle_collector_max_pause_content_above_2500 > 0",
    "ghost_windows_main" -> "ghost_windows_main_above_1 > 0",
    "ghost_windows_content" -> "ghost_windows_content_above_1 > 0",
    "long_main_input_latency" -> "input_event_response_coalesced_ms_main_above_2500 > 0",
    "long_content_input_latency" -> "input_event_response_coalesced_ms_content_above_2500 > 0")

  private val sums = Map(
    "input_jank_main_over_2500" -> "input_event_response_coalesced_ms_main_above_2500",
    "input_jank_main_over_250" -> "input_event_response_coalesced_ms_main_above_250",
    "input_jank_content_over_2500" -> "input_event_response_coalesced_ms_content_above_2500",
    "input_jank_content_over_250" -> "input_event_response_coalesced_ms_content_above_250",
    "subsession_length" -> "subsession_length")

  def aggregate(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    // these are SHIELD experiments, not telemetry experiments
    val withExperiments = df
      .where("client_id IS NOT NULL")
      .select(col("*"),
        explode(
          when($"experiments".isNotNull && (size($"experiments") !== 0),
            $"experiments"
          ).otherwise(
            map(lit("none").cast("string"), lit(null).cast("string"))
          )
      ).as(Array("experiment_id", "experiment_branch")))

    val selection = s"$HllCreate(client_id, 12) as hll" ::
      dimensions ++
      counts.map{ case (k, v) => s"($v) as $k" } ++
      sums.map{ case(k, v) => s"$v AS $k" }

    val sumCols = sums.keys.map(k => sum(k).as(k)).toList
    val countCols = counts.keys.map(k => FilteredHllMerge($"hll", col(k)).as(k)).toList 

    withExperiments
      .selectExpr(selection: _*)
      .groupBy(dimensions.head, dimensions.tail: _*)
      .agg(HllMerge($"hll").as(TotalCountColName), sumCols ++ countCols: _*)
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .appName(s"$DatasetPrefix $Version Job")
      .getOrCreate()

    spark.registerUDFs

    val from = getFrom(conf)
    val to = getTo(conf)

    val df = spark.read.option("mergeSchema", "true")
      .parquet(s"s3://telemetry-parquet/${MainSummaryView.jobName}/${MainSummaryView.schemaVersion}")
      .where(s"$from <= submission_date_s3 AND submission_date_s3 <= $to")

    aggregate(df)
      .repartition(NumFiles)
      .write
      .mode(WriteMode)
      .parquet(s"s3://${conf.bucket()}/$DatasetPrefix/$Version/$WeekPartitionName=$from")

    spark.stop()
  }
}
