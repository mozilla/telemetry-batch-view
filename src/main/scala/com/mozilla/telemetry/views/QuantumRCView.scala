/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import java.time._
import java.time.format.DateTimeFormatter

import com.mozilla.telemetry.utils.UDFs._
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
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
object QuantumRCView extends BatchJobBase {

  val TotalCountColName = "total_clients"
  val DatasetPrefix = "quantum_rc"
  val Version = "v2"
  val NumFiles = 2
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

  private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")

  // Get Sunday as the first day of the week
  private def getFirstDayOfWeek(other: LocalDate) = other.getDayOfWeek() match {
    case DayOfWeek.SUNDAY => other
    case _ => other.minusWeeks(1).`with`(DayOfWeek.SUNDAY)
  }

  private def getFrom(conf: Conf): String = {
    fmt.format(getFirstDayOfWeek(LocalDate.parse(getTo(conf), fmt)))
  }

  private def getTo(conf: Conf): String = {
    conf.to.toOption match {
      case Some(t) => t
      case _ => LocalDate.now(clock).minusDays(1).format(fmt)
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

    val withoutExperiments = df
      .where("client_id IS NOT NULL")
      .drop($"experiments")

    val selection = s"$HllCreate(client_id, 12) as hll" ::
      dimensions ++
      counts.map{ case (k, v) => s"($v) as $k" } ++
      sums.map{ case(k, v) => s"$v AS $k" }

    val sumCols = sums.keys.map(k => sum(k).as(k)).toList
    val countCols = counts.keys.map(k => FilteredHllMerge($"hll", col(k)).as(k)).toList

    withoutExperiments
      .selectExpr(selection: _*)
      .groupBy(dimensions.head, dimensions.tail: _*)
      .agg(HllMerge($"hll").as(TotalCountColName), sumCols ++ countCols: _*)
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val spark = getOrCreateSparkSession(s"$DatasetPrefix $Version Job")

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

    if (shouldStopContextAtEnd(spark)) { spark.stop() }
  }
}
