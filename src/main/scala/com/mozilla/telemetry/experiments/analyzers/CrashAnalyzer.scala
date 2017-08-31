package com.mozilla.telemetry.experiments.analyzers

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.types._

object CrashAnalyzer {

  val CrashTypes = 
    "main_crashes"             ::
    "content_crashes"          ::
    "gpu_crashes"              ::
    "plugin_crashes"           ::
    "gmplugin_crashes"         ::
    "content_shutdown_crashes" :: Nil

  val ComplexCrashTypes: Map[String, Column] = Map(
    "main_plus_content_crashes"                -> (sum("main_crashes") + sum("content_crashes")),
    "content_minus_shutdown_crashes"           -> (sum("content_crashes") - sum("content_shutdown_crashes")),
    "main_plus_content_minus_shutdown_crashes" -> (sum("main_crashes") + sum("content_crashes") - sum("content_shutdown_crashes")))

  val Extras =
    "usage_hours"      ::
    "subsession_count" :: Nil

  def makeRateName(name: String): String = name.dropRight(2) + "_rate"

  def makeTitle(name: String): String = name.split("_").map(_.capitalize).mkString(" ")

  def getExperimentCrashes(errorAggregates: DataFrame): Seq[MetricAnalysis] = {
    import errorAggregates.sparkSession.implicits._

    val aggregates = Extras.map{ c => sum(c).as(c) } ++
      CrashTypes.map{ c => sum(c).as(c) } ++
      ComplexCrashTypes.map{ case (k, v) => v.as(k) } ++
      CrashTypes.map{ c => (sum(c) / sum("usage_hours")).as(makeRateName(c)) } ++
      ComplexCrashTypes.map{ case (k, v) => (v / sum("usage_hours")).as(makeRateName(k)) }

    val crashes = errorAggregates
      .withColumn("subgroup", lit("All"))
      .groupBy(col("experiment_id"), col("experiment_branch"), col("subgroup"))
      .agg(aggregates.head, aggregates.tail: _*)
      .collect()

    val longMetrics = (Extras ++
      CrashTypes ++
      ComplexCrashTypes.keys.toList)
      .map((_, "Long"))

    val doubleMetrics = (CrashTypes.map(makeRateName) ++
      ComplexCrashTypes.keys.toList.map(makeRateName))
      .map((_, "Float"))

    val metrics = longMetrics ++ doubleMetrics

    metrics.flatMap{ case (metric, metricType) =>
      crashes.map { r =>
        val value = metricType match {
          case "Long" => r.getAs[Long](metric).toDouble
          case "Float" => r.getAs[Double](metric)
        }
        MetricAnalysis(r.getAs[String]("experiment_id"),
          r.getAs[String]("experiment_branch"),
          r.getAs[String]("subgroup"),
          r.getAs[Long]("subsession_count"),
          makeTitle(metric),
          metricType,
          Map.empty[Long, HistogramPoint],
          Some(Seq(
            Statistic(None, "value", value)
          ))
        )
      }
    }
  }
}
