package com.mozilla.telemetry.experiments

import scala.collection.{Map => CMap}


package object analyzers {
  def sumCMaps[T](l: CMap[T, Int], r: CMap[T, Int]): CMap[T, Int] = {
    l ++ r.map { case (k, v) => k -> (v + l.getOrElse(k, 0)) }
  }

  def sumMaps[T](l: Map[T, Long], r: Map[T, Long]): Map[T, Long] = {
    l ++ r.map { case (k, v) => k -> (v + l.getOrElse(k, 0L)) }
  }

  def addElement[T](m: CMap[T, Long], e: T): CMap[T, Long] = {
    m + (e -> (m.getOrElse(e, 0L) + 1L))
  }

  case class MetricKey(experiment_id: String, branch: String, subgroup: String)

  // pdf is the count normalized by counts for all buckets
  case class HistogramPoint(pdf: Double, count: Double, label: Option[String])

  case class Statistic(comparison_branch: Option[String],
                       name: String,
                       value: Double,
                       confidence_low: Double,
                       confidence_high: Double,
                       confidence_level: Double,
                       p_value: Double)

  case class HistogramAnalysis(experiment_id: String,
                               experiment_branch: String,
                               subgroup: String,
                               n: Long,
                               metric_name: String,
                               metric_type: String,
                               histogram: CMap[Long, HistogramPoint],
                               statistic: Option[Seq[Statistic]])
}
