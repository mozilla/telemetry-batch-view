package com.mozilla.telemetry.experiments.statistics

import com.mozilla.telemetry.experiments.analyzers.{HistogramPoint, MetricAnalysis, Statistic}


sealed abstract class ComparativeStatistic(control: MetricAnalysis, experimental: MetricAnalysis) {
  val name: String

  def run: (Double, Double)

  def asStatistics: (Statistic, Statistic) = {
    val (c, e) = run
    (
      Statistic(Some(experimental.experiment_branch), name, c),
      Statistic(Some(control.experiment_branch), name, e)
    )
  }
}

case class ChiSquareDistance(control: MetricAnalysis, experimental: MetricAnalysis)
  extends ComparativeStatistic(control, experimental) {
  val name = "Chi-Square Distance"

  def run: (Double, Double) = {
    val keys = control.histogram.keys.toSet ++ experimental.histogram.keys.toSet
    val d = keys.map(k => {
      val c = control.histogram.getOrElse(k, HistogramPoint(0.0, 0.0, None)).pdf
      val e = experimental.histogram.getOrElse(k, HistogramPoint(0.0, 0.0, None)).pdf
      c + e match {
        case 0 => 0.0
        case _ => math.pow(c - e, 2) / (c + e)
      }
    }).sum / 2
    (d, d)
  }
}

case class ComparativeStatistics(control: MetricAnalysis, experimental: MetricAnalysis) {
  val statisticsList = List(
    ChiSquareDistance(control, experimental)
  )

  def getStatistics: Map[MetricAnalysis, List[Statistic]] = {
    val (c, e) = statisticsList.map(_.asStatistics).unzip
    Map(control -> c, experimental -> e)
  }
}
