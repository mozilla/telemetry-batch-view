/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.statistics

import com.mozilla.telemetry.experiments.analyzers.{HistogramPoint, MetricAnalysis, Statistic}
import com.mozilla.telemetry.statistics


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

case class MWUDistance(control: MetricAnalysis, experimental: MetricAnalysis)
  extends ComparativeStatistic(control, experimental) {
  val name = "Mann-Whitney-U Distance"

  def run: (Double, Double) = {
    val c = control.histogram map {
      case (k, v) => (k, v.count.toLong)
    }
    val e = experimental.histogram map {
      case (k, v) => (k, v.count.toLong)
    }
    statistics.mwu(c, e)
  }

  override def asStatistics: (Statistic, Statistic) = {
    val (u, p) = run
    (
      Statistic(Some(experimental.experiment_branch), name, u, p_value=Some(p)),
      Statistic(Some(control.experiment_branch), name, u, p_value=Some(p))
    )
  }
}

case class ComparativeStatistics(control: MetricAnalysis, experimental: MetricAnalysis) {
  val statisticsList = List(
    ChiSquareDistance(control, experimental),
    MWUDistance(control, experimental)
  )

  def getStatistics: Map[MetricAnalysis, List[Statistic]] = {
    val (c, e) = statisticsList.map(_.asStatistics).unzip
    Map(control -> c, experimental -> e)
  }
}
