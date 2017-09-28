package com.mozilla.telemetry.experiments.statistics

import org.scalatest.{FlatSpec, Matchers}
import com.mozilla.telemetry.experiments.analyzers.{HistogramPoint, MetricAnalysis, MetricAnalyzer, Statistic}

class ComparativeStatisticsTest extends FlatSpec with Matchers {
  val control = MetricAnalysis(
    "test_experiment", "control", MetricAnalyzer.topLevelLabel, 3L, "Test Metric", "LinearHistogram",
    Map(1L -> HistogramPoint(0.2, 1.0, None),
      3L -> HistogramPoint(0.6, 3.0, None),
      10L -> HistogramPoint(0.2, 1.0, None)),
    Some(List(Statistic(None, "Random Stat", 1.0)))
  )

  val experimental = MetricAnalysis(
    "test_experiment", "experimental", MetricAnalyzer.topLevelLabel, 3L, "Test Metric", "LinearHistogram",
    Map(1L -> HistogramPoint(0.2, 1.0, None),
      3L -> HistogramPoint(0.4, 2.0, None),
      10L -> HistogramPoint(0.4, 2.0, None)),
    None)


  "Chi-square statistic" should "compute correctly" in {
    val no_distance = ChiSquareDistance(control, control).asStatistics
    no_distance._1.value should be(0.0)
    no_distance._2.value should be(0.0)

    val actual = ChiSquareDistance(control, experimental).asStatistics
    actual._1.value should be(0.05333333333333332)
    actual._2.value should be(0.05333333333333332)
  }

  "Comparative statistics" can "be returned" in  {
    val stats_obj = ComparativeStatistics(control, experimental)
    val stat_count = stats_obj.statisticsList.length
    val actual = stats_obj.getStatistics
    actual(control).length should equal(stat_count)
    actual(experimental).length should equal(stat_count)
  }
}
