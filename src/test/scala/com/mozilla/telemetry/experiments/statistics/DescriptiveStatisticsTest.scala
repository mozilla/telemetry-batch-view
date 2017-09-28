package com.mozilla.telemetry.experiments.statistics

import org.scalatest.{FlatSpec, Matchers}
import com.mozilla.telemetry.experiments.analyzers.{HistogramPoint, MetricAnalysis, MetricAnalyzer, Statistic}

class DescriptiveStatisticsTest extends FlatSpec with Matchers {
  val histo = Map(
    1L -> HistogramPoint(0.2, 1.0, None),
    3L -> HistogramPoint(0.6, 3.0, None),
    10L -> HistogramPoint(0.2, 1.0, None)
  )
  val metric = MetricAnalysis(
    "test_experiment", "control", MetricAnalyzer.topLevelLabel, 3L, "Test Metric", "LinearHistogram", histo,
    Some(List(Statistic(None, "Random Stat", 1.0)))
  )

  "Mean statistic" should "compute correctly" in {
    val actual = Mean(histo).asStatistic
    actual.value should equal(4.0)
  }

  "Percentile statistics" should "compute correctly" in {
    val actual_median = Percentile(histo, 0.5, "Median").asStatistic
    actual_median.value should equal(3.0)

    val actual_25th = Percentile(histo, 0.25, "25th Percentile").asStatistic
    actual_25th.value should equal(1.5)
  }

  "Descriptive statistics" can "be added" in {
    val stats_obj = DescriptiveStatistics(metric)
    val stat_count = stats_obj.statisticsList.length
    val actual = stats_obj.getStatistics

    actual.length should equal(stat_count)
  }
}
