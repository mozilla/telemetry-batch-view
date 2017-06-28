package com.mozilla.telemetry.experiments.statistics

import org.apache.commons.math3.stat.inference.MannWhitneyUTest
import com.mozilla.telemetry.experiments.analyzers.{HistogramPoint, MetricAnalysis, Statistic}

import scala.collection.Map

class PairwiseAggregateTests(x: MetricAnalysis, y: MetricAnalysis) {
  implicit class HistogramMap(h: Map[Long, HistogramPoint]) {
    def flat: Array[Double] = {
      h.toArray.flatMap {case (v, p) => Array.fill(p.count.toInt)(v.toDouble)}
    }
  }

  def mannWhitneyUTest: (Statistic, Statistic) = {
    val testName = "Mann-Whitney U test"
    val flatX = x.histogram.flat
    val flatY = y.histogram.flat
    val test = new MannWhitneyUTest()
    val uValue = test.mannWhitneyU(flatX, flatY)
    val pValue = test.mannWhitneyUTest(flatX, flatY)
    (Statistic(Some(y.experiment_branch), testName, uValue, None, None, None, Some(pValue)),
    Statistic(Some(x.experiment_branch), testName, uValue, None, None, None, Some(pValue)))
  }

  def runAllTests: Map[String, Array[Statistic]] = {
    // for now, we're running Mann Whitney on all tests
    val (xStat, yStat) = mannWhitneyUTest
    Map(x.experiment_branch -> Array(xStat), y.experiment_branch -> Array(yStat))
  }
}
