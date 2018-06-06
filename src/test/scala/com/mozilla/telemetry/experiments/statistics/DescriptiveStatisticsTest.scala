/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.statistics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.experiments.analyzers._
import com.mozilla.telemetry.metrics.UintScalar
import org.apache.spark.sql.functions.{count, mean, stddev_samp}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{FlatSpec, Matchers}

import scala.math.{ceil, floor, round}
import scala.util.Random

class DescriptiveStatisticsTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  val NumSamples = 10000
  val NumBlocks = 100
  val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  lazy val fixtures: (Dataset[Int], scala.collection.Map[Long, HistogramPoint], Dataset[MetricAnalysis]) = {
    import spark.implicits._
    val rand = new Random(42)

    // Generate normally distributed uint scalar values and a jackknife block assignment for each value
    val bareNormal = (0 to NumSamples)
      // Shift and widen the normal curve (since we need integers to make these uint scalars)
      .map(_ => (round(((rand.nextGaussian + 10) * 10)).toInt, rand.nextInt(NumBlocks)))
      .toDS

    // Transform the scalars into a shape we can use for the aggregation functions
    val normalDS = bareNormal
      .map {case (n, b) => UintScalarMapRow("a", "b", MetricAnalyzer.topLevelLabel, Some(Map(n -> 1L)), b)}
    normalDS.count

    val analyzer = new UintScalarAnalyzer("example", UintScalar(false, "example"), List.empty[Int].toDF, NumBlocks)
    val grouped = analyzer.aggregateBlocks(normalDS).persist()

    // Return the actual scalar values, the actual aggregate, and the aggregated jackknife pseudosamples
    (bareNormal.map(_._1), analyzer.aggregateAll(grouped).head.histogram, analyzer.aggregatePseudosamples(grouped))
  }

  "Mean statistic" should "compute correctly" in {
    val (asInts, histo, jackknifeAggs) = fixtures
    val actual = Mean(histo, Some(jackknifeAggs)).asStatistic
    val (m, stdDev, n) = asInts.agg(mean("value"), stddev_samp("value"), count("*")).first match {
      case Row(m: Double, s: Double, n: Long) => (m, s, n)
      case _ => fail("This should never happen")
    }

    actual.value should equal(m)

    // compute the standard 99% CI for the mean for a normal distribution
    val expectedInterval = ZScore99 * (stdDev / scala.math.sqrt(n.toDouble))
    actual.confidence_low.get should equal(m - expectedInterval +- (m * 0.005))
    actual.confidence_high.get should equal(m + expectedInterval +- (m * 0.005))
  }

  "Percentile statistics" should "compute correctly" in {
    val (asInts, histo, jackknifeAggs) = fixtures
    val sortedIntArray = asInts.sort("value").collect()
    val actualMedian = Percentile(histo, 0.5, "Median", Some(jackknifeAggs)).asStatistic
    // Change this if we tune NumSamples to an odd number
    val expected = sortedIntArray((NumSamples.toDouble * 0.5).toInt)
    actualMedian.value should equal(expected)

    // Using non-parametric interval calc of n(p) +- z * sqrt(n * p * (1 - p)) (where p is the percentile)
    val ordinalInterval = ZScore99 * scala.math.sqrt(NumSamples.toDouble * 0.5 * 0.5)
    val expectedLower = sortedIntArray(floor(NumSamples.toDouble * 0.5 - ordinalInterval).toInt)
    val expectedUpper = sortedIntArray(ceil(NumSamples.toDouble * 0.5 + ordinalInterval).toInt)

    actualMedian.confidence_low.get should equal (expectedLower.toDouble +- 1) // +- 1 since these are discrete
    actualMedian.confidence_high.get should equal (expectedUpper.toDouble +- 1)

    val actual25th = Percentile(histo, 0.25, "25th Percentile", Some(jackknifeAggs)).asStatistic
    // Change this if we tune NumSamples to a number not divisible by 4
    val expected25th = sortedIntArray((NumSamples.toDouble * 0.25).toInt)
    actual25th.value should equal(expected25th)

    val ordinalInterval25th = ZScore99 * scala.math.sqrt(NumSamples.toDouble * 0.25 * 0.75)
    val expectedLower25th = sortedIntArray(floor(NumSamples.toDouble * 0.25 - ordinalInterval25th).toInt)
    val expectedUpper25th = sortedIntArray(ceil(NumSamples.toDouble * 0.25 + ordinalInterval25th).toInt)

    actual25th.confidence_low.get should equal (expectedLower25th.toDouble +- 1)
    actual25th.confidence_high.get should equal (expectedUpper25th.toDouble +- 1)
  }

  "Descriptive statistics" can "be added" in {
    val (asInts, histo, jackknifeAggs) = fixtures

    val metric = MetricAnalysis(
      "test_experiment", "control", MetricAnalyzer.topLevelLabel, 3L, "Test Metric", "LinearHistogram", histo,
      Some(List(Statistic(None, "Random Stat", 1.0)))
    )

    val stats_obj = DescriptiveStatistics(metric, Some(jackknifeAggs))
    val stat_count = stats_obj.statisticsList.length
    val actual = stats_obj.getStatistics

    actual.length should equal(stat_count)
  }
}
