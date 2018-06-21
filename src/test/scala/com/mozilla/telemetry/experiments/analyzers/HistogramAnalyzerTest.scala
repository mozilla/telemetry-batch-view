/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.analyzers

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.mozilla.telemetry.metrics.{EnumeratedHistogram, LinearHistogram}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.Map


case class HistogramExperimentDataset(block_id: Int,
                                      experiment_id: String,
                                      experiment_branch: String,
                                      histogram: Option[Map[Int, Int]],
                                      keyed_histogram: Option[Map[String, Map[Int, Int]]])

class HistogramAnalyzerTest extends FlatSpec with Matchers with DatasetSuiteBase {
  val m1 = Map(0 -> 1, 1 -> 2, 2 -> 3)
  val m2 = Map(0 -> 2, 1 -> 4, 2 -> 6)
  val m3 = Map(0 -> 1, 1 -> 2, 2 -> 3, 100 -> 1)
  val m4 = Map(0 -> 1, 1 -> 2, 2 -> 3, 5 -> 0)

  lazy val fixture: DataFrame = {
    import spark.implicits._
    Seq(
      HistogramExperimentDataset(1, "experiment1", "control", Some(m1), Some(Map("key1" -> m1, "key2" -> m2))),
      HistogramExperimentDataset(2, "experiment1", "control", None, None),
      HistogramExperimentDataset(3, "experiment1", "control", Some(Map(0 -> 1, 10000 -> 2)), None),
      HistogramExperimentDataset(1, "experiment1", "control", Some(Map(0 -> 1, 1 -> -2)), None),
      HistogramExperimentDataset(2, "experiment1", "control", Some(m2), Some(Map("hi" -> m2, "there" -> m3))),
      HistogramExperimentDataset(3, "experiment1", "control", Some(m4), Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset(1, "experiment1", "branch1", Some(m1), Some(Map("key1" -> m1, "key2" -> m2))),
      HistogramExperimentDataset(2, "experiment1", "branch2", Some(m4), Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset(3, "experiment1", "branch2", Some(m4), Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset(1, "experiment2", "control", None, Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset(2, "experiment3", "control", Some(m4), Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset(3, "experiment3", "branch2", Some(m1), Some(Map("key1" -> m1, "key2" -> m2)))
    ).toDS().toDF()
  }

  def partialToPoint(total: Double, label: Option[String])(v: Int): HistogramPoint = {
    HistogramPoint(v.toDouble/total, v.toLong, label)
  }

  "Non-keyed Histograms" can "be aggregated" in {
    val df = fixture
    val categoricalAnalyzer = new HistogramAnalyzer("histogram",
      EnumeratedHistogram(keyed = false, "name", 150),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actualCategorical = categoricalAnalyzer.analyze().toSet

    def toPointControl: (Int => HistogramPoint) = partialToPoint(24.0, None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(6.0, None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(12.0, None)

    val expectedCategorical = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "histogram", "EnumeratedHistogram",
        Map(0L -> toPointControl(4), 1L -> toPointControl(8), 2L -> toPointControl(12), 5L -> toPointControl(0)),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.0),
          Statistic(Some("branch1"), "Mann-Whitney-U Distance", 72.0, None, None, None, Some(0.48867856487858774)),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.0),
          Statistic(Some("branch2"), "Mann-Whitney-U Distance", 144.0, None, None, None, Some(0.49267049900579374))))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "histogram", "EnumeratedHistogram",
        Map(0L -> toPointBranch1(1), 1L -> toPointBranch1(2), 2L -> toPointBranch1(3)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 72.0, None, None, None, Some(0.48867856487858774))))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "histogram", "EnumeratedHistogram",
        Map(0L -> toPointBranch2(2), 1L -> toPointBranch2(4), 2L -> toPointBranch2(6), 5L -> toPointBranch2(0)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 144.0, None, None, None, Some(0.49267049900579374))))))

      assert(actualCategorical == expectedCategorical)

    val numericAnalyzer = new HistogramAnalyzer("histogram",
      LinearHistogram(keyed = false, "name", 0, 149, 150),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )

    val actualNumeric = numericAnalyzer.analyze().toSet

    val expectedNumeric = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "histogram", "LinearHistogram",
        Map(0L -> toPointControl(4), 1L -> toPointControl(8), 2L -> toPointControl(12), 5L -> toPointControl(0)), None),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "histogram", "LinearHistogram",
        Map(0L -> toPointBranch1(1), 1L -> toPointBranch1(2), 2L -> toPointBranch1(3)), None),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "histogram", "LinearHistogram",
        Map(0L -> toPointBranch2(2), 1L -> toPointBranch2(4), 2L -> toPointBranch2(6), 5L -> toPointBranch2(0)), None))

    val expectedStats = Set(
      "Chi-Square Distance",
      "Mann-Whitney-U Distance",
      "Mean",
      "Median",
      "10th Percentile",
      "20th Percentile",
      "30th Percentile",
      "40th Percentile",
      "60th Percentile",
      "70th Percentile",
      "80th Percentile",
      "90th Percentile"
    )

    assert(actualNumeric.map(_.copy(statistics = None)) == expectedNumeric)
    actualNumeric.map(_.statistics.get.map(_.name).toSet).foreach(s => assert(s == expectedStats))
  }

  "Keyed Histograms" can "be aggregated" in {
    val df = fixture
    val analyzer = new HistogramAnalyzer("keyed_histogram",
      EnumeratedHistogram(keyed = true, "name", 150),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze().toSet

    def toPointControl: (Int => HistogramPoint) = partialToPoint(51.0, None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(18.0, None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(28.0, None)

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "keyed_histogram", "EnumeratedHistogram",
        Map(0L -> toPointControl(8), 1L -> toPointControl(16), 2L -> toPointControl(24), 100L -> toPointControl(3)),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.030303030303030304),
          Statistic(Some("branch1"), "Mann-Whitney-U Distance", 432.0, None, None, None, Some(0.34759898151474267)),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.019470404984423675),
          Statistic(Some("branch2"), "Mann-Whitney-U Distance", 654.0, None, None, None, Some(0.2570183003633981))))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "keyed_histogram", "EnumeratedHistogram",
        Map(0L -> toPointBranch1(3), 1L -> toPointBranch1(6), 2L -> toPointBranch1(9)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.030303030303030304),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 432.0, None, None, None, Some(0.34759898151474267))))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "keyed_histogram", "EnumeratedHistogram",
        Map(0L -> toPointBranch2(4), 1L -> toPointBranch2(8), 2L -> toPointBranch2(12), 100L -> toPointBranch2(4)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.019470404984423675),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 654.0, None, None, None, Some(0.2570183003633981))))))
    assert(actual == expected)
  }

  "Unknown histogram" should "return an empty list" in {
    val df = fixture
    val analyzer = new HistogramAnalyzer("unknown_histogram",
      EnumeratedHistogram(keyed = true, "name", 150),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze()

    val expected = List()
    assert(expected == actual)
  }

  "Invalid histograms" should "be dropped from analysis" in {
    val df = fixture
    val analyzer = new HistogramAnalyzer("histogram",
      EnumeratedHistogram(keyed = false, "name", 150),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )

    val actual = analyzer.analyze().filter(_.experiment_branch == "control").head
    assert(actual.n == 3L)
  }

  "Keyed histogram with null value" should "be discarded" in {
    import spark.implicits._
    val df = Seq(
      HistogramExperimentDataset(1, "experiment1", "control", Some(m1), Some(Map("key1" -> null, "key2" -> m2)))
    ).toDS().toDF()
    val analyzer = new HistogramAnalyzer("keyed_histogram",
      EnumeratedHistogram(keyed = true, "name", 150), df, 3
    )
    val actual = analyzer.analyze()
    val expected = List()
    assert(expected == actual)
  }
}
