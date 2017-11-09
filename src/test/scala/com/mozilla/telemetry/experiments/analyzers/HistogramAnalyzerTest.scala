package com.mozilla.telemetry.experiments.analyzers

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.mozilla.telemetry.metrics.{EnumeratedHistogram, LinearHistogram}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.Map


case class HistogramExperimentDataset(experiment_id: String,
                                      experiment_branch: String,
                                      histogram: Option[Map[Int, Int]],
                                      keyed_histogram: Option[Map[String, Map[Int, Int]]])

class HistogramAnalyzerTest extends FlatSpec with Matchers with DatasetSuiteBase {
  val m1 = Map(0 -> 1, 1 -> 2, 2 -> 3)
  val m2 = Map(0 -> 2, 1 -> 4, 2 -> 6)
  val m3 = Map(0 -> 1, 1 -> 2, 2 -> 3, 100 -> 1)
  val m4 = Map(0 -> 1, 1 -> 2, 2 -> 3, 5 -> 0)

  def fixture: DataFrame = {
    import spark.implicits._
    Seq(
      HistogramExperimentDataset("experiment1", "control", Some(m1), Some(Map("key1" -> m1, "key2" -> m2))),
      HistogramExperimentDataset("experiment1", "control", None, None),
      HistogramExperimentDataset("experiment1", "control", Some(Map(0 -> 1, 10000 -> 2)), None),
      HistogramExperimentDataset("experiment1", "control", Some(Map(0 -> 1, 1 -> -2)), None),
      HistogramExperimentDataset("experiment1", "control", Some(m2), Some(Map("hi" -> m2, "there" -> m3))),
      HistogramExperimentDataset("experiment1", "control", Some(m4), Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset("experiment1", "branch1", Some(m1), Some(Map("key1" -> m1, "key2" -> m2))),
      HistogramExperimentDataset("experiment1", "branch2", Some(m4), Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset("experiment1", "branch2", Some(m4), Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset("experiment2", "control", None, Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset("experiment3", "control", Some(m4), Some(Map("hi" -> m3, "there" -> m3))),
      HistogramExperimentDataset("experiment3", "branch2", Some(m1), Some(Map("key1" -> m1, "key2" -> m2)))
    ).toDS().toDF()
  }

  def partialToPoint(total: Double, label: Option[String])(v: Int): HistogramPoint = {
    HistogramPoint(v.toDouble/total, v.toLong, label)
  }

  "Non-keyed Histograms" can "be aggregated" in {
    val df = fixture
    val categoricalAnalyzer = new HistogramAnalyzer("histogram",
      EnumeratedHistogram(keyed = false, "name", 150),
      df.where(df.col("experiment_id") === "experiment1")
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
          Statistic(Some("branch2"), "Chi-Square Distance", 0.0)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "histogram", "EnumeratedHistogram",
        Map(0L -> toPointBranch1(1), 1L -> toPointBranch1(2), 2L -> toPointBranch1(3)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "histogram", "EnumeratedHistogram",
        Map(0L -> toPointBranch2(2), 1L -> toPointBranch2(4), 2L -> toPointBranch2(6), 5L -> toPointBranch2(0)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0)))))

      assert(actualCategorical == expectedCategorical)

    val numericAnalyzer = new HistogramAnalyzer("histogram",
      LinearHistogram(keyed = false, "name", 0, 149, 150),
      df.where(df.col("experiment_id") === "experiment1")
    )

    val actualNumeric = numericAnalyzer.analyze().toSet

    val expectedNumeric = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "histogram", "LinearHistogram",
        Map(0L -> toPointControl(4), 1L -> toPointControl(8), 2L -> toPointControl(12), 5L -> toPointControl(0)),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.0),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.0),
          Statistic(None, "Mean", 1.3333333333333333),
          Statistic(None, "Median", 1.0),
          Statistic(None, "25th Percentile", 1.0),
          Statistic(None, "75th Percentile", 2.0)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "histogram", "LinearHistogram",
        Map(0L -> toPointBranch1(1), 1L -> toPointBranch1(2), 2L -> toPointBranch1(3)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0),
          Statistic(None, "Mean", 1.3333333333333333),
          Statistic(None, "Median", 1.0),
          Statistic(None, "25th Percentile", 0.5),
          Statistic(None, "75th Percentile", 2.0)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "histogram", "LinearHistogram",
        Map(0L -> toPointBranch2(2), 1L -> toPointBranch2(4), 2L -> toPointBranch2(6), 5L -> toPointBranch2(0)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0),
          Statistic(None, "Mean", 1.3333333333333333),
          Statistic(None, "Median", 1.0),
          Statistic(None, "25th Percentile", 1.0),
          Statistic(None, "75th Percentile", 2.0)))))

    assert(actualNumeric == expectedNumeric)
  }

  "Keyed Histograms" can "be aggregated" in {
    val df = fixture
    val analyzer = new HistogramAnalyzer("keyed_histogram",
      EnumeratedHistogram(keyed = true, "name", 150),
      df.where(df.col("experiment_id") === "experiment1")
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
          Statistic(Some("branch2"), "Chi-Square Distance", 0.019470404984423675)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "keyed_histogram", "EnumeratedHistogram",
        Map(0L -> toPointBranch1(3), 1L -> toPointBranch1(6), 2L -> toPointBranch1(9)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.030303030303030304)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "keyed_histogram", "EnumeratedHistogram",
        Map(0L -> toPointBranch2(4), 1L -> toPointBranch2(8), 2L -> toPointBranch2(12), 100L -> toPointBranch2(4)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.019470404984423675)))))
    assert(actual == expected)
  }

  "Unknown histogram" should "return an empty list" in {
    val df = fixture
    val analyzer = new HistogramAnalyzer("unknown_histogram",
      EnumeratedHistogram(keyed = true, "name", 150),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze()

    import spark.implicits._
    val expected = List()
    assert(expected == actual)
  }

  "Invalid histograms" should "be dropped from analysis" in {
    val df = fixture
    val analyzer = new HistogramAnalyzer("histogram",
      EnumeratedHistogram(keyed = false, "name", 150),
      df.where(df.col("experiment_id") === "experiment1")
    )

    val actual = analyzer.analyze().filter(_.experiment_branch == "control").head
    assert(actual.n == 3L)
  }
}
