package com.mozilla.telemetry.experiments.analyzers

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.mozilla.telemetry.metrics.EnumeratedHistogram
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.{Map => CMap}


case class ExperimentDataset(experiment_id: String,
                             experiment_branch: String,
                             histogram: Option[CMap[Int, Int]],
                             keyed_histogram: Option[CMap[String, CMap[Int, Int]]])

class HistogramAnalyzerTest extends FlatSpec with Matchers with DatasetSuiteBase {
  val m1 = CMap(0 -> 1, 1 -> 2, 2 -> 3)
  val m2 = CMap(0 -> 2, 1 -> 4, 2 -> 6)
  val m3 = CMap(0 -> 1, 1 -> 2, 2 -> 3, 100 -> 1)
  val m4 = CMap(0 -> 1, 1 -> 2, 2 -> 3, 5 -> 0)

  def fixture = {
    import spark.implicits._
    Seq(
      ExperimentDataset("experiment1", "control", Some(m1), Some(CMap("key1" -> m1, "key2" -> m2))),
      ExperimentDataset("experiment1", "control", None, None),
      ExperimentDataset("experiment1", "control", Some(m2), Some(CMap("hi" -> m2, "there" -> m3))),
      ExperimentDataset("experiment1", "control", Some(m4), Some(CMap("hi" -> m3, "there" -> m3))),
      ExperimentDataset("experiment1", "branch1", Some(m1), Some(CMap("key1" -> m1, "key2" -> m2))),
      ExperimentDataset("experiment1", "branch2", Some(m4), Some(CMap("hi" -> m3, "there" -> m3))),
      ExperimentDataset("experiment1", "branch2", Some(m4), Some(CMap("hi" -> m3, "there" -> m3))),
      ExperimentDataset("experiment2", "control", None, Some(CMap("hi" -> m3, "there" -> m3))),
      ExperimentDataset("experiment3", "control", Some(m4), Some(CMap("hi" -> m3, "there" -> m3))),
      ExperimentDataset("experiment3", "branch2", Some(m1), Some(CMap("key1" -> m1, "key2" -> m2)))
    ).toDS().toDF()
  }

  def partialToPoint(total: Double, label: Option[String])(v: Int): HistogramPoint = {
    HistogramPoint(v.toDouble/total, v.toLong, label)
  }

  "Non-keyed Histograms" can "be aggregated" in {
    val df = fixture
    val analyzer = new HistogramAnalyzer("histogram",
      EnumeratedHistogram(false, 150),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().get.collect().toSet

    def toPointControl: (Int => HistogramPoint) = partialToPoint(24.0, None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(6.0, None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(12.0, None)

    val expected = Set(
      HistogramAnalysis("experiment1", "control", "All", 3L, "histogram", "EnumeratedHistogram",
        CMap(0L -> toPointControl(4), 1L -> toPointControl(8), 2L -> toPointControl(12), 5L -> toPointControl(0)), None),
      HistogramAnalysis("experiment1", "branch1", "All", 1L, "histogram", "EnumeratedHistogram",
        CMap(0L -> toPointBranch1(1), 1L -> toPointBranch1(2), 2L -> toPointBranch1(3)), None),
      HistogramAnalysis("experiment1", "branch2", "All", 2L, "histogram", "EnumeratedHistogram",
        CMap(0L -> toPointBranch2(2), 1L -> toPointBranch2(4), 2L -> toPointBranch2(6), 5L -> toPointBranch2(0)), None)
    )
    assert(actual == expected)
  }

  "Keyed Histograms" can "be aggregated" in {
    val df = fixture
    val analyzer = new HistogramAnalyzer("keyed_histogram",
      EnumeratedHistogram(true, 150),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().get.collect().toSet

    def toPointControl: (Int => HistogramPoint) = partialToPoint(51.0, None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(18.0, None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(28.0, None)

    val expected = Set(
      HistogramAnalysis("experiment1", "control", "All", 3L, "keyed_histogram", "EnumeratedHistogram",
        CMap(0L -> toPointControl(8), 1L -> toPointControl(16), 2L -> toPointControl(24), 100L -> toPointControl(3)), None),
      HistogramAnalysis("experiment1", "branch1", "All", 1L, "keyed_histogram", "EnumeratedHistogram",
        CMap(0L -> toPointBranch1(3), 1L -> toPointBranch1(6), 2L -> toPointBranch1(9)), None),
      HistogramAnalysis("experiment1", "branch2", "All", 2L, "keyed_histogram", "EnumeratedHistogram",
        CMap(0L -> toPointBranch2(4), 1L -> toPointBranch2(8), 2L -> toPointBranch2(12), 100L -> toPointBranch2(4)), None)
    )
    assert(actual == expected)
  }
}
