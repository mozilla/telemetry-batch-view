package com.mozilla.telemetry.experiments.analyzers

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.mozilla.telemetry.metrics._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.Map


case class ScalarExperimentDataset(experiment_id: String,
                                   experiment_branch: String,
                                   uint_scalar: Option[Int],
                                   keyed_uint_scalar: Option[Map[String, Int]],
                                   boolean_scalar: Option[Boolean],
                                   keyed_boolean_scalar: Option[Map[String, Boolean]],
                                   string_scalar: Option[String],
                                   keyed_string_scalar: Option[Map[String, String]])

case class ConfidenceIntervalsDataset(experiment_id: String, experiment_branch: String, uint_scalar: Option[Int])


class ScalarAnalyzerTest extends FlatSpec with Matchers with DatasetSuiteBase {
  val keyed_uint = Map("key1" -> 3, "key2" -> 1)
  val keyed_boolean1 = Map("key1" -> false, "key2" -> false)
  val keyed_boolean2 = Map("key1" -> true, "key2" -> false)
  val keyed_string = Map("key1" -> "hello", "key2" -> "world")

  def fixture: DataFrame = {
    import spark.implicits._
    Seq(
      ScalarExperimentDataset("experiment1", "control", Some(1), Some(keyed_uint),
        Some(true), Some(keyed_boolean1), Some("hello"), Some(keyed_string)),
      ScalarExperimentDataset("experiment1", "control", None, None, None, None, None, None),
      ScalarExperimentDataset("experiment1", "control", Some(5), Some(keyed_uint),
        Some(false), Some(keyed_boolean2), Some("world"), Some(keyed_string)),
      ScalarExperimentDataset("experiment1", "control", Some(5), Some(keyed_uint),
        Some(true), Some(keyed_boolean2), Some("hello"), Some(keyed_string)),
      ScalarExperimentDataset("experiment1", "control", Some(-1), Some(Map("test" -> -1)), None, None, None, None),
      ScalarExperimentDataset("experiment1", "branch1", Some(1), Some(keyed_uint),
        Some(true), Some(keyed_boolean1), Some("hello"), Some(keyed_string)),
      ScalarExperimentDataset("experiment1", "branch2", Some(1), Some(keyed_uint),
        Some(true), Some(keyed_boolean1), Some("ohai"), Some(keyed_string)),
      ScalarExperimentDataset("experiment1", "branch2", Some(1), Some(keyed_uint),
        Some(false), Some(keyed_boolean2), Some("ohai"), Some(keyed_string)),
      ScalarExperimentDataset("experiment2", "control", None, Some(keyed_uint),
        Some(true), Some(keyed_boolean1), Some("ohai"), Some(keyed_string)),
      ScalarExperimentDataset("experiment3", "control", None, Some(keyed_uint),
        Some(false), Some(keyed_boolean1), Some("ohai"), Some(keyed_string)),
      ScalarExperimentDataset("experiment3", "branch2", Some(2), Some(keyed_uint),
        Some(false), Some(keyed_boolean1), Some("orly"), Some(keyed_string))
    ).toDS().toDF()
  }

  def partialToPoint(total: Double)(label: Option[String])(v: Int): HistogramPoint = {
    HistogramPoint(v.toDouble/total, v.toLong, label)
  }

  def booleansToPoints(f: Int, t: Int): Map[Long, HistogramPoint] = Map(
    0L -> HistogramPoint(f.toDouble/(t + f), f.toDouble, Some("False")),
    1L -> HistogramPoint(t.toDouble/(t + f), t.toDouble, Some("True"))
  )

  def stringsToPoints(l: List[(Int, String, Int)]): Map[Long, HistogramPoint] = {
    val sum = l.foldLeft(0)(_ + _._3).toDouble
    l.map {
      case(i, k, v) => i.toLong -> HistogramPoint(v.toDouble / sum, v.toDouble, Some(k))
    }.toMap
  }

  "Uint Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("uint_scalar",
      UintScalar(false, "name"),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().toSet

    def toPointControl: (Int => HistogramPoint) = partialToPoint(3.0)(None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(1.0)(None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(2.0)(None)

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "uint_scalar", "UintScalar",
        Map(1L -> toPointControl(1), 5L -> toPointControl(2)),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.5),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.5),
          Statistic(None, "Mean", 3.6666666666666665),
          Statistic(None, "Median", 3.0),
          Statistic(None, "25th Percentile", 1.0),
          Statistic(None, "75th Percentile", 5.0)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "uint_scalar", "UintScalar",
        Map(1L -> toPointBranch1(1)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.5, None, None, None, None),
          Statistic(None, "Mean", 1.0),
          Statistic(None, "Median", 1.0),
          Statistic(None, "25th Percentile", 1.0),
          Statistic(None, "75th Percentile", 1.0)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "uint_scalar", "UintScalar",
        Map(1L -> toPointBranch2(2)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.5),
          Statistic(None, "Mean", 1.0),
          Statistic(None, "Median", 1.0),
          Statistic(None, "25th Percentile", 1.0),
          Statistic(None, "75th Percentile", 1.0)))))
    assert(actual == expected)
  }

  "Keyed Uint Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_uint_scalar",
      UintScalar(true, "name"),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().toSet


    def toPointControl: (Int => HistogramPoint) = partialToPoint(6.0)(None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(2.0)(None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(4.0)(None)

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointControl(3), 3L -> toPointControl(3)),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.0),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.0),
          Statistic(None, "Mean", 2.0),
          Statistic(None, "Median", 1.0),
          Statistic(None, "25th Percentile", 1.0),
          Statistic(None, "75th Percentile", 3.0)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointBranch1(1), 3L -> toPointBranch1(1)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0),
          Statistic(None, "Mean", 2.0),
          Statistic(None, "Median", 1.0),
          Statistic(None, "25th Percentile", 1.0),
          Statistic(None, "75th Percentile", 2.0)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointBranch2(2), 3L -> toPointBranch2(2)),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0),
          Statistic(None, "Mean", 2.0),
          Statistic(None, "Median", 1.0),
          Statistic(None, "25th Percentile", 1.0),
          Statistic(None, "75th Percentile", 3.0)))))
    assert(actual == expected)
  }

  "Boolean Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("boolean_scalar",
      BooleanScalar(false, "name"),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(1, 2),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.2),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.02857142857142857)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(0, 1),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.2)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(1, 1),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.02857142857142857)))))
    assert(actual == expected)
  }


  "Keyed Boolean Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_boolean_scalar",
      BooleanScalar(true, "name"),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(4, 2),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.2),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.008403361344537816)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(2, 0),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.2)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(3, 1),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.008403361344537816)))))
    assert(actual == expected)
  }

  "String Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("string_scalar",
      StringScalar(false, "name"),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 2), (2, "world", 1))),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.2),
          Statistic(Some("branch2"), "Chi-Square Distance", 1.0)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 1))),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.2)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "string_scalar", "StringScalar",
        stringsToPoints(List((1, "ohai", 2))), Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 1.0)))))
    assert(actual == expected)
  }


  "Keyed String Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_string_scalar",
      StringScalar(true, "name"),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 3), (1, "world", 3))),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.0),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.0)))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 1), (1, "world", 1))),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0)))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 2), (1, "world", 2))),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0)))))
    assert(actual == expected)
  }

  "Invalid scalars" should "be filtered out" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("uint_scalar",
      UintScalar(false, "name"),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().filter(_.experiment_branch == "control").head
    assert(actual.n == 3L)

    val keyed_analyzer = ScalarAnalyzer.getAnalyzer("keyed_uint_scalar",
      UintScalar(true, "name"),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val keyed_actual = keyed_analyzer.analyze().filter(_.experiment_branch == "control").head
    assert(keyed_actual.n == 3L)
  }

  "Confidence intervals" should "be added" in {
    import spark.implicits._

    val ciFixture: DataFrame = Seq(
      ConfidenceIntervalsDataset("experiment1", "control", Some(84)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(46)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(7)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(39)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(44)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(92)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(35)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(54)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(58)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(96)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(115)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(94)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(113)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(66)),
      ConfidenceIntervalsDataset("experiment1", "control", Some(68))
    ).toDS().toDF()

    val analyzer = ScalarAnalyzer.getAnalyzer("uint_scalar", UintScalar(false, "name"), ciFixture, true)
    val actual = analyzer.analyze()

    val expected = List(MetricAnalysis(
      "experiment1", "control", MetricAnalyzer.topLevelLabel, 15, "uint_scalar", "UintScalar",
      Map(
        115L -> HistogramPoint(0.06666666666666667, 1.0, None),
        46L -> HistogramPoint(0.06666666666666667, 1.0, None),
        84L -> HistogramPoint(0.06666666666666667, 1.0, None),
        92L -> HistogramPoint(0.06666666666666667, 1.0, None),
        96L -> HistogramPoint(0.06666666666666667, 1.0, None),
        44L -> HistogramPoint(0.06666666666666667, 1.0, None),
        54L -> HistogramPoint(0.06666666666666667, 1.0, None),
        113L -> HistogramPoint(0.06666666666666667, 1.0, None),
        7L -> HistogramPoint(0.06666666666666667, 1.0, None),
        39L -> HistogramPoint(0.06666666666666667, 1.0, None),
        66L -> HistogramPoint(0.06666666666666667, 1.0, None),
        35L -> HistogramPoint(0.06666666666666667, 1.0, None),
        58L -> HistogramPoint(0.06666666666666667, 1.0, None),
        94L -> HistogramPoint(0.06666666666666667, 1.0, None),
        68L -> HistogramPoint(0.06666666666666667, 1.0, None)),
      Some(List(
        // These CIs are in line with the numbers from the scipy.bootstrap implementation
        Statistic(None, "Mean", 67.4, Some(54.93333333333333), Some(86.46666666666667), Some(0.05), None),
        Statistic(None, "Median", 62.0, Some(41.5), Some(88.0), Some(0.05), None),
        Statistic(None, "25th Percentile", 42.75, Some(28.0), Some(58.0), Some(0.05), None),
        Statistic(None, "75th Percentile", 92.5, Some(58.0), Some(100.25), Some(0.05), None)))))

    assert(actual == expected)
  }
}
