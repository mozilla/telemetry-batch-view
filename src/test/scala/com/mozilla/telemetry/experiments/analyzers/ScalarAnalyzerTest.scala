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
      UintScalar(false),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().collect().toSet

    def toPointControl: (Int => HistogramPoint) = partialToPoint(3.0)(None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(1.0)(None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(2.0)(None)

    val expected = Set(
      MetricAnalysis("experiment1", "control", "All", 3L, "uint_scalar", "UintScalar",
        Map(1L -> toPointControl(1), 5L -> toPointControl(2)), None),
      MetricAnalysis("experiment1", "branch1", "All", 1L, "uint_scalar", "UintScalar",
        Map(1L -> toPointBranch1(1)), None),
      MetricAnalysis("experiment1", "branch2", "All", 2L, "uint_scalar", "UintScalar",
        Map(1L -> toPointBranch2(2)), None)
    )
    assert(actual == expected)
  }

  "Keyed Uint Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_uint_scalar",
      UintScalar(true),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().collect().toSet


    def toPointControl: (Int => HistogramPoint) = partialToPoint(6.0)(None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(2.0)(None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(4.0)(None)

    val expected = Set(
      MetricAnalysis("experiment1", "control", "All", 3L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointControl(3), 3L -> toPointControl(3)), None),
      MetricAnalysis("experiment1", "branch1", "All", 1L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointBranch1(1), 3L -> toPointBranch1(1)), None),
      MetricAnalysis("experiment1", "branch2", "All", 2L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointBranch2(2), 3L -> toPointBranch2(2)), None)
    )
    assert(actual == expected)
  }

  "Boolean Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("boolean_scalar",
      BooleanScalar(false),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().collect().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", "All", 3L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(1, 2), None),
      MetricAnalysis("experiment1", "branch1", "All", 1L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(0, 1), None),
      MetricAnalysis("experiment1", "branch2", "All", 2L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(1, 1), None)
    )
    assert(actual == expected)
  }


  "Keyed Boolean Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_boolean_scalar",
      BooleanScalar(true),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().collect().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", "All", 3L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(4, 2), None),
      MetricAnalysis("experiment1", "branch1", "All", 1L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(2, 0), None),
      MetricAnalysis("experiment1", "branch2", "All", 2L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(3, 1), None)
    )
    assert(actual == expected)
  }

  "String Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("string_scalar",
      StringScalar(false),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().collect().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", "All", 3L, "string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 2), (2, "world", 1))), None),
      MetricAnalysis("experiment1", "branch1", "All", 1L, "string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 1))), None),
      MetricAnalysis("experiment1", "branch2", "All", 2L, "string_scalar", "StringScalar",
        stringsToPoints(List((1, "ohai", 2))), None)
    )
    assert(actual == expected)
  }


  "Keyed String Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_string_scalar",
      StringScalar(true),
      df.where(df.col("experiment_id") === "experiment1")
    )
    val actual = analyzer.analyze().collect().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", "All", 3L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 3), (1, "world", 3))), None),
      MetricAnalysis("experiment1", "branch1", "All", 1L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 1), (1, "world", 1))), None),
      MetricAnalysis("experiment1", "branch2", "All", 2L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 2), (1, "world", 2))), None)
    )
    assert(actual == expected)
  }
}
