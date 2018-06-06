/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.analyzers

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.mozilla.telemetry.metrics._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.Map


case class ScalarExperimentDataset(block_id: Int,
                                   experiment_id: String,
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
      ScalarExperimentDataset(1, "experiment1", "control", Some(1), Some(keyed_uint),
        Some(true), Some(keyed_boolean1), Some("hello"), Some(keyed_string)),
      ScalarExperimentDataset(2, "experiment1", "control", None, None, None, None, None, None),
      ScalarExperimentDataset(2, "experiment1", "control", Some(5), Some(keyed_uint),
        Some(false), Some(keyed_boolean2), Some("world"), Some(keyed_string)),
      ScalarExperimentDataset(3, "experiment1", "control", Some(5), Some(keyed_uint),
        Some(true), Some(keyed_boolean2), Some("hello"), Some(keyed_string)),
      ScalarExperimentDataset(3, "experiment1", "control", Some(-1), Some(Map("test" -> -1)), None, None, None, None),
      ScalarExperimentDataset(1, "experiment1", "branch1", Some(1), Some(keyed_uint),
        Some(true), Some(keyed_boolean1), Some("hello"), Some(keyed_string)),
      ScalarExperimentDataset(1, "experiment1", "branch2", Some(1), Some(keyed_uint),
        Some(true), Some(keyed_boolean1), Some("ohai"), Some(keyed_string)),
      ScalarExperimentDataset(2, "experiment1", "branch2", Some(1), Some(keyed_uint),
        Some(false), Some(keyed_boolean2), Some("ohai"), Some(keyed_string)),
      ScalarExperimentDataset(1, "experiment2", "control", None, Some(keyed_uint),
        Some(true), Some(keyed_boolean1), Some("ohai"), Some(keyed_string)),
      ScalarExperimentDataset(1, "experiment3", "control", None, Some(keyed_uint),
        Some(false), Some(keyed_boolean1), Some("ohai"), Some(keyed_string)),
      ScalarExperimentDataset(2, "experiment3", "branch2", Some(2), Some(keyed_uint),
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
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze().toSet

    def toPointControl: (Int => HistogramPoint) = partialToPoint(3.0)(None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(1.0)(None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(2.0)(None)

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "uint_scalar", "UintScalar",
        Map(1L -> toPointControl(1), 5L -> toPointControl(2)), None),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "uint_scalar", "UintScalar",
        Map(1L -> toPointBranch1(1)), None),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "uint_scalar", "UintScalar",
        Map(1L -> toPointBranch2(2)), None))

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

    assert(actual.map(_.copy(statistics = None)) == expected)
    actual.map(_.statistics.get.map(_.name).toSet).foreach(s => assert(s == expectedStats))
  }

  "Keyed Uint Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_uint_scalar",
      UintScalar(true, "name"),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze().toSet


    def toPointControl: (Int => HistogramPoint) = partialToPoint(6.0)(None)
    def toPointBranch1: (Int => HistogramPoint) = partialToPoint(2.0)(None)
    def toPointBranch2: (Int => HistogramPoint) = partialToPoint(4.0)(None)

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointControl(3), 3L -> toPointControl(3)), None),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointBranch1(1), 3L -> toPointBranch1(1)), None),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "keyed_uint_scalar", "UintScalar",
        Map(1L -> toPointBranch2(2), 3L -> toPointBranch2(2)), None))

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

    assert(actual.map(_.copy(statistics = None)) == expected)
    actual.map(_.statistics.get.map(_.name).toSet).foreach(s => assert(s == expectedStats))
  }

  "Boolean Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("boolean_scalar",
      BooleanScalar(false, "name"),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(1, 2),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.2),
          Statistic(Some("branch1"), "Mann-Whitney-U Distance", 1.0, None, None, None, Some(0.5000000005)),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.02857142857142857),
          Statistic(Some("branch2"), "Mann-Whitney-U Distance", 2.5, None, None, None, Some(0.5000000005))))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(0, 1),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.2),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 1.0, None, None, None, Some(0.5000000005))))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "boolean_scalar", "BooleanScalar",
        booleansToPoints(1, 1),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.02857142857142857),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 2.5, None, None, None, Some(0.5000000005))))))

    assert(actual == expected)
  }


  "Keyed Boolean Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_boolean_scalar",
      BooleanScalar(true, "name"),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(4, 2),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.2),
          Statistic(Some("branch1"), "Mann-Whitney-U Distance", 4.0, None, None, None, Some(0.2541657222661252)),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.008403361344537816),
          Statistic(Some("branch2"), "Mann-Whitney-U Distance", 11.0, None, None, None, Some(0.44684730310744686))))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(2, 0),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.2),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 4.0, None, None, None, Some(0.2541657222661252))))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "keyed_boolean_scalar", "BooleanScalar",
        booleansToPoints(3, 1),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.008403361344537816),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 11.0, None, None, None, Some(0.44684730310744686))))))
    assert(actual == expected)
  }

  "String Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("string_scalar",
      StringScalar(false, "name"),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 2), (2, "world", 1))),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.2),
          Statistic(Some("branch1"), "Mann-Whitney-U Distance", 1.0, None, None, None, Some(0.5000000005)),
          Statistic(Some("branch2"), "Chi-Square Distance", 1.0),
          Statistic(Some("branch2"), "Mann-Whitney-U Distance", 2.0, None, None, None, Some(0.3804534321513065))))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 1))),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.2),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 1.0, None, None, None, Some(0.5000000005))))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "string_scalar", "StringScalar",
        stringsToPoints(List((1, "ohai", 2))), Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 1.0),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 2.0, None, None, None, Some(0.3804534321513065))))))
    assert(actual == expected)
  }


  "Keyed String Scalars" can "be aggregated" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("keyed_string_scalar",
      StringScalar(true, "name"),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze().toSet

    val expected = Set(
      MetricAnalysis("experiment1", "control", MetricAnalyzer.topLevelLabel, 3L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 3), (1, "world", 3))),
        Some(List(
          Statistic(Some("branch1"), "Chi-Square Distance", 0.0),
          Statistic(Some("branch1"), "Mann-Whitney-U Distance", 6.0, None, None, None, Some(0.42428606222331366)),
          Statistic(Some("branch2"), "Chi-Square Distance", 0.0),
          Statistic(Some("branch2"), "Mann-Whitney-U Distance", 12.0, None, None, None, Some(0.45126158423731005))))),
      MetricAnalysis("experiment1", "branch1", MetricAnalyzer.topLevelLabel, 1L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 1), (1, "world", 1))),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 6.0, None, None, None, Some(0.42428606222331366))))),
      MetricAnalysis("experiment1", "branch2", MetricAnalyzer.topLevelLabel, 2L, "keyed_string_scalar", "StringScalar",
        stringsToPoints(List((0, "hello", 2), (1, "world", 2))),
        Some(List(
          Statistic(Some("control"), "Chi-Square Distance", 0.0),
          Statistic(Some("control"), "Mann-Whitney-U Distance", 12.0, None, None, None, Some(0.45126158423731005))))))
    assert(actual == expected)
  }

  "Invalid scalars" should "be filtered out" in {
    val df = fixture
    val analyzer = ScalarAnalyzer.getAnalyzer("uint_scalar",
      UintScalar(false, "name"),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val actual = analyzer.analyze().filter(_.experiment_branch == "control").head
    assert(actual.n == 3L)

    val keyed_analyzer = ScalarAnalyzer.getAnalyzer("keyed_uint_scalar",
      UintScalar(true, "name"),
      df.where(df.col("experiment_id") === "experiment1"),
      3
    )
    val keyed_actual = keyed_analyzer.analyze().filter(_.experiment_branch == "control").head
    assert(keyed_actual.n == 3L)
  }
}
