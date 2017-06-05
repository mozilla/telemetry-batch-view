package com.mozilla.telemetry

import com.mozilla.telemetry.histograms._
import com.mozilla.telemetry.views.ExperimentAnalysisView
import com.mozilla.telemetry.experiments.analyzers.MetricAnalyzer
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

case class TestHistogram(values: Seq[Int])
case class TestLongitudinal(experiment: String,
                            branch: String,
                            flag_histogram: Seq[Boolean],
                            keyed_flag_histogram: Map[String, Seq[Boolean]],
                            boolean_histogram: Seq[Seq[Int]],
                            keyed_boolean_histogram: Map[String, Seq[Seq[Int]]],
                            count_histogram: Seq[Int],
                            keyed_count_histogram: Map[String, Seq[Int]],
                            enum_histogram: Seq[Seq[Int]],
                            keyed_enum_histogram: Map[String, Seq[Seq[Int]]],
                            linear_histogram: Seq[TestHistogram],
                            keyed_linear_histogram: Map[String, Seq[TestHistogram]],
                            exponential_histogram: Seq[TestHistogram],
                            keyed_exponential_histogram: Map[String, Seq[TestHistogram]],
                            empty_histogram: Seq[Boolean]
                           )


class ExperimentAnalysisViewTest extends FlatSpec with Matchers{
  "Histograms" can "be aggregated" in {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("AnalysisTest")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    try {
      val histogramList = Map(
        "flag_histogram" -> FlagHistogram(false),
        "keyed_flag_histogram" -> FlagHistogram(true),
        "boolean_histogram" -> BooleanHistogram(false),
        "keyed_boolean_histogram" -> BooleanHistogram(true),
        "count_histogram" -> CountHistogram(false),
        "keyed_count_histogram" -> CountHistogram(true),
        "enum_histogram" -> EnumeratedHistogram(false, 4),
        "keyed_enum_histogram" -> EnumeratedHistogram(true, 4),
        "linear_histogram" -> LinearHistogram(false, 1, 30, 3),
        "keyed_linear_histogram" -> LinearHistogram(true, 1, 30, 3),
        "exponential_histogram" -> ExponentialHistogram(false, 1, 100, 3),
        "keyed_exponential_histogram" -> ExponentialHistogram(true, 1, 100, 3),
        "empty_histogram" -> FlagHistogram(false)
      )

      val df = Seq(
        TestLongitudinal(
          "experiment1",
          "control",
          Seq(true, false),
          Map("key1" -> Seq(false, true), "key2" -> Seq(true, true)),
          Seq(Seq(2, 3), Seq(0, 0)),
          Map("key1" -> Seq(Seq(1, 1), Seq(2, 1)), "key2" -> Seq(Seq(0, 1), Seq(4, 5))),
          Seq(5, 6),
          Map("key1" -> Seq(2, 3), "key2" -> Seq(9, 8)),
          Seq(Seq(1, 2, 3, 4), Seq(2, 3, 4, 5)),
          Map("key1" -> Seq(Seq(0, 1, 0, 1), Seq(2, 1, 2, 1)), "key2" -> Seq(Seq(1, 0, 1, 0), Seq(1, 2, 1, 2))),
          Seq(TestHistogram(Seq(1, 2, 3)), TestHistogram(Seq(3, 2, 1))),
          Map("key1" -> Seq(TestHistogram(Seq(2, 3, 4)), TestHistogram(Seq(4, 3, 2)))),
          Seq(TestHistogram(Seq(1, 2, 3)), TestHistogram(Seq(3, 2, 1))),
          Map("key1" -> Seq(TestHistogram(Seq(2, 3, 4)), TestHistogram(Seq(4, 3, 2)))),
          null
        ),
        TestLongitudinal(
          "experiment1",
          "branch1",
          Seq(true, false),
          Map("key1" -> Seq(false, true), "key2" -> Seq(true, true)),
          Seq(Seq(2, 3), Seq(0, 0)),
          Map("key1" -> Seq(Seq(1, 1), Seq(2, 1)), "key2" -> Seq(Seq(0, 1), Seq(4, 5))),
          Seq(5, 6),
          Map("key1" -> Seq(2, 3), "key2" -> Seq(9, 8)),
          Seq(Seq(1, 2, 3, 4), Seq(2, 3, 4, 5)),
          Map("key1" -> Seq(Seq(0, 1, 0, 1), Seq(2, 1, 2, 1)), "key2" -> Seq(Seq(1, 0, 1, 0), Seq(1, 2, 1, 2))),
          Seq(TestHistogram(Seq(1, 2, 3)), TestHistogram(Seq(3, 2, 1))),
          Map("key1" -> Seq(TestHistogram(Seq(2, 3, 4)), TestHistogram(Seq(4, 3, 2)))),
          Seq(TestHistogram(Seq(1, 2, 3)), TestHistogram(Seq(3, 2, 1))),
          Map("key1" -> Seq(TestHistogram(Seq(2, 3, 4)), TestHistogram(Seq(4, 3, 2)))),
          null
        ),
        TestLongitudinal(
          "experiment1",
          "control",
          Seq(true, false),
          Map("key1" -> Seq(false, true), "key3" -> Seq(true, true)),
          Seq(Seq(2, 3), Seq(0, 0)),
          null,
          Seq(5, 6),
          Map("key1" -> Seq(2, 3)),
          null,
          Map("key1" -> Seq(Seq(0, 1, 0, 1), Seq(2, 1, 2, 1)), "key3" -> Seq(Seq(1, 0, 1, 0), Seq(1, 2, 1, 2))),
          Seq(TestHistogram(Seq(1, 2, 3)), TestHistogram(Seq(3, 2, 1))),
          Map("key1" -> Seq(TestHistogram(Seq(2, 3, 4)), TestHistogram(Seq(4, 3, 2)))),
          Seq(TestHistogram(Seq(1, 2, 3)), TestHistogram(Seq(3, 2, 1))),
          Map("key1" -> Seq(TestHistogram(Seq(2, 3, 4)), TestHistogram(Seq(4, 3, 2)))),
          null
        ),
        TestLongitudinal(
          "experiment1",
          "control",
          null, null, null, null, null, null, null, null, null, null, null, null, null
        )
      ).toDS().toDF()

      df.count should be(4)
      val results = histogramList.flatMap {
        case(name: String, hd: HistogramDefinition) =>
          MetricAnalyzer.getAnalyzer(
            name.toLowerCase, hd, df
          ).analyze
      }.toList

      results.length should be(24)
      results.foreach(println)
      noException should be thrownBy spark.sqlContext.createDataFrame(
        spark.sparkContext.parallelize(results), ExperimentAnalysisView.buildOutputSchema
      ).count()
    } finally {
      spark.stop
    }
  }
}
