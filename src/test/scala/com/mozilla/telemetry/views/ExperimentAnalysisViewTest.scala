package com.mozilla.telemetry

import com.mozilla.telemetry.views.ExperimentAnalysisView
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll}
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

case class ExperimentSummaryRow(
  experiment_id: String,
  experiment_branch: String,
  scalar_content_browser_usage_graphite: Int,
  histogram_content_gc_max_pause_ms: Map[Int, Int])

class ExperimentAnalysisViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession =
    SparkSession.builder()
    .appName("Experiment Aggregate Test")
    .master("local[*]")
    .getOrCreate()

  val predata = Seq(
    ExperimentSummaryRow("id1", "control", 1, Map(1 -> 1)),
    ExperimentSummaryRow("id2", "branch1", 2, Map(2 -> 1)),
    ExperimentSummaryRow("id2", "control", 1, Map(2 -> 1)),
    ExperimentSummaryRow("id2", "branch1", 1, Map(2 -> 1)),
    ExperimentSummaryRow("id3", "control", 1, Map(2 -> 1))
  )

  "Child Scalars" can "be counted" in {
    import spark.implicits._

    val data = predata.toDS().toDF()
    val args =
      "--input" :: "telemetry-mock-bucket" ::
      "--output" :: "telemetry-mock-bucket" :: Nil
    val conf = new ExperimentAnalysisView.Conf(args.toArray)

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, conf).collect()
    val row = res.filter(_.getAs[String]("metric_name") == "scalar_content_browser_usage_graphite")(0)
    row.getAs[Map[Int, Row]]("histogram")(1).getDouble(0) should be (1.0)
  }

  "Child Histograms" can "be counted" in {
    import spark.implicits._

    val data = predata.toDS().toDF()
    val args =
      "--input" :: "telemetry-mock-bucket" ::
      "--output" :: "telemetry-mock-bucket" :: Nil
    val conf = new ExperimentAnalysisView.Conf(args.toArray)

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, conf).collect()
    val row = res.filter(_.getAs[String]("metric_name") == "histogram_content_gc_max_pause_ms")(0)
    row.getAs[Map[Int, Row]]("histogram")(1).getDouble(0) should be (1.0)
  }

  override def afterAll() {
    spark.stop()
  }
}
