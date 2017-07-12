package com.mozilla.telemetry

import com.mozilla.telemetry.views.ExperimentAnalysisView
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfter}
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

case class ExperimentSummaryRow(
  experiment_id: String,
  experiment_branch: String,
  scalar_parent_browser_engagement_max_concurrent_tab_count: Int)

class ExperimentAnalysisViewTest extends FlatSpec with Matchers with BeforeAndAfter {
  val spark: SparkSession =
    SparkSession.builder()
    .appName("Experiment Aggregate Test")
    .master("local[*]")
    .getOrCreate()

  "Scalars" can "be counted" in {
    import spark.implicits._

    val data = Seq(
      ExperimentSummaryRow("id1", "control", 1),
      ExperimentSummaryRow("id1", "branch1", 2),
      ExperimentSummaryRow("id2", "control", 1),
      ExperimentSummaryRow("id2", "branch1", 1),
      ExperimentSummaryRow("id3", "control", 1)
    ).toDS().toDF()

    val args =
      "--input" :: "telemetry-mock-bucket" ::
      "--output" :: "telemetry-mock-bucket" :: Nil
    val conf = new ExperimentAnalysisView.Conf(args.toArray)

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, conf).collect()
    res(0).getAs[String]("metric_name") should be ("scalar_parent_browser_engagement_max_concurrent_tab_count")
  }

  after {
    spark.stop()
  }
}
