package com.mozilla.telemetry

import com.mozilla.telemetry.views.ExperimentAnalysisView
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfter}
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

case class ExperimentSummaryRow(
  experiment_id: String,
  experiment_branch: String,
  crashes_detected_content: Long)

class ExperimentAnalysisViewTest extends FlatSpec with Matchers with BeforeAndAfter {
  val spark: SparkSession =
    SparkSession.builder()
    .appName("Experiment Aggregate Test")
    .master("local[*]")
    .getOrCreate()

  "Content Crash Counts" can "be counted" in {
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
    res(0).getAs[String]("metric_name") should be ("crashes_detected_content")
  }

  after {
    spark.stop()
  }
}
