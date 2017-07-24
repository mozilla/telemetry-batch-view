package com.mozilla.telemetry

import com.mozilla.telemetry.views.ExperimentAnalysisView
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll}

case class ExperimentSummaryRow(
  client_id: String,
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
    ExperimentSummaryRow("a", "id1", "control", 1, Map(1 -> 1)),
    ExperimentSummaryRow("b", "id2", "branch1", 2, Map(2 -> 1)),
    ExperimentSummaryRow("c", "id2", "control", 1, Map(2 -> 1)),
    ExperimentSummaryRow("d", "id2", "branch1", 1, Map(2 -> 1)),
    ExperimentSummaryRow("e", "id3", "control", 1, Map(2 -> 1))
  )

  "Child Scalars" can "be counted" in {
    import spark.implicits._

    val data = predata.toDS().toDF()
    val args =
      "--input" :: "telemetry-mock-bucket" ::
      "--output" :: "telemetry-mock-bucket" :: Nil
    val conf = new ExperimentAnalysisView.Conf(args.toArray)

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, conf).collect()
    val agg = res.filter(_.metric_name == "scalar_content_browser_usage_graphite").head
    agg.histogram(1).pdf should be (1.0)
  }

  "Child Histograms" can "be counted" in {
    import spark.implicits._

    val data = predata.toDS().toDF()
    val args =
      "--input" :: "telemetry-mock-bucket" ::
      "--output" :: "telemetry-mock-bucket" :: Nil
    val conf = new ExperimentAnalysisView.Conf(args.toArray)

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, conf).collect()
    val agg = res.filter(_.metric_name == "histogram_content_gc_max_pause_ms").head
    agg.histogram(1).pdf should be (1.0)
  }

  "Total client ids and pings" can "be counted" in {
    import spark.implicits._

    val data = predata.toDS().toDF()
    val args =
      "--input" :: "telemetry-mock-bucket" ::
      "--output" :: "telemetry-mock-bucket" :: Nil
    val conf = new ExperimentAnalysisView.Conf(args.toArray)

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, conf).collect()
    val metadata = res.filter(_.metric_name == "Experiment Metadata")
    metadata.length should be (1)

    val first = metadata.head.statistics.get
    first.filter(_.name == "Total Pings").head.value should be (1.0)
    first.filter(_.name == "Total Clients").head.value should be (1.0)
  }

  override def afterAll() {
    spark.stop()
  }
}
