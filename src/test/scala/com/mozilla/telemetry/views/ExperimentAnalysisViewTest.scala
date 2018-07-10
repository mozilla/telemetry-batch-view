/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.experiments.analyzers.{CrashAnalyzer, ExperimentEngagementAnalyzer, MetricAnalysis}
import com.mozilla.telemetry.views.ExperimentAnalysisView
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}

case class ExperimentSummaryRow(
  client_id: String,
  experiment_id: String,
  experiment_branch: String,
  scalar_content_browser_usage_graphite: Int,
  histogram_content_gc_max_pause_ms_2: Map[Int, Int])

case class ErrorAggRow(
  experiment_id: String,
  experiment_branch: String,
  usage_hours: Double,
  subsession_count: Int,
  main_crashes: Int,
  content_crashes: Int,
  gpu_crashes: Int,
  plugin_crashes: Int,
  gmplugin_crashes: Int,
  content_shutdown_crashes: Int
)

case class ExperimentSummaryEngagementRow(
  client_id: String,
  experiment_id: String,
  experiment_branch: String,
  submission_date_s3: String,
  total_time: Int,
  active_ticks: Int,
  scalar_parent_browser_engagement_total_uri_count: Int)

// It appears a case class has to be accessible in the scope the spark session
// is created in or implicit conversions won't work
case class PermutationsRow(client_id: String)

class ExperimentAnalysisViewTest extends FlatSpec with Matchers with DataFrameSuiteBase {

  val predata = Seq(
    ExperimentSummaryRow("a", "id1", "control", 1, Map(1 -> 1)),
    ExperimentSummaryRow("b", "id2", "branch1", 2, Map(2 -> 1)),
    ExperimentSummaryRow("c", "id2", "control", 1, Map(2 -> 1)),
    ExperimentSummaryRow("d", "id2", "branch1", 1, Map(2 -> 1)),
    ExperimentSummaryRow("e", "id3", "control", 1, Map(2 -> 1))
  )

  val missingBranchData = Seq(
    ExperimentSummaryRow("a", "id1", "control", 1, Map(1 -> 1)),
    ExperimentSummaryRow("b", "id1", "control", 2, Map(2 -> 1)),
    ExperimentSummaryRow("c", "id1", "branch1", 1, Map(2 -> 1)),
    ExperimentSummaryRow("d", "id1", null, 1, Map(2 -> 1))
  )

  val missingClientId = Seq(
    ExperimentSummaryRow("a", "id1", "control", 1, Map(1 -> 1)),
    ExperimentSummaryRow("b", "id1", "control", 2, Map(2 -> 1)),
    ExperimentSummaryRow("c", "id1", "branch1", 1, Map(2 -> 1)),
    ExperimentSummaryRow(null, "id1", "branch1", 1, Map(2 -> 1))
  )

  val errorAgg = Seq(
    ErrorAggRow("id1", "control", 1, 1, 1, 0, 0, 0, 0, 0),
    ErrorAggRow("id1", "branch1", 1, 1, 2, 0, 0, 0, 0, 0),
    ErrorAggRow("id2", "control", 2, 2, 3, 3, 0, 0, 0, 0),
    ErrorAggRow("id2", "branch1", 4, 4, 3, 3, 0, 0, 0, 0)
  )

  val experimentMetrics = List(
    "scalar_content_browser_usage_graphite",
    "histogram_content_gc_max_pause_ms_2"
  )

  val viewConf = new ExperimentAnalysisView.Conf(
    Array(
      "--input", "telemetry-mock-bucket",
      "--output", "telemetry-mock-bucket",
      "--bootstrapIterations", "0"
    )
  )

  lazy val id1ExperimentMetrics: Seq[MetricAnalysis] = {
    import spark.implicits._
    val data = predata.toDS().toDF().where(col("experiment_id") === "id1")

    ExperimentAnalysisView.getExperimentMetrics("id1", data, spark.emptyDataset[ErrorAggRow].toDF(),
      viewConf, experimentMetrics)
  }

  "Child Scalars" can "be counted" in {
    val res = id1ExperimentMetrics
    val agg = res.filter(_.metric_name == "scalar_content_browser_usage_graphite").head
    agg.histogram(1).pdf should be (1.0)
  }


  "Total client ids and pings" can "be counted" in {
    val res = id1ExperimentMetrics
    val metadata = res.filter(_.metric_name == "Experiment Metadata")
    metadata.length should be (1)

    val first = metadata.head.statistics.get
    first.filter(_.name == "Total Pings").head.value should be (1.0)
    first.filter(_.name == "Total Clients").head.value should be (1.0)
  }

  "Crashes" can "be crash counted correctly" in {
    import spark.implicits._

    val multiplier = 3
    val data = List.fill(multiplier)(errorAgg).flatten.toDS().toDF()

    val results = errorAgg.map(_.experiment_id).distinct.map{ id =>
      id -> ExperimentAnalysisView.getExperimentMetrics(id, spark.emptyDataset[ExperimentSummaryRow].toDF(),
        data.filter(col("experiment_id") === id), viewConf, experimentMetrics)
    }.toMap

    errorAgg.foreach{ e =>
      val metrics = Map(
        "main_crashes" -> e.main_crashes * multiplier,
        "content_crashes" -> e.content_crashes * multiplier,
        "main_plus_content_crashes" -> (e.main_crashes + e.content_crashes) * multiplier,
        "main_crash_rate" -> e.main_crashes.toDouble / e.usage_hours,
        "content_crash_rate" -> e.content_crashes.toDouble / e.usage_hours,
        "main_plus_content_crash_rate" -> (e.main_crashes + e.content_crashes).toDouble / e.usage_hours
      )

      val rows = results(e.experiment_id).filter(r => r.experiment_branch == e.experiment_branch)
      metrics.foreach{ case (metric, value) =>
        rows.filter(r => r.metric_name == CrashAnalyzer.makeTitle(metric)).head.statistics.get.head.value should be (value)
      }
    }
  }

  "Experiment Analysis View" can "handle missing error_aggregates data" in {
    import spark.implicits._

    val df = spark.emptyDataFrame

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", spark.emptyDataset[ExperimentSummaryRow].toDF(), df,
      viewConf, experimentMetrics)
    res.size should be (0)
  }

  // Bug 1463248
  "Experiment Analysis View" can "filter out pings with missing branches" in {
    import spark.implicits._

    val data = missingBranchData.toDS().toDF().where(col("experiment_id") === "id1")

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, spark.emptyDataset[ErrorAggRow].toDF(), viewConf,
      experimentMetrics)

    val metadata = res.filter(_.metric_name == "Experiment Metadata")
    metadata.length should be (2)

    val totalClients = metadata.flatMap(_.statistics.get.filter(_.name == "Total Clients").map(_.value)).sum
    totalClients should be (3.0)
  }

  "Experiment Analysis View" can "filter out pings with missing client IDs" in {
    import spark.implicits._

    val data = missingClientId.toDS().toDF().where(col("experiment_id") === "id1")

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, spark.emptyDataset[ErrorAggRow].toDF(), viewConf,
      experimentMetrics)

    val metadata = res.filter(_.metric_name == "Experiment Metadata")
    metadata.length should be (2)

    val totalClients = metadata.flatMap(_.statistics.get.filter(_.name == "Total Clients").map(_.value)).sum
    totalClients should be (3.0)
  }

  "Engagement metrics" can "be calculated" in {
    import spark.implicits._

    val rand = new scala.util.Random(0)
    def jittered(i: Int): Int = {
      val multiplier = 1.0 + (0.1 * (rand.nextDouble() - 0.5))
      (i * multiplier).toInt
    }

    val rows: Seq[ExperimentSummaryEngagementRow] =
      for {
        clientId <- List("a", "b", "c", "d", "e")
        date <- List("20180601", "20180602", "20180603", "20180604")
        _ <- 1 to 2
      } yield ExperimentSummaryEngagementRow(
        client_id = clientId,
        experiment_id = "experiment_id",
        experiment_branch = "control",
        submission_date_s3 = date,
        total_time = jittered(3600),
        active_ticks = jittered(1000),
        scalar_parent_browser_engagement_total_uri_count = jittered(20)
      )

    val data = rows.toDF()

    val metrics = ExperimentEngagementAnalyzer.getMetrics(data, iterations = 10)
    metrics should have length 4

    val filtered = metrics.filter(_.metric_name == "engagement_daily_hours")
    filtered should have length 1
    val mth = filtered.head
    val stats = mth.statistics.get
    stats should have length 9
    val medianStats = stats.filter(_.name == "Median").head
    val expectedRange = 2.0 +- 0.1
    medianStats.value should equal(expectedRange)
    medianStats.confidence_low.get should equal(expectedRange)
    medianStats.confidence_high.get should equal(expectedRange)
    medianStats.confidence_low.get should be < medianStats.value
    medianStats.confidence_high.get should be > medianStats.value
  }
}
