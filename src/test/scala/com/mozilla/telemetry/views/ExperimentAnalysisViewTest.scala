/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.experiments.analyzers.{CrashAnalyzer, EngagementAggCols, EnrollmentWindowCols, ExperimentEngagementAnalyzer, MetricAnalysis}
import com.mozilla.telemetry.experiments.statistics.StatisticalComputation
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
  it can "filter out pings with missing branches" in {
    import spark.implicits._

    val data = missingBranchData.toDS().toDF().where(col("experiment_id") === "id1")

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, spark.emptyDataset[ErrorAggRow].toDF(), viewConf,
      experimentMetrics)

    val metadata = res.filter(_.metric_name == "Experiment Metadata")
    metadata.length should be (2)

    val totalClients = metadata.flatMap(_.statistics.get.filter(_.name == "Total Clients").map(_.value)).sum
    totalClients should be (3.0)
  }

  it can "filter out pings with missing client IDs" in {
    import spark.implicits._

    val data = missingClientId.toDS().toDF().where(col("experiment_id") === "id1")

    val res = ExperimentAnalysisView.getExperimentMetrics("id1", data, spark.emptyDataset[ErrorAggRow].toDF(), viewConf,
      experimentMetrics)

    val metadata = res.filter(_.metric_name == "Experiment Metadata")
    metadata.length should be (2)

    val totalClients = metadata.flatMap(_.statistics.get.filter(_.name == "Total Clients").map(_.value)).sum
    totalClients should be (3.0)
  }


  val sampleEngagementRow = ExperimentSummaryEngagementRow(
    client_id = "client1",
    experiment_id = "experiment_1",
    experiment_branch = "control",
    submission_date_s3 = "20180101",
    total_time = 3600,
    active_ticks = 1000,
    scalar_parent_browser_engagement_total_uri_count = 20
  )

  // This one row should get thrown out the the outlier cuts.
  val outlierEngagementRow: ExperimentSummaryEngagementRow = sampleEngagementRow.copy(
    client_id = "other",
    total_time = 40000,
    active_ticks = 10000,
    scalar_parent_browser_engagement_total_uri_count = 300
  )

  "Engagement metrics" should "calculate the correct week number" in {
    import spark.implicits._

    val df = Seq(
      sampleEngagementRow.copy(submission_date_s3 = "20180101"),
      sampleEngagementRow.copy(submission_date_s3 = "20180107"),
      sampleEngagementRow.copy(submission_date_s3 = "20180108"),
      sampleEngagementRow.copy(submission_date_s3 = "20180114"),
      sampleEngagementRow.copy(submission_date_s3 = "20180115"),
      sampleEngagementRow.copy(submission_date_s3 = "20180121"),
      sampleEngagementRow.copy(submission_date_s3 = "20180122"),
      sampleEngagementRow.copy(submission_date_s3 = "20180128"),
      sampleEngagementRow.copy(submission_date_s3 = "20180129"),
      sampleEngagementRow.copy(submission_date_s3 = "20180201")
    ).toDF()

    val weekNums = df
      .select(EnrollmentWindowCols.week_number.expr)
      .collect()
      .map(_.getLong(0))
    weekNums should be (Array(1, 1, 2, 2, 3, 3, 4, 4, 5, 5))
  }

  it should "calculate reasonable median and confidence intervals" in {
    import spark.implicits._

    val rand = new scala.util.Random(0)
    def jittered(i: Int): Int = {
      val multiplier = 1.0 + (0.1 * (rand.nextDouble() - 0.5))
      (i * multiplier).toInt
    }

    val rows: Seq[ExperimentSummaryEngagementRow] =
      for {
        clientId <- (1 to 5).map(i => s"client_$i")
        date <- (0 to 9).map(i => s"2018071$i")
        _ <- 1 to 2  // ensure we have multiple pings per client to aggregate
      } yield ExperimentSummaryEngagementRow(
        client_id = clientId,
        experiment_id = "experiment_1",
        experiment_branch = "control",
        submission_date_s3 = date,
        total_time = jittered(3600),
        active_ticks = jittered(1000),
        scalar_parent_browser_engagement_total_uri_count = jittered(20)
      )

    val branchSwitcher = ExperimentSummaryEngagementRow("client_1", "experiment_1", "treatment", "20180601", 3600, 1000, 20)

    val data = (rows :+ branchSwitcher).toDF()

    val metrics = ExperimentEngagementAnalyzer.getMetrics(data, iterations = 50)
        .filter(_.experiment_branch == "control")
    metrics should have length EngagementAggCols.values.size

    val filteredDailyHours = metrics.filter(_.metric_name == "engagement_daily_hours")
    filteredDailyHours should have length 1
    val dailyHours = filteredDailyHours.head
    val stats = dailyHours.statistics.get
    stats should have length StatisticalComputation.values.size
    val medianStats = stats.filter(_.name == "Median").head
    val expectedRange = 2.0 +- 0.2
    medianStats.value should equal(expectedRange)
    medianStats.confidence_low.get should equal(expectedRange)
    medianStats.confidence_high.get should equal(expectedRange)
    medianStats.confidence_low.get should be < medianStats.value
    medianStats.confidence_high.get should be > medianStats.value

    val filteredRetentionWeek2 = metrics.filter(_.metric_name == "retained_in_week_2")
    filteredRetentionWeek2 should have length 1
    val retained = filteredRetentionWeek2.head
    val meanStats = retained.statistics.get.filter(_.name == "Mean").head
    meanStats.value should equal(0.75 +- 0.01)
    meanStats.confidence_low.get should be < meanStats.value
    meanStats.confidence_low.get should be >= 0.0
    meanStats.confidence_high.get should be > meanStats.value
    meanStats.confidence_high.get should be <= 1.0
  }

  it can "calculate reasonable retention means" in {
    import spark.implicits._

    val enrollmentRow = sampleEngagementRow.copy(submission_date_s3 = "20180101")

    val activeInWeek2 = enrollmentRow.copy(
      submission_date_s3 = "20180108",
      scalar_parent_browser_engagement_total_uri_count = 10)
    val retainedInWeek3 = enrollmentRow.copy(
      submission_date_s3 = "20180115",
      scalar_parent_browser_engagement_total_uri_count = 0)

    val data = Seq(outlierEngagementRow, enrollmentRow, activeInWeek2, retainedInWeek3).toDF()

    val metrics = ExperimentEngagementAnalyzer.getMetrics(data, iterations = 5)

    def meanValueForMetric(metricName: String): Double =
      metrics.filter(_.metric_name == metricName).head.statistics.get.filter(_.name == "Mean").head.value

    meanValueForMetric("retained_in_week_1") should be (0.0)
    meanValueForMetric("retained_in_week_2") should be (1.0)
    meanValueForMetric("retained_in_week_3") should be (1.0)
    meanValueForMetric("retained_active_in_week_1") should be (0.0)
    meanValueForMetric("retained_active_in_week_2") should be (1.0)
    meanValueForMetric("retained_active_in_week_3") should be (0.0)
  }
}
