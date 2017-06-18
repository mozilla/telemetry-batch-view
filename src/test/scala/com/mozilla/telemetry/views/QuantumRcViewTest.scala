package com.mozilla.telemetry

import com.mozilla.telemetry.utils.UDFs._
import com.mozilla.telemetry.views.QuantumRCView
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

case class MainSummarySubmission(client_id: String,
                      app_name: String,
                      app_version: String,
                      app_build_id: String,
                      normalized_channel: String,
                      submission_date_s3: String,
                      country: String,
                      gfx_compositor: String,
                      e10s_enabled: Boolean,
                      os: String,
                      env_build_arch: String,
                      quantum_ready: Boolean,
                      gc_max_pause_ms_main_above_150: Int,
                      cycle_collector_max_pause_main_above_150: Int,
                      gc_max_pause_ms_content_above_2500: Int,
                      cycle_collector_max_pause_content_above_2500: Int,
                      input_event_response_coalesced_ms_main_above_2500: Int,
                      input_event_response_coalesced_ms_content_above_2500: Int,
                      input_event_response_coalesced_ms_main_above_250: Int,
                      input_event_response_coalesced_ms_content_above_250: Int,
                      subsession_length: Long,
                      ghost_windows_main_above_1: Int,
                      ghost_windows_content_above_1: Int,
                      experiments: Map[String, String])

object MainSummarySubmissions {
  val dimensions = Map(
    "client_id" -> List("y", "z", null),
    "app_name" -> List("Firefox", "Fennec"),
    "app_version" -> List("44.0"),
    "app_build_id" -> List("20170501000000"),
    "normalized_channel" -> List("release"),
    "submission_date_s3" -> List("20160107"),
    "country" -> List("IT", "US"),
    "gfx_compositor" -> List("d3d11", "none"),
    "e10s_enabled" -> List(false),
    "os" -> List("Windows", "Darwin"),
    "env_build_arch" -> List("x86"),
    "quantum_ready" -> List(false),
    "experiments" -> List(Map("exp1" -> "branch1"), null))

  val countValues = Map(
    "gc_max_pause_ms_main_above_150" -> List(3, 0),
    "cycle_collector_max_pause_main_above_150" -> List(3, 0),
    "gc_max_pause_ms_content_above_2500" -> List(3, 0),
    "cycle_collector_max_pause_content_above_2500" -> List(3, 0),
    "input_event_response_coalesced_ms_main_above_2500" -> List(4, 0),
    "input_event_response_coalesced_ms_content_above_2500" -> List(5, 0),
    "ghost_windows_main_above_1" -> List(0, 0),
    "ghost_windows_content_above_1" -> List(3, 0))

  val sumValues = Map(
    "input_event_response_coalesced_ms_main_above_250" -> List(2, 0),
    "input_event_response_coalesced_ms_content_above_250" -> List(1, 0),
    "subsession_length" -> List(5, 1))

  def randomList: List[MainSummarySubmission] = {
    for {
      client_id <- dimensions("client_id")
      app_name <- dimensions("app_name")
      app_version <- dimensions("app_version")
      app_build_id <- dimensions("app_build_id")
      normalized_channel <- dimensions("normalized_channel")
      submission_date_s3 <- dimensions("submission_date_s3")
      country <- dimensions("country")
      gfx_compositor <- dimensions("gfx_compositor")
      e10s_enabled <- dimensions("e10s_enabled")
      os <- dimensions("os")
      env_build_arch <- dimensions("env_build_arch")
      quantum_ready <- dimensions("quantum_ready")
      gc_max_pause_ms_main_above_150 <- countValues("gc_max_pause_ms_main_above_150")
      cycle_collector_max_pause_main_above_150 <- countValues("cycle_collector_max_pause_main_above_150")
      gc_max_pause_ms_content_above_2500 <- countValues("gc_max_pause_ms_content_above_2500")
      cycle_collector_max_pause_content_above_2500 <- countValues("cycle_collector_max_pause_content_above_2500")
      input_event_response_coalesced_ms_main_above_2500 <- countValues("input_event_response_coalesced_ms_main_above_2500")
      input_event_response_coalesced_ms_content_above_2500 <- countValues("input_event_response_coalesced_ms_content_above_2500")
      input_event_response_coalesced_ms_main_above_250 <- sumValues("input_event_response_coalesced_ms_main_above_250")
      input_event_response_coalesced_ms_content_above_250 <- sumValues("input_event_response_coalesced_ms_content_above_250")
      subsession_length <- sumValues("subsession_length")
      ghost_windows_main_above_1 <- countValues("ghost_windows_main_above_1")
      ghost_windows_content_above_1 <- countValues("ghost_windows_content_above_1")
      experiments <- dimensions("experiments")
    } yield {
      MainSummarySubmission(client_id.asInstanceOf[String],
                 app_name.asInstanceOf[String],
                 app_version.asInstanceOf[String],
                 app_build_id.asInstanceOf[String],
                 normalized_channel.asInstanceOf[String],
                 submission_date_s3.asInstanceOf[String],
                 country.asInstanceOf[String],
                 gfx_compositor.asInstanceOf[String],
                 e10s_enabled.asInstanceOf[Boolean],
                 os.asInstanceOf[String],
                 env_build_arch.asInstanceOf[String],
                 quantum_ready.asInstanceOf[Boolean],
                 gc_max_pause_ms_main_above_150.asInstanceOf[Int],
                 cycle_collector_max_pause_main_above_150.asInstanceOf[Int],
                 gc_max_pause_ms_content_above_2500.asInstanceOf[Int],
                 cycle_collector_max_pause_content_above_2500.asInstanceOf[Int],
                 input_event_response_coalesced_ms_main_above_2500.asInstanceOf[Int],
                 input_event_response_coalesced_ms_content_above_2500.asInstanceOf[Int],
                 input_event_response_coalesced_ms_main_above_250.asInstanceOf[Int],
                 input_event_response_coalesced_ms_content_above_250.asInstanceOf[Int],
                 subsession_length.asInstanceOf[Long],
                 ghost_windows_main_above_1.asInstanceOf[Int],
                 ghost_windows_content_above_1.asInstanceOf[Int],
                 experiments.asInstanceOf[Map[String, String]])
    }
  }
}

class QuantumReleaseCriteriaViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val spark = SparkSession
    .builder()
    .appName("QuantumRCViewTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  spark.registerUDFs

  private val sc = spark.sparkContext

  //setup data table
  private val tableName = "randomtablename"
  val df = sc.parallelize(MainSummarySubmissions.randomList).toDF

  // make client count dataset
  private val aggregates = QuantumRCView.aggregate(df)

  "FilteredHllMerge" can "properly count" in {
    spark.udf.register("hll_merge", FilteredHllMerge)
    val frame = sc.parallelize(List(("a", false), ("b", false), ("c", true), ("c", true)), 4).toDF("id", "allowed")
    val count = frame
      .selectExpr("hll_create(id, 12) as hll", "allowed")
      .groupBy()
      .agg(expr("hll_cardinality(hll_merge(hll, allowed)) as count"))
      .collect()
    count(0)(0) should be (1)
  }

  // aggregates need to have correct count of RC
  "Aggregates" can "have the correct schema" in {

    val dims = "total_clients" ::
      "subsession_length" ::
      "input_jank_main_over_2500" ::
      "input_jank_main_over_250" ::
      "input_jank_content_over_2500" ::
      "input_jank_content_over_250" ::
      "long_main_gc_or_cc_pause" ::
      "long_content_gc_or_cc_pause" ::
      "ghost_windows_main" ::
      "ghost_windows_content" ::
      "long_main_input_latency" ::
      "long_content_input_latency" ::
      "app_version" ::
      "app_build_id" ::
      "normalized_channel" ::
      "app_name" ::
      "os" ::
      "env_build_arch" ::
      "country" ::
      "gfx_compositor" ::
      "e10s_enabled" ::
      "quantum_ready" ::
      "experiment_id" ::
      "experiment_branch" :: Nil

    (aggregates.columns.toSet) should be (dims.toSet)
  }

  val expectedClientCount = MainSummarySubmissions.dimensions("client_id").count(x => x != null)

  "HLL Columns" can "Count the correct number of clients" in {
    val hllCols = Map(
      "total_clients" -> expectedClientCount,
      "long_main_gc_or_cc_pause" -> expectedClientCount,
      "long_content_gc_or_cc_pause" -> expectedClientCount,
      "ghost_windows_main" -> 0, // no clients have main ghost windows > 0
      "ghost_windows_content" -> expectedClientCount,
      "long_main_input_latency" -> expectedClientCount,
      "long_content_input_latency" -> expectedClientCount)

    hllCols.foreach { case (c, expectedCount) =>
      aggregates.selectExpr(s"$HllCardinality($c)").collect().foreach { e =>
        e(0) should be (expectedCount)
      }

      val count = aggregates
        .select(col(c))
        .agg(HllMerge(col(c)).as("hll"))
        .selectExpr(s"$HllCardinality(hll)").collect()

      count.length should be (1)
      count(0)(0) should be (expectedCount)
    }
  }

  "Sum columns" can "correctly sum the values" in {
    val numDimensions = MainSummarySubmissions.dimensions.filterKeys(_ != "client_id").map(_._2.length).reduce(_ * _) //48
    val numCountDimensions = MainSummarySubmissions.countValues.map(_._2.length).reduce(_ * _) //256
    val numSumDimensions = MainSummarySubmissions.sumValues.map(_._2.length).reduce(_ * _) //8

    val valueMap = MainSummarySubmissions.countValues ++ MainSummarySubmissions.sumValues

    val sumColumns: Map[String, String] = Map(
      "subsession_length" -> "subsession_length",
      "input_jank_main_over_2500" -> "input_event_response_coalesced_ms_main_above_2500",
      "input_jank_main_over_250" -> "input_event_response_coalesced_ms_main_above_250",
      "input_jank_content_over_2500" -> "input_event_response_coalesced_ms_content_above_2500",
      "input_jank_content_over_250" -> "input_event_response_coalesced_ms_content_above_250"
    )

    sumColumns.foreach {
      case (outCol, inCol) =>
        val expected = valueMap(inCol).reduce(_ + _)
        val sumColDims = valueMap(inCol).length

        aggregates.select(outCol).collect().foreach{ e =>
          e(0) should be (expected * (numSumDimensions / sumColDims) * numCountDimensions * expectedClientCount)
        }

        val totalSum = aggregates.select(sum(outCol)).collect()
        totalSum(0)(0) should be (expected * numDimensions * numCountDimensions * expectedClientCount * (numSumDimensions / sumColDims))
    }
  }

  override def afterAll() = {
    spark.stop()
  }
}
