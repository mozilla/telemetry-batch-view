package com.mozilla.telemetry

import com.mozilla.telemetry.views.MainEventsView
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

case class Event(timestamp: Long,
                 category: String,
                 method: String,
                 `object`: String,
                 string_value: String,
                 map_values: Map[String, String])

case class TestMainSummary(document_id: String,
                           client_id: String,
                           normalized_channel: String,
                           country: String,
                           locale: String,
                           app_name: String,
                           app_version: String,
                           os: String,
                           os_version: String,
                           e10s_enabled: Boolean,
                           e10s_cohort: String,
                           subsession_start_date: String,
                           subsession_length: Long,
                           sync_configured: Boolean,
                           sync_count_desktop: Int,
                           sync_count_mobile: Int,
                           timestamp: Long,
                           sample_id: Long,
                           active_experiment_id: String,
                           active_experiment_branch: String,
                           events: Option[Seq[Event]])


class MainEventsViewTest extends FlatSpec with Matchers{
  "Event records" can "be extracted from MainSummary" in {
    val sparkConf = new SparkConf().setAppName("MainEventsViewTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val spark = SparkSession
      .builder()
      .appName("MainEventsViewTest")
      .getOrCreate()

    import spark.implicits._

    try {
      val e = Event(0, "navigation", "search", "urlbar", "enter", Map("engine" -> "google"))
      val m = TestMainSummary("6609b4d8-94d4-4e87-9f6f-80183079ff1b",
        "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f", "release", "US", "en-US", "Firefox", "50.1.0", "Windows_NT", "10.0",
        true, "test", "2017-01-23T20:54:10.123Z", 1000, false, 0, 0, 1485205018000000000L, 42, "test_experiment",
        "test_branch", Some(Seq(e)))

      val pings : DataFrame = Seq(
        m,
        m.copy(
          document_id = "22539231-c1c6-4b9a-bed6-2a8d2e4e5e8c",
          events = Some(Seq(
            e.copy(timestamp = 234),
            e.copy(timestamp = 345)))),
        m.copy(
          document_id = "547b5406-8717-4696-b12b-b6c796bdbf8b",
          events = None),
        m.copy(
          client_id = "baedfe78-676e-440e-98b4-a4066657ded1",
          document_id = "72062950-3daf-450e-adfd-58eda3151a97",
          sample_id = 10,
          events = Some(Seq(
            e.copy(timestamp = 123))))
      ).toDS().toDF()

      pings.count should be(4)
      val events = MainEventsView.eventsFromMain(pings, None)

      events.count should be(4)

      events.select("client_id").distinct.count should be(2)
      events.select("document_id").distinct.count should be(3)

      val sampledEvents = MainEventsView.eventsFromMain(pings, Some("10"))
      sampledEvents.count should be(1)
      sampledEvents
        .where("document_id = '72062950-3daf-450e-adfd-58eda3151a97'")
        .count should be(1)
    } finally {
      spark.stop()
    }
  }
}
