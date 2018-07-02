/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.views.MainEventsView
import org.apache.spark.sql.{DataFrame, Row}
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
                           session_id: String,
                           subsession_id: String,
                           session_start_date: String,
                           timestamp: Long,
                           sample_id: Long,
                           experiments: Map[String, String],
                           events: Option[Seq[Event]])


class MainEventsViewTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  "Event records" can "be extracted from MainSummary" in {
    sc.setLogLevel("WARN")

    import spark.implicits._

    val e = Event(0, "navigation", "search", "urlbar", "enter", Map("engine" -> "google"))
    val m = TestMainSummary("6609b4d8-94d4-4e87-9f6f-80183079ff1b",
      "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f", "release", "US", "en-US", "Firefox", "50.1.0", "Windows_NT", "10.0",
      "a2d045be-6b97-488a-8d74-a9c09427fc92", "3340d4e5-9618-4512-b612-da8ac1578d95", "2018-06-08T00:00:00.0+06:00",
      1485205018000000000L, 42, Map("experiment1" -> "branch1"), Some(Seq(e)))

    val pings: DataFrame = Seq(
      m,
      m.copy(
        document_id = "22539231-c1c6-4b9a-bed6-2a8d2e4e5e8c",
        events = Some(Seq(
          e.copy(timestamp = 234),
          e.copy(timestamp = 345, map_values = e.map_values + ("telemetry_process" -> "parent"))))),
      m.copy(
        document_id = "547b5406-8717-4696-b12b-b6c796bdbf8b",
        events = None),
      m.copy(
        client_id = "baedfe78-676e-440e-98b4-a4066657ded1",
        document_id = "72062950-3daf-450e-adfd-58eda3151a97",
        sample_id = 10,
        events = Some(Seq(
          e.copy(timestamp = 123, map_values = e.map_values + ("telemetry_process" -> "content")))))
    ).toDS().toDF()

    pings.count should be(4)
    val events = MainEventsView.eventsFromMain(pings, None)

    events.count should be(4)

    events.select("client_id").distinct.count should be(2)
    events.select("document_id").distinct.count should be(3)
    events.select("event_process").distinct.collect should contain theSameElementsAs List(
      Row(null), Row("parent"), Row("content"))

    val sampledEvents = MainEventsView.eventsFromMain(pings, Some("10"))
    sampledEvents.count should be(1)
    sampledEvents
      .where("document_id = '72062950-3daf-450e-adfd-58eda3151a97'")
      .count should be(1)

    events.select("session_start_time").collect.head.getLong(0) should be(1528394400000L)
  }
}
