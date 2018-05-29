package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.utils.{Events, getOrCreateSparkSession}
import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{FlatSpec, Matchers}

class EventsTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  "Events" can "be extracted" in {
    val events = parse(
      """[
           [533352, "navigation", "search", "urlbar", "enter", {"engine": "other-StartPage - English"}],
           [533352, "navigation", "search", "urlbar", "enter", {"engine": "other-StartPage - English"}, "random extra"],
           [85959, "navigation", "search", "urlbar", "enter"],
           [81994404, "navigation", "search", "searchbar"],
           [1234, "navigation", "search", "urlbar", null, {"engine": "other-StartPage - English"}],
           ["malformed"]
         ]""")

    val eventRows = Events.getEvents(events, "parent")
    val processMap = Map("telemetry_process" -> "parent")

    eventRows should be (
      List(
        Row(533352, "navigation", "search", "urlbar", "enter",
          processMap ++ Map("engine" -> "other-StartPage - English")),
        Row(85959, "navigation", "search", "urlbar", "enter", processMap),
        Row(81994404, "navigation", "search", "searchbar", null, processMap),
        Row(1234, "navigation", "search", "urlbar", null, processMap ++ Map("engine" -> "other-StartPage - English"))
      )
    )
    Events.getEvents(parse("{}") \ "events", "parent") should be (List())
    Events.getEvents(parse("""[]"""), "parent") should be (List())

    val eventMapTypeTest = parse(
      """[
           [533352, "navigation", "search", "urlbar", "enter", {
             "string": "hello world",
             "int": 0,
             "float": 1.0,
             "null": null,
             "boolean": true}
           ]
         ]""")

    val eventMapTypeTestRows = Events.getEvents(eventMapTypeTest, "parent")

    eventMapTypeTestRows should be (
      List(
        Row(533352, "navigation", "search", "urlbar", "enter", processMap ++ Map(
          "string" -> "hello world",
          "int" -> "0",
          "float" -> "1.0",
          "null" -> "null",
          "boolean" -> "true"
        ))
      )
    )

    // Apply events schema to event rows
    val schema = Events.buildEventSchema
    noException should be thrownBy spark.createDataFrame(sc.parallelize(eventRows), schema).count()
  }
}
