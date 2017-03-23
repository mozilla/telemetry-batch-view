package com.mozilla.telemetry

import com.mozilla.telemetry.utils.Events
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{FlatSpec, Matchers}

class EventsTest extends FlatSpec with Matchers{
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

    val eventRows = Events.getEvents(events)

    eventRows should be (
      Some(List(
        Row(533352, "navigation", "search", "urlbar", "enter", Map("engine" -> "other-StartPage - English")),
        Row(85959, "navigation", "search", "urlbar", "enter", null),
        Row(81994404, "navigation", "search", "searchbar", null, null),
        Row(1234, "navigation", "search", "urlbar", null, Map("engine" -> "other-StartPage - English"))
      )
      ))
    Events.getEvents(parse("{}") \ "events") should be (None)
    Events.getEvents(parse("""[]""")) should be (None)

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

    val eventMapTypeTestRows = Events.getEvents(eventMapTypeTest)

    eventMapTypeTestRows should be (
      Some(List(
        Row(533352, "navigation", "search", "urlbar", "enter", Map(
          "string" -> "hello world",
          "int" -> "0",
          "float" -> "1.0",
          "null" -> "null",
          "boolean" -> "true"
        ))
      ))
    )

    // Apply events schema to event rows
    val schema = Events.buildEventSchema
    val sparkConf = new SparkConf().setAppName("MainSummary")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .appName("MainSummary")
      .getOrCreate()
    try {
      noException should be thrownBy spark.createDataFrame(sc.parallelize(eventRows.get), schema).count()
    } finally {
      sc.stop
    }
  }
}
