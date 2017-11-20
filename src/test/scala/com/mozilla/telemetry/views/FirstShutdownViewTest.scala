package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{File, Message, RichMessage}
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class FirstShutdownViewTest extends FlatSpec with Matchers {
  val scalarUrlMock = (a: String, b: String) => Source.fromFile("src/test/resources/Scalars.yaml")

  val scalars = new ScalarsClass {
    override protected val getURL = scalarUrlMock
  }

  val scalarDefs = scalars.definitions(includeOptin = true).toList.sortBy(_._1)

  val histogramUrlMock = (a: String, b: String) => Source.fromFile("src/test/resources/ShortHistograms.json")

  val histograms = new HistogramsClass {
    override protected val getURL = histogramUrlMock
  }

  val histogramDefs = FirstShutdownView.filterHistogramDefinitions(histograms.definitions(true, nameJoiner = Histograms.prefixProcessJoiner _, includeCategorical = true))

  val userPrefs = FirstShutdownView.userPrefsList

  val defaultSchema = FirstShutdownView.buildSchema(userPrefs, scalarDefs, histogramDefs)

  val defaultMessageToRow = (m: Message) =>
    FirstShutdownView.messageToRow(m, userPrefs, scalarDefs, histogramDefs)

  // Apply the given schema to the given potentially-generic Row.
  def applySchema(row: Row, schema: StructType): Row = new GenericRowWithSchema(row.toSeq.toArray, schema)

  "MainSummary plugin counts" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" ->
          """{ "payload": {"histograms": {
    "PLUGINS_NOTIFICATION_SHOWN":{
      "range":[1,2],
      "histogram_type":2,
      "values":{"1":3,"0":0,"2":0},
      "bucket_count":3,
      "sum":3
    },
    "PLUGINS_NOTIFICATION_USER_ACTION":{
      "range":[1,3],
      "histogram_type":1,
      "values":{"1":0,"0":3},
      "bucket_count":4,
      "sum":0
    },
    "PLUGINS_INFOBAR_SHOWN": {
       "range": [1,2],
       "histogram_type": 2,
       "values":{"1":12,"0":0,"2":0},
       "bucket_count":3,
       "sum":12
    },
    "PLUGINS_INFOBAR_ALLOW":{
      "range":[1,2],
      "histogram_type":2,
      "values":{"1":2,"0":0,"2":0},
      "bucket_count":3,
      "sum":2
    },
    "PLUGINS_INFOBAR_BLOCK":{
      "range":[1,2],
      "histogram_type":2,
      "values":{"1":1,"0":0,"2":0},
      "bucket_count":3,
      "sum":1
    },
    "PLUGINS_INFOBAR_DISMISSED":{
      "range":[1,2],
      "histogram_type":2,
      "values":{"1":1,"0":0,"2":0},
      "bucket_count":3,
      "sum":1
    }
  }}}"""),
      None)
    val summary = defaultMessageToRow(message)

    val expected = Map(
      "document_id" -> "foo",
      "plugins_notification_shown" -> 3,
      "plugins_notification_user_action" -> Row(3, 0, 0),
      "plugins_infobar_shown" -> 12,
      "plugins_infobar_allow" -> 2,
      "plugins_infobar_block" -> 1,
      "plugins_infobar_dismissed" -> 1
    )
    val actual = applySchema(summary.get, defaultSchema)
      .getValuesMap(expected.keys.toList)
    for ((f, v) <- expected) {
      withClue(s"$f:") {
        actual.get(f) should be(Some(v))
      }
      actual.get(f) should be(Some(v))
    }
    actual should be(expected)
  }

  "MainSummary experiments" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.experiments" ->
          """{
          "experiment1": { "branch": "alpha" },
          "experiment2": { "branch": "beta" }
        }"""),
      None)
    val summary = defaultMessageToRow(message)

    val expected = Map(
      "document_id" -> "foo",
      "experiments" -> Map("experiment1" -> "alpha", "experiment2" -> "beta")
    )
    val actual = applySchema(summary.get, defaultSchema)
      .getValuesMap(expected.keys.toList)
    for ((f, v) <- expected) {
      withClue(s"$f:") {
        actual.get(f) should be(Some(v))
      }
      actual.get(f) should be(Some(v))
    }
    actual should be(expected)
  }

  "MainSummary legacy addons" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" -> """{"payload": {"addonDetails": {
          "XPI": {
            "some-disabled-addon-id": {
              "dont-care": "about-this-data",
              "we-discard-this": 11
            },
            "active-addon-id": {
              "dont-care": 12
            }
          }
        }}}""",
        "environment.addons" -> """{
          "activeAddons": {
            "active-addon-id": {
              "isSystem": false,
              "isWebExtension": true
            },
            "gom-jabbar": {
              "isSystem": false,
              "isWebExtension": true
            }
          },
          "theme": {
            "id": "firefox-compact-light@mozilla.org"
          }
        }"""),
      None)
    val summary = defaultMessageToRow(message)

    // This will make sure that:
    // - the disabled addon is in the list;
    // - active addons are filtered out.
    val expected = Map(
      "document_id" -> "foo",
      "disabled_addons_ids" -> List("some-disabled-addon-id")
    )
    val actual = applySchema(summary.get, defaultSchema)
      .getValuesMap(expected.keys.toList)
    actual should be (expected)
  }
}
