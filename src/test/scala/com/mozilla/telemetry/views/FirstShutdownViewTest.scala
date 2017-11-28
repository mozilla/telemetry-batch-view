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

  val histogramDefs = MainSummaryView.filterHistogramDefinitions(histograms.definitions(true, nameJoiner = Histograms.prefixProcessJoiner _, includeCategorical = true))

  val userPrefs = MainSummaryView.userPrefsList

  val defaultSchema = MainSummaryView.buildSchema(userPrefs, scalarDefs, histogramDefs)

  val defaultMessageToRow = (m: Message) =>
    FirstShutdownView.messageToRow(m, userPrefs, scalarDefs, histogramDefs)

  // Apply the given schema to the given potentially-generic Row.
  def applySchema(row: Row, schema: StructType): Row = new GenericRowWithSchema(row.toSeq.toArray, schema)

  "Parent and Process histograms" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" ->
          """{
               "payload":{
                  "histograms":{
                     "PLUGINS_NOTIFICATION_SHOWN":{
                        "range":[
                           1,
                           2
                        ],
                        "histogram_type":2,
                        "values":{
                           "1":3,
                           "0":0,
                           "2":0
                        },
                        "bucket_count":3,
                        "sum":3
                     }
                  },
                  "processes":{
                     "parent":{
                        "keyedScalars":{
                           "mock.keyed.scalar.uint":{
                              "search_enter":1,
                              "search_suggestion":2
                           }
                        }
                     }
                  }
               }
            }"""),
      None)
    val summary = defaultMessageToRow(message)

    val expected = Map(
      "document_id" -> "foo",
      "plugins_notification_shown" -> 3,
      "scalar_parent_mock_keyed_scalar_uint" -> Map(
        "search_enter" -> 1,
        "search_suggestion" -> 2
      )
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

  "Info" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" ->
          """{
               "payload":{
                 "info": {
                   "subsessionLength": 14557,
                   "subsessionCounter": 12
                 }
               }
            }"""),
      None)
    val summary = defaultMessageToRow(message)

    val expected = Map(
      "document_id" -> "foo",
      "subsession_length" -> 14557l,
      "subsession_counter" -> 12
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

  "Experiments" can "be summarized" in {
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

  "Legacy addons" can "be summarized" in {
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

  "Simple measurements" can "fall back to simple measurements values" in {
    val migratedScalarsUrl = (a: String, b: String) => Source.fromFile("src/test/resources/ScalarsFromSimpleMeasures.yaml")
    val migratedScalars = new ScalarsClass {
      override protected val getURL = migratedScalarsUrl
    }
    val scalarsDef = migratedScalars.definitions(includeOptin = true).toList

    val messageSMPresent = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" ->
          """{
             "payload": {
               "simpleMeasurements": {
                 "activeTicks": 111,
                 "firstPaint": 222
               }
             }
             }"""
      ),
      None)

    val messageSummary = defaultMessageToRow(messageSMPresent)
    val appliedSummary = applySchema(messageSummary.get, defaultSchema)

    val selectedActiveTicks = appliedSummary.getAs[Int]("active_ticks")
    selectedActiveTicks should be(111)

    val selectedFirstPaint = appliedSummary.getAs[Int]("first_paint")
    selectedFirstPaint should be(222)
  }
}
