package com.mozilla.telemetry.utils

import org.scalatest.{FlatSpec, Matchers}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.Row
import scala.io.Source

import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.views.MainSummaryView

class MainPingTest extends FlatSpec with Matchers {
 val scalarUrlMock = (a: String, b: String) => Source.fromFile("src/test/resources/Scalars.yaml")

  val scalars =  new ScalarsClass {
    override protected val getURL = scalarUrlMock
  }

  val scalarDefs = scalars.definitions(true).toList.sortBy(_._1)

  val histogramUrlMock = (a: String, b: String) => Source.fromFile("src/test/resources/ShortHistograms.json")

  val histograms = new HistogramsClass {
    override protected val getURL = histogramUrlMock
  }

  val histogramDefs = MainSummaryView.filterHistogramDefinitions(histograms.definitions(true))

  val testPayload = """
{
 "environment": {
  "addons": {
   "activeAddons": {
    "addon 1": {
      "blocklisted": false,
      "description": "First example addon.",
      "name": "Example 1",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16861,
      "updateDay": 16875,
      "isSystem": true,
      "isWebExtension": false,
      "multiprocessCompatible": true
    },
    "addon 2": {
      "blocklisted": false,
      "description": "Second example addon.",
      "name": "Example 2",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16862,
      "updateDay": 16880,
      "isSystem": false,
      "isWebExtension": true,
      "multiprocessCompatible": true
    },
    "addon 3": {
      "blocklisted": false,
      "description": "Third example addon.",
      "name": "Example 3",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16865,
      "updateDay": 16890,
      "isSystem": false,
      "isWebExtension": true,
      "multiprocessCompatible": false
    }
   },
   "activePlugins": [
    {
     "name": "Default Browser Helper",
     "version": "601",
     "description": "Provides information about the default web browser",
     "blocklisted": false,
     "disabled": false,
     "clicktoplay": true,
     "mimeTypes": ["application/apple-default-browser"],
     "updateDay": 16780
    },
    {
     "name": "Java Applet Plug-in",
     "version": "Java 8 Update 73 build 02",
     "description": "Displays Java applet content, or a placeholder if Java is not installed.",
     "blocklisted": false,
     "disabled": false,
     "clicktoplay": true,
     "mimeTypes": [
      "application/x-java-applet;jpi-version=1.8.0_73",
      "application/x-java-applet;version=1.5"
     ],
     "updateDay": 16829
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 1",
     "version": "19.0.0.226"
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 2",
     "version": "19.0.0.225"
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 3",
     "version": "9.9.9.227"
    }
   ]
  }
 },
 "payload": {
  "emptyKey": {},
  "keyedHistograms": {
   "SEARCH_COUNTS": {
    "test.urlbar": {
     "range": [1, 2],
     "bucket_count": 3,
     "histogram_type": 4,
     "values": {"0": 78, "1": 0},
     "sum": 78,
     "sum_squares_lo": 78,
     "sum_squares_hi": 0
    },
    "test.abouthome": {
     "range": [1, 2],
     "bucket_count": 3,
     "histogram_type": 4,
     "values": {"0": 10, "1": 0},
     "sum": 10,
     "sum_squares_lo": 10,
     "sum_squares_hi": 0
    }
   }
  }
 }
}
"""

  "A json object's keys" can "be counted" in {
    val json = parse(testPayload)

    val countKeys = MainPing.countKeys _
    countKeys(json \ "environment" \ "addons" \ "activeAddons").get should be (3)
    countKeys(json).get should be (2)
    countKeys(json \ "payload").get should be (2)
    countKeys(json \ "payload" \ "emptyKey").get should be (0)
    countKeys(json \ "dummy") should be (None)
  }

  "Latest flash version" can "be extracted" in {
    // Valid data
    val json = parse(testPayload)
    val getFlash = MainPing.getFlashVersion _
    getFlash(json \ "environment" \ "addons").get should be ("19.0.0.226")
    getFlash(json \ "environment") should be (None)
    getFlash(json \ "foo") should be (None)

    // Contains plugins, but not Flash:
    val json2 = parse(
      """
        |{
        | "environment": {
        |  "addons": {
        |   "activePlugins": [
        |    {
        |     "name": "Default Browser Helper",
        |     "version": "601",
        |     "description": "Provides information about the default web browser"
        |    },
        |    {
        |     "name": "Java Applet Plug-in",
        |     "version": "Java 8 Update 73 build 02",
        |     "description": "Displays Java applet content"
        |    }
        |   ]
        |  }
        | }
        |}
      """.stripMargin)
    getFlash(json2 \ "environment" \ "addons") should be (None)

    // Doesn't contain any plugins:
    val json3 = parse(
      """
        |{
        | "environment": {
        |  "addons": {
        |   "activePlugins": []
        |  }
        | }
        |}
      """.stripMargin)
    getFlash(json3 \ "environment" \ "addons") should be (None)

    // Contains many plugins, some with invalid versions
    val json4 = parse(
      """
        |{
        | "environment": {
        |  "addons": {
        |   "activePlugins": [
        |    {
        |     "name": "Shockwave Flash",
        |     "description": "Example Flash 1",
        |     "version": "19.0.0.g226"
        |    },
        |    {
        |     "name": "Shockwave Flash",
        |     "description": "Example Flash 2",
        |     "version": "19.0.0.225"
        |    },
        |    {
        |     "name": "Shockwave Flash",
        |     "description": "Example Flash 3",
        |     "version": "9.9.9.227"
        |    },
        |    {
        |     "name": "Shockwave Flash",
        |     "description": "Example Flash 4",
        |     "version": "999.x.y.227"
        |    }
        |   ]
        |  }
        | },
        | "payload": {
        |  "emptyKey": {}
        | }
        |}
      """.stripMargin)
    getFlash(json4 \ "environment" \ "addons").get should be ("19.0.0.225")
  }

  "Flash versions" can "be compared" in {
    val cmpFlash = MainPing.compareFlashVersions _
    cmpFlash(Some("1.2.3.4"), Some("1.2.3.4")).get should be (0)
    cmpFlash(Some("1.2.3.5"), Some("1.2.3.4")).get should be (1)
    cmpFlash(Some("1.2.3.4"), Some("1.2.3.5")).get should be (-1)

    // Lexically less, but numerically greater:
    cmpFlash(Some("10.2.3.5"), Some("9.3.4.8")).get should be (1)
    cmpFlash(Some("foo"), Some("1.2.3.4")).get should be (-1)
    cmpFlash(Some("1.2.3.4"), Some("foo")).get should be (1)
    cmpFlash(Some("foo"), Some("bar")) should be (None)

    // Equal but bogus values are equal (for efficiency).
    cmpFlash(Some("foo"), Some("foo")).get should be (0)

    // Something > Nothing
    cmpFlash(Some("1.2.3.5"), None).get should be (1)
    cmpFlash(None, Some("1.2.3.5")).get should be (-1)
  }

  val exampleSearches = parse("""
      |{
      |  "google.abouthome": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 1, "1": 0},
      |    "sum": 1,
      |    "sum_squares_lo": 1,
      |    "sum_squares_hi": 0
      |  },
      |  "google.urlbar": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 67, "1": 0},
      |    "sum": 67,
      |    "sum_squares_lo": 67,
      |    "sum_squares_hi": 0
      |  },
      |  "yahoo.urlbar": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 78, "1": 0},
      |    "sum": 78,
      |    "sum_squares_lo": 78,
      |    "sum_squares_hi": 0
      |  },
      |  "toast1": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 100, "1": 0},
      |    "sum": "toast",
      |    "sum_squares_lo": 100,
      |    "sum_squares_hi": 0
      |  },
      |  "toast2": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 10, "1": 0},
      |    "sum": 10,
      |    "sum_squares_lo": 10,
      |    "sum_squares_hi": 0
      |  },
      |  "toast3.badcount": {
      |    "range": [1, 2],
      |    "bucket_count": 3,
      |    "histogram_type": 4,
      |    "values": {"0": 100, "1": 0},
      |    "sum": "toast",
      |    "sum_squares_lo": 100,
      |    "sum_squares_hi": 0
      |  }
      |}
    """.stripMargin)

  "Search counts" can "be converted" in {
    var expected = 0l
    for ((k, e, s, c) <- List(
      ("google.abouthome", "google", "abouthome", 1l),
      ("google.urlbar",    "google", "urlbar",    67l),
      ("yahoo.urlbar",     "yahoo",  "urlbar",    78l),
      ("toast1",           null,     null,        null),
      ("toast2",           null,     null,        10l),
      ("toast3.badcount",  "toast3", "badcount",  null))) {
      val m = MainPing.searchHistogramToRow(k, exampleSearches \ k)
      m(0) shouldBe e
      m(1) shouldBe s
      m(2) shouldBe c
      expected = expected + (c match {
        case x: Long => x
        case _ => 0
      })
    }

    MainPing.searchHistogramToRow("toast1", exampleSearches \ "toast1") should be (Row(null, null, null))

    var actual = 0l
    for (search <- MainPing.getSearchCounts(exampleSearches).get) {

      actual = actual + (search.get(2) match {
        case x: Long => x
        case _ => 0
      })
    }
    actual should be (expected)

    val json = parse(testPayload)
    var payloadCount = 0l
    for (search <- MainPing.getSearchCounts(json \ "payload" \ "keyedHistograms" \ "SEARCH_COUNTS").get) {
      payloadCount = payloadCount + search.getLong(2)
    }
    payloadCount should be (88l)
  }

  "Histogram means" can "be computed" in {
    val example = parse("""{
      |  "H1": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 30798,
      |    "values": {
      |      "21371": 0,
      |      "28231": 1,
      |      "37292": 0
      |    }
      |  },
      |  "H2": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 30798,
      |    "values": {
      |      "21371": 0,
      |      "28231": 2,
      |      "37292": 0
      |    }
      |  },
      |  "H3": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 30798,
      |    "values": {
      |      "21371": 1,
      |      "28231": 2,
      |      "37292": 0
      |    }
      |  },
      |  "H4": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 30798,
      |    "values": {
      |      "21371": 1,
      |      "28231": 2,
      |      "37292": 1
      |    }
      |  },
      |  "H5": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 0,
      |    "values": {
      |      "21371": 1,
      |      "28231": 2,
      |      "37292": 1
      |    }
      |  },
      |  "H6": {
      |    "bucket_count": 20,
      |    "histogram_type": 0,
      |    "log_sum": 0,
      |    "log_sum_squares": 0,
      |    "range": [1000, 150000],
      |    "sum": 10,
      |    "values": {
      |      "21371": 0,
      |      "28231": 0,
      |      "37292": 0
      |    }
      |  }
      |}""".stripMargin)
    MainPing.histogramToMean(example \ "H1").get should be (30798)
    MainPing.histogramToMean(example \ "H2").get should be (15399)
    MainPing.histogramToMean(example \ "H3").get should be (10266)
    MainPing.histogramToMean(example \ "H4").get should be (7699)
    MainPing.histogramToMean(example \ "H5").get should be (0) // Sum is zero
    MainPing.histogramToMean(example \ "H6") should be (None) // bucket counts sum to zero
    MainPing.histogramToMean(example \ "H0") should be (None) // Missing
  }

  "Enum Histograms" can "be converted to Rows" in {
    val example = parse("""{
      |  "H1": {
      |    "bucket_count": 5,
      |    "histogram_type": 1,
      |    "sum": 30798,
      |    "values": {
      |      "0": 1,
      |      "1": 2,
      |      "3": 4
      |    }
      |  }
      |}""".stripMargin)
    MainPing.enumHistogramToRow(example \ "H1", (0 to 3).map(_.toString)) should be (Row(1, 2, 0, 4))
  }

  it can "be converted to a Map" in {
    val example = parse(
      """{
        |  "H1": {
        |    "bucket_count": 5,
        |    "histogram_type": 1,
        |    "sum": 30798,
        |    "values": {
        |      "0": 1,
        |      "1": 2,
        |      "3": 4,
        |      "4": 0
        |    }
        |  }
        |}""".stripMargin)
    MainPing.enumHistogramToMap(example \ "H1", (0 to 4).map(_.toString)) should be (
      Map("0" -> 1, "1" -> 2, "3" -> 4))
  }

  it can "be counted by individual buckets" in {
    val example = parse(
      """{
        |  "H1": {
        |    "bucket_count": 5,
        |    "histogram_type": 1,
        |    "sum": 30798,
        |    "values": {
        |      "0": 1,
        |      "1": 2,
        |      "3": 4,
        |      "4": 0
        |    }
        |  }
        |}""".stripMargin)
    MainPing.enumHistogramBucketCount(example \ "H1", "3") should be (Some(4))
    MainPing.enumHistogramBucketCount(example \ "H1", "42") should be (None)
    MainPing.enumHistogramBucketCount(example \ "H42", "0") should be (None)
  }

  it can "be summed over keys" in {
    val example = parse(
      """{
        |  "H1": {
        |    "bucket_count": 5,
        |    "histogram_type": 1,
        |    "sum": 30798,
        |    "values": {
        |      "0": 1,
        |      "1": 2,
        |      "3": 4,
        |      "4": 0
        |    }
        |  }
        |}""".stripMargin)
    MainPing.enumHistogramSumCounts(example \ "H1", (0 to 4).map(_.toString)) should be (7)
    MainPing.enumHistogramSumCounts(example \ "H1", IndexedSeq("3", "1")) should be (6)
  }

  "Keyed Enum Histograms" can "be converted to Maps of Rows" in {
    val example = parse("""{
      |  "H1": {
      |    "foo": {
      |      "bucket_count": 5,
      |      "histogram_type": 1,
      |      "sum": 30798,
      |      "values": {
      |        "0": 1,
      |        "2": 2
      |      }
      |    },
      |    "bar": {
      |      "bucket_count": 5,
      |      "histogram_type": 1,
      |      "sum": 30798,
      |      "values": {
      |        "0": 5,
      |        "1": 1,
      |        "2": 3
      |      }
      |    }
      |  }
      |}""".stripMargin)
    val expected = Map[String,Row](
      "foo" -> Row(1,0,2),
      "bar" -> Row(5,1,3)
    )
    MainPing.keyedEnumHistogramToMap(example \ "H1", (0 to 2).map(_.toString)).get should be (expected)
  }

  "Engagement measures" can "be extracted" in {
    def makeScalarsMap(v: JValue): Map[String, JValue] = {
      MainPing.ProcessTypes.map{ p =>
        p -> v
      } toMap
    }

    val order = List("mock.all.children", "mock.keyed.scalar.bool", "mock.keyed.scalar.string", "mock.keyed.scalar.uint", "mock.scalar.bool", "mock.scalar.string", "mock.scalar.uint", "mock.uint.optin", "mock.uint.optout")
    def makeExpected(expected: List[Any]): Row = {
      Row.fromSeq(
        scalarDefs.map(d => expected(order.indexOf(d._2.originalName)))
      )
    }

    // Doesn't have scalars
    val jNoScalars = parse(
      """{}""")
    MainPing.scalarsToRow(makeScalarsMap(jNoScalars), scalarDefs) should be (makeExpected(List(null, null, null, null, null, null, null, null, null)))

    // Has scalars, but none of the expected ones
    val jUnexpectedScalars = parse(
      """{
        |  "example1": 249,
        |  "example2": 2
        |}""".stripMargin)
    MainPing.scalarsToRow(makeScalarsMap(jUnexpectedScalars), scalarDefs) should be (makeExpected(List(null, null, null, null, null, null, null, null, null)))

    // Has scalars, and some of the expected ones
    val jSomeExpectedScalars = parse(
      """{
        |  "mock.scalar.uint": 2,
        |  "mock.uint.optin": 97
        |}""".stripMargin)
    MainPing.scalarsToRow(makeScalarsMap(jSomeExpectedScalars), scalarDefs) should be (makeExpected(List(null, null, null, null, null, null, 2, 97, null)))

    // Keyed scalars convert correctly
    val jKeyedScalar = parse(
      """{
        |  "mock.keyed.scalar.uint": {"a": 1, "b": 2}
        |}""".stripMargin)
    MainPing.scalarsToRow(makeScalarsMap(jKeyedScalar), scalarDefs) should be
      (makeExpected(List(null, null, null, Map("a" -> 1, "b" -> 2), null, null, null, null, null)))

    // Has scalars, all of the expected ones
    val jAllScalars = parse(
      """{
        |  "mock.keyed.scalar.bool": {"a": true, "b": false},
        |  "mock.keyed.scalar.string": {"a": "hello", "b": "world"},
        |  "mock.keyed.scalar.uint": {"a": 1, "b": 2},
        |  "mock.scalar.bool": false,
        |  "mock.scalar.string": "hello world",
        |  "mock.scalar.uint": 93,
        |  "mock.uint.optin": 1,
        |  "mock.uint.optout": 42
        |}""".stripMargin)

    val expected = makeExpected(List(
        null,
        Map("a" -> true, "b" -> false),
        Map("a" -> "hello", "b" -> "world"),
        Map("a" -> 1, "b" -> 2),
        false,
        "hello world",
        93,
        1,
        42
    ))

    MainPing.scalarsToRow(makeScalarsMap(jAllScalars), scalarDefs) should be (expected)

    // Has scalars with weird data
    val jWeirdScalars = parse(
      """{
        |  "mock.keyed.scalar.bool": {"a": 3, "b": "hello"},
        |  "mock.scalar.uint": false,
        |  "mock.uint.optin": [9, 7],
        |  "mock.uint.optout": "hello, world"
        |}""".stripMargin)
    MainPing.scalarsToRow(makeScalarsMap(jWeirdScalars), scalarDefs) should be (makeExpected(List(null, null, null, null, null, null, null, null, null)))

    // Has a scalars section containing unexpected data.
    val jBogusScalars = parse("""[10, "ten"]""")
    MainPing.scalarsToRow(makeScalarsMap(jBogusScalars), scalarDefs) should be (makeExpected(List(null, null, null, null, null, null, null, null, null)))
  }

  "histogram to threshold count" can "extract correct counts" in {
    val json1 = parse(
      """
        |{
        |}
      """.stripMargin)
    MainPing.histogramToThresholdCount(json1, 3) should be (0)
    MainPing.histogramToThresholdCount(json1 \ "GC_MAX_PAUSE_MS", 3) should be (0)

    val json2 = parse(
      """
        |{
        | "values": {
        |  "1": 1,
        |  "2": 10
        | }
        |}
      """.stripMargin)
    MainPing.histogramToThresholdCount(json2, 3) should be (0)

    val json3 = parse(
      """
        |{
        | "values": {
        |  "1": 1,
        |  "2": 10,
        |  "3": 10
        | }
        |}
      """.stripMargin)
    MainPing.histogramToThresholdCount(json3, 3) should be (10)

    val json4 = parse(
      """
        |{
        | "values": {
        |  "1": 1,
        |  "2": 10,
        |  "3": 10,
        |  "5": 10,
        |  "100": 10
        | }
        |}
      """.stripMargin)
    MainPing.histogramToThresholdCount(json4, 3) should be (30)
  }

  "histogram to threshold count" can "handle improper data" in {
    val json1 = parse(
      """
        |{
        |  "hello": "world"
        |}
      """.stripMargin)
    MainPing.histogramToThresholdCount(json1, 3) should be (0)
    MainPing.histogramToThresholdCount(json1 \ "GC_MAX_PAUSE_MS", 3) should be (0)

    val json2 = parse(
      """
        |{
        |  "values": "world"
        |}
      """.stripMargin)
    MainPing.histogramToThresholdCount(json2, 3) should be (0)
    MainPing.histogramToThresholdCount(json2 \ "GC_MAX_PAUSE_MS", 3) should be (0)

    val json3 = parse(
      """
        |{
        |  "values": {
        |     "a": 2
        |   }
        |}
      """.stripMargin)
    MainPing.histogramToThresholdCount(json3, 3) should be (0)
    MainPing.histogramToThresholdCount(json3 \ "GC_MAX_PAUSE_MS", 3) should be (0)
  }
}
