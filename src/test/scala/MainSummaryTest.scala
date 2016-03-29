package telemetry.test

import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import utils.TelemetryUtils
import org.json4s.JsonDSL._

class MainSummaryTest extends FlatSpec with Matchers{
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
      "isSystem": true
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
      "isSystem": false
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
      "isSystem": false
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

    TelemetryUtils.countKeys(json \ "environment" \ "addons" \ "activeAddons").get should be (3)
    TelemetryUtils.countKeys(json).get should be (2)
    TelemetryUtils.countKeys(json \ "payload").get should be (2)
    TelemetryUtils.countKeys(json \ "payload" \ "emptyKey").get should be (0)
    TelemetryUtils.countKeys(json \ "dummy") should be (None)
  }

  "Latest flash version" can "be extracted" in {
    // Valid data
    val json = parse(testPayload)
    TelemetryUtils.getFlashVersion(json \ "environment" \ "addons").get should be ("19.0.0.226")
    TelemetryUtils.getFlashVersion(json \ "environment") should be (None)
    TelemetryUtils.getFlashVersion(json \ "foo") should be (None)

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
    TelemetryUtils.getFlashVersion(json2 \ "environment" \ "addons") should be (None)

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
    TelemetryUtils.getFlashVersion(json3 \ "environment" \ "addons") should be (None)

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
    TelemetryUtils.getFlashVersion(json4 \ "environment" \ "addons").get should be ("19.0.0.225")
  }

  "Flash versions" can "be compared" in {
    TelemetryUtils.compareFlashVersions(Some("1.2.3.4"), Some("1.2.3.4")).get should be (0)
    TelemetryUtils.compareFlashVersions(Some("1.2.3.5"), Some("1.2.3.4")).get should be (1)
    TelemetryUtils.compareFlashVersions(Some("1.2.3.4"), Some("1.2.3.5")).get should be (-1)

    // Lexically less, but numerically greater:
    TelemetryUtils.compareFlashVersions(Some("10.2.3.5"), Some("9.3.4.8")).get should be (1)
    TelemetryUtils.compareFlashVersions(Some("foo"), Some("1.2.3.4")).get should be (-1)
    TelemetryUtils.compareFlashVersions(Some("1.2.3.4"), Some("foo")).get should be (1)
    TelemetryUtils.compareFlashVersions(Some("foo"), Some("bar")) should be (None)

    // Equal but bogus values are equal (for efficiency).
    TelemetryUtils.compareFlashVersions(Some("foo"), Some("foo")).get should be (0)

    // Something > Nothing
    TelemetryUtils.compareFlashVersions(Some("1.2.3.5"), None).get should be (1)
    TelemetryUtils.compareFlashVersions(None, Some("1.2.3.5")).get should be (-1)
  }

  "Search counts" can "be converted" in {
    val exampleSearches = parse(
      """
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
        |  "toast": {
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

    var expected = 0
    for ((k, e, s, c) <- List(
      ("google.abouthome", "google", "abouthome", 1),
      ("google.urlbar",    "google", "urlbar",    67),
      ("yahoo.urlbar",     "yahoo",  "urlbar",    78))) {
      val m = TelemetryUtils.searchHistogramToMap(k, exampleSearches \ k).get
      m("engine") shouldBe e
      m("source") shouldBe s
      m("count") shouldBe c
      expected = expected + c
    }

    TelemetryUtils.searchHistogramToMap("toast", exampleSearches \ "toast") should be (None)

    var actual = 0
    for (search <- TelemetryUtils.getSearchCounts(exampleSearches).get) {
      actual = actual + (search("count") match {
        case x: Int => x
        case _ => -1000
      })
    }
    actual should be (expected)

    val json = parse(testPayload)
    var payloadCount = 0
    for (search <- TelemetryUtils.getSearchCounts(json \ "payload" \ "keyedHistograms" \ "SEARCH_COUNTS").get) {
      payloadCount = payloadCount + (search("count") match {
        case x: Int => x
        case _ => -1000
      })
    }
    payloadCount should be (88)
  }
}
