package telemetry.test

import org.apache.avro.generic.GenericRecordBuilder
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import telemetry.heka.HekaFrame
import telemetry.streams.main_summary.{MainSummary, Utils}
import telemetry.parquet.ParquetFile

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

    val countKeys = Utils.countKeys(_)
    countKeys(json \ "environment" \ "addons" \ "activeAddons").get should be (3)
    countKeys(json).get should be (2)
    countKeys(json \ "payload").get should be (2)
    countKeys(json \ "payload" \ "emptyKey").get should be (0)
    countKeys(json \ "dummy") should be (None)
  }

  "Latest flash version" can "be extracted" in {
    // Valid data
    val json = parse(testPayload)
    val getFlash = Utils.getFlashVersion(_)
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
    val cmpFlash = Utils.compareFlashVersions(_, _)
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

  "Search counts" can "be converted" in {
    var expected = 0
    for ((k, e, s, c) <- List(
      ("google.abouthome", "google", "abouthome", 1),
      ("google.urlbar",    "google", "urlbar",    67),
      ("yahoo.urlbar",     "yahoo",  "urlbar",    78))) {
      val m = Utils.searchHistogramToMap(k, exampleSearches \ k).get
      m("engine") shouldBe e
      m("source") shouldBe s
      m("count") shouldBe c
      expected = expected + c
    }

    Utils.searchHistogramToMap("toast", exampleSearches \ "toast") should be (None)

    var actual = 0
    for (search <- Utils.getSearchCounts(exampleSearches).get) {
      actual = actual + (search("count") match {
        case x: Int => x
        case _ => -1000
      })
    }
    actual should be (expected)

    val json = parse(testPayload)
    var payloadCount = 0
    for (search <- Utils.getSearchCounts(json \ "payload" \ "keyedHistograms" \ "SEARCH_COUNTS").get) {
      payloadCount = payloadCount + (search("count") match {
        case x: Int => x
        case _ => -1000
      })
    }
    payloadCount should be (88)
  }

  "SearchCounts schema" can "be used" in {
    val ms = MainSummary("")
    val schema = ms.buildSchema
    val fieldSchema = schema.getField("search_counts").schema().getTypes().get(1).getElementType()

    val root = new GenericRecordBuilder(fieldSchema)
    root should not be (null)

    val searches = Utils.searchHistogramToMap("google.urlbar", exampleSearches \ "google.urlbar").get
    val built = ms.buildRecord(searches, fieldSchema)
    built.isEmpty should be (false)
    val b = built.get
    b.get("engine") should be ("google")
    b.get("source") should be ("urlbar")
    b.get("count") should be (67)
  }
  val testMap = Map[String, Any](
    "document_id" -> "foo",
    "submission_date" -> "20160330",
    "timestamp" -> 1000,
    "client_id" -> "hello",
    "sample_id" -> 10,
    "channel" -> "nightly",
    "normalized_channel" -> "nightly",
    "country" -> "CA",
    "city" -> "",
    "profile_creation_date" -> 16000,
    "sync_configured" -> true,
    "sync_count_desktop" -> 1,
    "sync_count_mobile" -> 1,
    "subsession_start_date" -> "2016-03-30T00:00:00",
    "subsession_length" -> 300,
    "distribution_id" -> "mozilla31",
    "e10s_enabled" -> true,
    "e10s_cohort" -> "something",
    "os" -> "Darwin",
    "os_version" -> "10",
    "os_service_pack_major" -> null,
    "os_service_pack_minor" -> null,
    "app_build_id" -> "20160330000000",
    "app_display_version" -> "47.0",
    "app_name" -> "Firefox",
    "app_version" -> "47.0a1",
    "env_build_id" -> "20160329000000",
    "env_build_version" -> "46.0a1",
    "env_build_arch" -> "victorian",
    "locale" -> "en-US",
    "active_experiment_id" -> null,
    "active_experiment_branch" -> null,
    "reason" -> "gather-payload",
    "vendor" -> "Mozilla",
    "timezone_offset" -> -180,
    "plugin_hangs" -> 0,
    "aborts_plugin" -> 0,
    "aborts_content" -> 0,
    "aborts_gmplugin" -> 0,
    "crashes_detected_plugin" -> 0,
    "crashes_detected_content" -> 0,
    "crashes_detected_gmplugin" -> 0,
    "crash_submit_attempt_main" -> 0,
    "crash_submit_attempt_content" -> 0,
    "crash_submit_attempt_plugin" -> 0,
    "crash_submit_success_main" -> 0,
    "crash_submit_success_content" -> 0,
    "crash_submit_success_plugin" -> 0,
    "active_addons_count" -> 3,
    "flash_version" -> null,
    "is_default_browser" -> true,
    "default_search_engine_data_name" -> "Google",
    "loop_activity_open_panel" -> 1,
    "loop_activity_open_conversation" -> 1,
    "loop_activity_room_open" -> 1,
    "loop_activity_room_share" -> 1,
    "loop_activity_room_delete" -> 1,
    "devtools_toolbox_opened_count" -> 3,
    "search_counts" -> Utils.getSearchCounts(exampleSearches)
  )
  "MainSummary records" can "be built" in {
    val ms = MainSummary("")
    val schema = ms.buildSchema
    val built = ms.buildRecord(testMap, schema)

    built.isEmpty should be (false)
  }

  "MainSummary records" can "be serialized" in {
    val ms = MainSummary("")
    val schema = ms.buildSchema
    val built = ms.buildRecord(testMap, schema)

    val filePath = ParquetFile.serialize(List(built.get).toIterator, schema)

    val data = ParquetFile.deserialize(filePath.toString)
    var counter = 0
    for (recovered <- data) {
      counter = counter + 1
      recovered.get("document_id") should be ("foo")
    }
    counter should be (1)
  }

  "Heka records" can "be summarized" in {
    val ms = MainSummary("")
    // Use an example framed-heka message. It is based on test_main.json.gz,
    // submitted with a URL of
    //    /submit/telemetry/foo/main/Firefox/48.0a1/nightly/20160315030230
    val hekaFileName = "/test_main.snappy.heka"
    val hekaURL = getClass.getResource(hekaFileName)
    val input = hekaURL.openStream()

    var count = 0
    for (message <- HekaFrame.parse(input, hekaFileName)) {
      message.timestamp should be (1460036116829920000l)
      message.`type`.get should be ("telemetry")
      message.logger.get should be ("telemetry")

      for (summary <- ms.messageToMap(message)) {
        summary.get("document_id").get should be ("foo")
        summary.get("app_build_id").get should be ("20160315030230")
        summary.get("devtools_toolbox_opened_count").get should be (3)
        var searchCounter = 0
        for (searches <- summary.get("search_counts")) {
          searches match {
            case Some(x: List[Map[String, Any]]) =>
              for (search <- x) {
                val someSearches = search.get("count") match {
                  case Some(x: Int) => x
                  case _ => 0
                }
                searchCounter += someSearches
              }
          }
        }
        searchCounter should be (65)

        // Make sure we can also build a record from the resulting map.
        val built = ms.buildRecord(summary, ms.buildSchema)
        built.isEmpty should be (false)
        count += 1
      }
    }
    input.close()
    count should be (1)
  }
}
