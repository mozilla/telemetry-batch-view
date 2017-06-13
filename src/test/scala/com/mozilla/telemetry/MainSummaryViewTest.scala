package com.mozilla.telemetry

import com.mozilla.telemetry.heka.{File, RichMessage}
import com.mozilla.telemetry.utils.{MainPing, Events}
import com.mozilla.telemetry.views.MainSummaryView
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils.{Events, MainPing}
import com.mozilla.telemetry.views.{MainSummaryView}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import scala.io.Source

class MainSummaryViewTest extends FlatSpec with Matchers{
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

  "MainSummary records" can "be serialized" in {
    val sparkConf = new SparkConf().setAppName("MainSummary")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val spark = SparkSession
      .builder()
      .appName("MainSummary")
      .getOrCreate()

    try {
      val schema = MainSummaryView.buildSchema(scalarDefs, histogramDefs)

      // Use an example framed-heka message. It is based on test_main.json.gz,
      // submitted with a URL of
      //    /submit/telemetry/foo/main/Firefox/48.0a1/nightly/20160315030230
      for (hekaFileName <- List("/test_main_hindsight.heka", "/test_main.snappy.heka")) {
        val hekaURL = getClass.getResource(hekaFileName)
        val input = hekaURL.openStream()
        val rows = File.parse(input).flatMap(i => MainSummaryView.messageToRow(i, scalarDefs, histogramDefs))

        // Serialize this one row as Parquet
        val dataframe = spark.createDataFrame(sc.parallelize(rows.toSeq), schema)
        val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
        dataframe.write.parquet(tempFile.toString)

        // Then read it back
        val data = spark.read.parquet(tempFile.toString)

        data.count() should be (1)
        data.filter(data("document_id") === "foo").count() should be (1)
      }
    } finally {
      sc.stop()
    }
  }

  "MainSummary plugin counts" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" -> """{
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
    }
  }"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val expected = Map(
      "document_id" -> "foo",
      "plugins_notification_shown" -> 3,
      "plugins_notification_user_action" -> Row(3, 0, 0),
      "plugins_infobar_shown" -> 12,
      "plugins_infobar_allow" -> 2,
      "plugins_infobar_block" -> 1
    )
    val actual = applySchema(summary.get, MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .getValuesMap(expected.keys.toList)
    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
  }

  "MainSummary experiments" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.experiments" -> """{
          "experiment1": { "branch": "alpha" },
          "experiment2": { "branch": "beta" }
        }"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val expected = Map(
      "document_id" -> "foo",
      "experiments" -> Map("experiment1" -> "alpha", "experiment2" -> "beta")
    )
    val actual = applySchema(summary.get, MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .getValuesMap(expected.keys.toList)
    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
  }

  // Apply the given schema to the given potentially-generic Row.
  def applySchema(row: Row, schema: StructType): Row = new GenericRowWithSchema(row.toSeq.toArray, schema)

  def checkAddonValues(row: Row, schema: StructType, expected: Map[String,Any]) = {
    val actual = applySchema(row, schema).getValuesMap(expected.keys.toList)
    val aid = expected("addon_id")
    for ((f, v) <- expected) {
      withClue(s"$aid[$f]:") { actual.get(f) should be (Some(v)) }
    }
    actual should be (expected)
  }

  "Heka records" can "be summarized" in {
    // Use an example framed-heka message. It is based on test_main.json.gz,
    // submitted with a URL of
    //    /submit/telemetry/foo/main/Firefox/48.0a1/nightly/20160315030230
    for (hekaFileName <- List("/test_main_hindsight.heka", "/test_main.snappy.heka")) {
      val hekaURL = getClass.getResource(hekaFileName)
      val input = hekaURL.openStream()

      val schema = MainSummaryView.buildSchema(scalarDefs, histogramDefs)

      var count = 0
      for (message <- File.parse(input)) {
        message.timestamp should be (1460036116829920000l)
        message.`type`.get should be ("telemetry")
        message.logger.get should be ("telemetry")

        for (summary <- MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)) {
          // Apply our schema to a generic Row object
          val r = applySchema(summary, schema)

          val expected = Map(
            "document_id"                       -> "foo",
            "client_id"                         -> "c4582ba1-79fc-1f47-ae2a-671118dccd8b",
            "sample_id"                         -> 4l,
            "channel"                           -> "nightly",
            "normalized_channel"                -> "nightly",
            "country"                           -> "??",
            "city"                              -> "??",
            "os"                                -> "Darwin",
            "os_version"                        -> "15.3.0",
            "os_service_pack_major"             -> null,
            "os_service_pack_minor"             -> null,
            "windows_build_number"              -> null,
            "windows_ubr"                       -> null,
            "install_year"                      -> null,
            "is_wow64"                          -> null,
            "memory_mb"                         -> 16384,
            "profile_creation_date"             -> 16861l,
            "subsession_start_date"             -> "2016-03-28T00:00:00.0-03:00",
            "subsession_length"                 -> 14557l,
            "subsession_counter"                -> 12,
            "profile_subsession_counter"        -> 43,
            "creation_date"                     -> "2016-03-28T16:02:52.676Z",
            "distribution_id"                   -> null,
            "submission_date"                   -> "20160407",
            "sync_configured"                   -> false,
            "sync_count_desktop"                -> null,
            "sync_count_mobile"                 -> null,
            "app_build_id"                      -> "20160315030230",
            "app_display_version"               -> "48.0a1",
            "app_name"                          -> "Firefox",
            "app_version"                       -> "48.0a1",
            "timestamp"                         -> 1460036116829920000l,
            "env_build_id"                      -> "20160315030230",
            "env_build_version"                 -> "48.0a1",
            "env_build_arch"                    -> "x86-64",
            "e10s_enabled"                      -> true,
            "e10s_cohort"                       -> "unsupportedChannel",
            "locale"                            -> "en-US",
            "active_experiment_id"              -> null,
            "active_experiment_branch"          -> null,
            "reason"                            -> "gather-payload",
            "timezone_offset"                   -> -180,
            "plugin_hangs"                      -> null,
            "aborts_plugin"                     -> null,
            "aborts_content"                    -> null,
            "aborts_gmplugin"                   -> null,
            "crashes_detected_plugin"           -> null,
            "crashes_detected_content"          -> null,
            "crashes_detected_gmplugin"         -> null,
            "crash_submit_attempt_main"         -> null,
            "crash_submit_attempt_content"      -> null,
            "crash_submit_attempt_plugin"       -> null,
            "crash_submit_success_main"         -> null,
            "crash_submit_success_content"      -> null,
            "crash_submit_success_plugin"       -> null,
            "shutdown_kill"                     -> null,
            "active_addons_count"               -> 3l,
            "flash_version"                     -> null,
            "vendor"                            -> "Mozilla",
            "is_default_browser"                -> true,
            "default_search_engine_data_name"   -> "Google",
            "default_search_engine_data_load_path"      -> "jar:[app]/omni.ja!browser/google.xml",
            "default_search_engine_data_origin"         -> null,
            "default_search_engine_data_submission_url" -> "https://www.google.com/search?q=&ie=utf-8&oe=utf-8",
            "default_search_engine"             -> "google",
            "devtools_toolbox_opened_count"     -> 3,
            "client_submission_date"            -> null,
            "push_api_notify"                   -> null,
            "web_notification_shown"            -> null,
            "places_pages_count"                -> 104849,
            "places_bookmarks_count"            -> 183,
            "blocklist_enabled"                 -> true,
            "addon_compatibility_check_enabled" -> true,
            "telemetry_enabled"                 -> true,
            "user_prefs"                        -> null,
            "active_ticks"                      -> 17354,
            "main"                              -> 199,
            "first_paint"                       -> 1999,
            "session_restored"                  -> 3289,
            "total_time"                        -> 1027690,
            "plugins_notification_shown"        -> null,
            "plugins_notification_user_action"  -> null,
            "plugins_infobar_shown"             -> null,
            "plugins_infobar_block"             -> null,
            "plugins_infobar_allow"             -> null,
            "search_cohort"                     -> null,
            "gfx_compositor"                    -> "none",
            "gc_max_pause_ms_main_above_150"                        -> 0,
            "gc_max_pause_ms_content_above_2500"                    -> 0,
            "cycle_collector_max_pause_main_above_150"              -> 1416,
            "cycle_collector_max_pause_content_above_2500"          -> 0,
            "input_event_response_coalesced_ms_main_above_250"      -> 0,
            "input_event_response_coalesced_ms_main_above_2500"     -> 0,
            "input_event_response_coalesced_ms_content_above_250"   -> 0,
            "input_event_response_coalesced_ms_content_above_2500"  -> 0,
            "ghost_windows_main_above_1"                            -> 0,
            "ghost_windows_content_above_1"                         -> 0,
            "scalar_parent_mock_keyed_scalar_bool"   -> null,
            "scalar_parent_mock_keyed_scalar_string" -> null,
            "scalar_parent_mock_keyed_scalar_uint"   -> null,
            "scalar_parent_mock_scalar_bool"         -> null,
            "scalar_parent_mock_scalar_string"       -> null,
            "scalar_parent_mock_scalar_uint"         -> null,
            "scalar_parent_mock_uint_optin"          -> null,
            "scalar_parent_mock_uint_optout"         -> null,
            "experiments"                            -> null
          )

          val actual = r.getValuesMap(expected.keys.toList)
          for ((f, v) <- expected) {
            withClue(s"$f:") { actual.get(f) should be (Some(v)) }
            actual.get(f) should be (Some(v))
          }
          actual should be (expected)

          val searchSchema = MainSummaryView.buildSearchSchema
          val searches = r.getSeq[Row](r.fieldIndex("search_counts"))
          val searchCounter = searches.map(search => {
            val sW = applySchema(search, searchSchema)
            sW.getLong(sW.fieldIndex("count"))
          }).sum
          searchCounter should be (65l)

          r.getStruct(r.fieldIndex("loop_activity_counter")) should be (null)
          val popup = r.getMap[String,Row](r.fieldIndex("popup_notification_stats"))
          val expectedPopup = Map[String,Row](
            "(all)"             -> Row(8,2,0,0,0,1,0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0),
            "geolocation"       -> Row(1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0),
            "password"          -> Row(5,0,0,0,0,1,0,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0),
            "web-notifications" -> Row(2,1,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
          popup should be (expectedPopup)

          val addonSchema = MainSummaryView.buildAddonSchema
          checkAddonValues(r.getStruct(r.fieldIndex("active_theme")), addonSchema, Map(
            "addon_id"              -> "{972ce4c6-7e08-4474-a285-3208198ce6fd}",
            "blocklisted"           -> false,
            "name"                  -> "Default",
            "user_disabled"         -> false,
            "app_disabled"          -> false,
            "version"               -> "48.0a1",
            "scope"                 -> 4,
            "type"                  -> null,
            "foreign_install"       -> false,
            "has_binary_components" -> false,
            "install_day"           -> 16861,
            "update_day"            -> 16875,
            "signed_state"          -> null,
            "is_system"             -> null,
            "is_web_extension"      -> null,
            "multiprocess_compatible" -> null
          ))

          val addons = r.getSeq[Row](r.fieldIndex("active_addons"))
          addons.size should be (3)

          for (addon <- addons) {
            val a = applySchema(addon, addonSchema)
            val addonId = a.getString(a.fieldIndex("addon_id"))
            addonId match {
              case "e10srollout@mozilla.org" => checkAddonValues(addon, addonSchema, Map(
                "addon_id"              -> "e10srollout@mozilla.org",
                "blocklisted"           -> false,
                "name"                  -> "Multi-process staged rollout",
                "user_disabled"         -> false,
                "app_disabled"          -> false,
                "version"               -> "1.0",
                "scope"                 -> 1,
                "type"                  -> "extension",
                "foreign_install"       -> false,
                "has_binary_components" -> false,
                "install_day"           -> 16865,
                "update_day"            -> 16875,
                "signed_state"          -> null,
                "is_system"             -> true,
                "is_web_extension"      -> null,
                "multiprocess_compatible" -> null
              ))
              case "firefox@getpocket.com" => checkAddonValues(addon, addonSchema, Map(
                "addon_id"              -> "firefox@getpocket.com",
                "blocklisted"           -> false,
                "name"                  -> "Pocket",
                "user_disabled"         -> false,
                "app_disabled"          -> false,
                "version"               -> "1.0",
                "scope"                 -> 1,
                "type"                  -> "extension",
                "foreign_install"       -> false,
                "has_binary_components" -> false,
                "install_day"           -> 16861,
                "update_day"            -> 16875,
                "signed_state"          -> null,
                "is_system"             -> true,
                "is_web_extension"      -> null,
                "multiprocess_compatible" -> null
              ))
              case "loop@mozilla.org" => checkAddonValues(addon, addonSchema, Map(
                "addon_id"              -> "loop@mozilla.org",
                "blocklisted"           -> false,
                "name"                  -> "Firefox Hello Beta",
                "user_disabled"         -> false,
                "app_disabled"          -> false,
                "version"               -> "1.1.12",
                "scope"                 -> 1,
                "type"                  -> "extension",
                "foreign_install"       -> false,
                "has_binary_components" -> false,
                "install_day"           -> 16861,
                "update_day"            -> 16875,
                "signed_state"          -> null,
                "is_system"             -> true,
                "is_web_extension"      -> null,
                "multiprocess_compatible" -> null
              ))
              case x => x should be ("Should not have happened")
            }
          }
          count += 1
        }
      }
      input.close()
      count should be (1)
    }
  }

  "Job parameters" can "conform to expected values" in {
    MainSummaryView.jobName should be ("main_summary")
    val versionPattern = "^v[0-9]+$".r
    (versionPattern findAllIn MainSummaryView.schemaVersion).mkString("Oops") should be (MainSummaryView.schemaVersion)
  }

  "User prefs" can "be extracted" in {
    // Contains prefs, but not dom.ipc.processCount or extensions.allow-non-mpc-extensions:
    val json1 = parse(
      """
        |{
        | "environment": {
        |  "settings": {
        |   "userPrefs": {
        |    "browser.cache.disk.capacity": 358400,
        |    "browser.newtabpage.enhanced": true,
        |    "browser.startup.page": 3
        |   }
        |  }
        | }
        |}
      """.stripMargin)
    MainSummaryView.getUserPrefs(json1 \ "environment" \ "settings" \ "userPrefs") should be (None)

    // Doesn't contain any prefs:
    val json2 = parse(
      """
        |{
        | "environment": {
        |  "settings": {
        |   "userPrefs": {}
        |  }
        | }
        |}
      """.stripMargin)
    MainSummaryView.getUserPrefs(json2 \ "environment" \ "settings" \ "userPrefs") should be (None)

    // Contains prefs, including dom.ipc.processCount and extensions.allow-non-mpc-extensions
    val json3 = parse(
      """
        |{
        | "environment": {
        |  "settings": {
        |   "userPrefs": {
        |    "dom.ipc.processCount": 2,
        |    "browser.newtabpage.enhanced": true,
        |    "browser.startup.page": 3,
        |    "extensions.allow-non-mpc-extensions": true
        |   }
        |  }
        | }
        |}
      """.stripMargin)
    MainSummaryView.getUserPrefs(json3 \ "environment" \ "settings" \ "userPrefs") should be (Some(Row(2, true)))

    // Contains dom.ipc.processCount and extensions.allow-non-mpc-extensions with bogus data types
    val json4 = parse(
      """
        |{
        | "environment": {
        |  "settings": {
        |   "userPrefs": {
        |    "dom.ipc.processCount": "2",
        |    "browser.newtabpage.enhanced": true,
        |    "browser.startup.page": 3,
        |    "extensions.allow-non-mpc-extensions": 1
        |   }
        |  }
        | }
        |}
      """.stripMargin)
    MainSummaryView.getUserPrefs(json4 \ "environment" \ "settings" \ "userPrefs") should be (None)

    // Missing the prefs section entirely:
    val json5 = parse(
      """
        |{
        | "environment": {
        |  "settings": {
        |  }
        | }
        |}
      """.stripMargin)
    MainSummaryView.getUserPrefs(json5 \ "environment" \ "settings" \ "userPrefs") should be (None)

  // Contains dom.ipc.processCount but not extensions.allow-non-mpc-extensions
  val json6 = parse(
    """
      |{
      | "environment": {
      |  "settings": {
      |   "userPrefs": {
      |    "dom.ipc.processCount": 4,
      |    "browser.newtabpage.enhanced": true,
      |    "browser.startup.page": 3
      |   }
      |  }
      | }
      |}
    """.stripMargin)
  MainSummaryView.getUserPrefs(json6 \ "environment" \ "settings" \ "userPrefs") should be (Some(Row(4, null)))

    // Contains extensions.allow-non-mpc-extensions but not dom.ipc.processCount
    val json7 = parse(
      """
        |{
        | "environment": {
        |  "settings": {
        |   "userPrefs": {
        |    "browser.newtabpage.enhanced": true,
        |    "browser.startup.page": 3,
        |    "extensions.allow-non-mpc-extensions": false
        |   }
        |  }
        | }
        |}
      """.stripMargin)
    MainSummaryView.getUserPrefs(json7 \ "environment" \ "settings" \ "userPrefs") should be (Some(Row(null, false)))

}

  "Engagement measures" can "be extracted" in {
    // Doesn't have scalars
    val jNoScalars = parse(
      """{}""")
    MainPing.scalarsToRow(jNoScalars, scalarDefs) should be (Row(null, null, null, null, null, null, null, null))

    // Has scalars, but none of the expected ones
    val jUnexpectedScalars = parse(
      """{
        |  "example1": 249,
        |  "example2": 2
        |}""".stripMargin)
    MainPing.scalarsToRow(jUnexpectedScalars, scalarDefs) should be (Row(null, null, null, null, null, null, null, null))

    // Has scalars, and some of the expected ones
    val jSomeExpectedScalars = parse(
      """{
        |  "mock.scalar.uint": 2,
        |  "mock.uint.optin": 97
        |}""".stripMargin)
    MainPing.scalarsToRow(jSomeExpectedScalars, scalarDefs) should be (Row(null, null, null, null, null, 2, 97, null))

    // Keyed scalars convert correctly
    val jKeyedScalar = parse(
      """{
        |  "mock.keyed.scalar.uint": {"a": 1, "b": 2}
        |}""".stripMargin)
    MainPing.scalarsToRow(jKeyedScalar, scalarDefs) should be (Row(null, null, Map("a" -> 1, "b" -> 2), null, null, null, null, null))


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

    val expected = Row(
        Map("a" -> true, "b" -> false),
        Map("a" -> "hello", "b" -> "world"),
        Map("a" -> 1, "b" -> 2),
        false,
        "hello world",
        93,
        1,
        42
    )

    MainPing.scalarsToRow(jAllScalars, scalarDefs) should be (expected)

    // Has scalars with weird data
    val jWeirdScalars = parse(
      """{
        |  "mock.keyed.scalar.bool": {"a": 3, "b": "hello"},
        |  "mock.scalar.uint": false,
        |  "mock.uint.optin": [9, 7],
        |  "mock.uint.optout": "hello, world"
        |}""".stripMargin)
    MainPing.scalarsToRow(jWeirdScalars, scalarDefs) should be (Row(null, null, null, null, null, null, null, null))

    // Has a scalars section containing unexpected data.
    val jBogusScalars = parse("""[10, "ten"]""")
    MainPing.scalarsToRow(jBogusScalars, scalarDefs) should be (Row(null, null, null, null, null, null, null, null))
  }

  "Keyed Scalars" can "be properly shown" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" -> """{
  "payload": {
    "processes": {
      "parent": {
        "keyedScalars": {
          "mock.keyed.scalar.uint": {
            "search_enter": 1,
            "search_suggestion": 2
          }
        }
      }
    }
  }
}"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val expected = Map(
      "scalar_parent_mock_keyed_scalar_uint" -> Map(
        "search_enter" -> 1,
        "search_suggestion" -> 2
      )
    )

    val actual =
      applySchema(summary.get, MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .getValuesMap(expected.keys.toList)

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
  }

  "Stub attribution" can "be extracted" in {
    // Contains a single attribute
    val json1 = parse(
      """
        |{
        | "environment": {
        |  "settings": {
        |   "attribution": {
        |     "source": "sample_source"
        |   }
        |  }
        | }
        |}
      """.stripMargin)
    MainSummaryView.getAttribution(json1 \ "environment" \ "settings" \ "attribution") should be (
      Some(Row("sample_source", null, null, null)))

    // Contains no attributes
    val json2 = parse(
      """
        |{
        | "environment": {
        |  "settings": {}
        | }
        |}
      """.stripMargin)
    MainSummaryView.getAttribution(json2 \ "environment" \ "settings" \ "attribution") should be (None)

    // Contains all attributes, in no particular order
    val json3 = parse(
      """
        |{
        | "environment": {
        |  "settings": {
        |   "attribution": {
        |     "content": "sample_content",
        |     "source": "sample_source",
        |     "medium": "sample_medium",
        |     "campaign": "sample_campaign"
        |   }
        |  }
        | }
        |}
      """.stripMargin)
    MainSummaryView.getAttribution(json3 \ "environment" \ "settings" \ "attribution") should be (
      Some(Row("sample_source", "sample_medium", "sample_campaign", "sample_content")))
  }

  "Search cohort" can "be properly shown" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.settings" -> """{
          "searchCohort": "helloworld"
        }"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val expected = Map(
      "search_cohort" -> "helloworld"
    )

    val actual =
      applySchema(summary.get, MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .getValuesMap(expected.keys.toList)

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
  }

  "User prefs" can "be properly shown" in {
    val spark = SparkSession.builder()
        .appName("Generic Longitudinal Test")
        .master("local[1]")
        .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.settings" -> """{
          "userPrefs": {
            "dom.ipc.processCount": 2
          }
        }"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val expected = Map(
      "dom_ipc_process_count" -> 2,
      "extensions_allow_non_mpc_extensions" -> null
    )

    val actual =
      spark
      .createDataFrame(sc.parallelize(List(summary.get)), MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .first
      .getAs[Row]("user_prefs")
      .getValuesMap(expected.keys.toList)

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
    spark.stop()
  }

  "User prefs" can "handle null" in {
    val spark = SparkSession.builder()
        .appName("Generic Longitudinal Test")
        .master("local[1]")
        .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.settings" -> """{
          "userPrefs": {
            "extensions.allow-non-mpc-extensions": true
          }
        }"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val expected = Map(
      "dom_ipc_process_count" -> null,
      "extensions_allow_non_mpc_extensions" -> true 
    )

    val actual =
      spark
      .createDataFrame(sc.parallelize(List(summary.get)), MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .first
      .getAs[Row]("user_prefs")
      .getValuesMap(expected.keys.toList)

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
    spark.stop()
  }

  "Histograms" can "be stored" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" -> """{
    "MOCK_EXPONENTIAL_OPTOUT": {
        "range": [1,100],
        "bucket_count": 10,
        "histogram_type": 0,
        "values": {
          "1": 0,
          "16": 1,
          "54": 1
        },
        "sum": 64
      },
    "MOCK_OPTOUT": {
      "range": [1,10],
      "bucket_count": 10,
      "histogram_type": 2,
      "values": {
        "1": 0,
        "3": 1,
        "9": 1
      },
      "sum": 12
    }
  }"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val mock_exp_vals = Map(
      1 -> 0,
      16 -> 1,
      54 -> 1
    )

    val mock_lin_vals = Map(
      1 -> 0,
      3 -> 1,
      9 -> 1
    )

    val expected = Map(
      "histogram_parent_mock_exponential_optout" -> mock_exp_vals,
      "histogram_parent_mock_optout" -> mock_lin_vals
    )

    val actual = applySchema(summary.get, MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .getValuesMap(expected.keys.toList)

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
  }

  "Keyed Histograms" can "be stored" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.keyedHistograms" -> """
{
  "MOCK_KEYED_LINEAR": {
    "hello": {
      "range": [1,10],
      "bucket_count": 10,
      "histogram_type": 0,
      "values": {
        "1": 0,
        "3": 1,
        "9": 1
      },
      "sum": 12
    },
    "world": {
      "range": [1,10],
      "bucket_count": 10,
      "histogram_type": 0,
      "values": {
        "1": 0,
        "2": 1
      },
      "sum": 2
    }
  },
  "MOCK_KEYED_EXPONENTIAL": {
    "foo": {
      "range": [1,100],
      "bucket_count": 10,
      "histogram_type": 0,
      "values": {
        "1": 0,
        "16": 1,
        "54": 1
      },
      "sum": 64
    },
    "42": {
      "range": [1,100],
      "bucket_count": 10,
      "histogram_type": 0,
      "values": {
        "1": 1
      },
      "sum": 0
    }
  }
}"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val mock_lin_vals = Map(
      "hello" -> Map(1->0, 3->1, 9->1),
      "world" -> Map(1->0, 2->1)
    )

    val mock_exp_vals = Map(
      "foo" -> Map(1->0, 16->1, 54->1),
      "42" -> Map(1->1)
    )

    val expected = Map(
      "histogram_parent_mock_keyed_linear" -> mock_lin_vals,
      "histogram_parent_mock_keyed_exponential" -> mock_exp_vals
    )

    val actual = applySchema(summary.get, MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .getValuesMap(expected.keys.toList)

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
  }

  "Bad histograms" can "be handled" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.keyedHistograms" -> """
{
  "MOCK_KEYED_LINEAR": {
    "hello": {
      "theempiredidnothingwrong": true,
      "histogram_type": 0,
      "valuess": {
        "1": 0,
        "3": 1,
        "9": 1
      },
      "summ": 12
    }
  },
  "NONCURRENT_KEYED_HISTOGRAM": {
    "foo": {
      "range": [1,100],
      "bucket_count": 10,
      "histogram_type": 0,
      "values": {
        "1": 0,
        "16": 1,
        "54": 1
      },
      "sum": 64
    },
    "42": {
      "range": [1,100],
      "bucket_count": 10,
      "histogram_type": 0,
      "values": {
        "1": 1
      },
      "sum": 0
    }
  }
}"""),
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val expected = Map(
      "histogram_parent_mock_keyed_linear" -> Map("hello" -> null)
    )

    val actualWithSchema = applySchema(summary.get, MainSummaryView.buildSchema(scalarDefs, histogramDefs))
    val actual = actualWithSchema.getValuesMap(expected.keys.toList)

    intercept[IllegalArgumentException] {
      actualWithSchema.fieldIndex("noncurrent_histogram")
    }

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }
    actual should be (expected)
  }

  "All possible histograms" can "be included" in {
    val spark = SparkSession.builder()
       .appName("")
       .master("local[1]")
       .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val allHistogramDefs = MainSummaryView.filterHistogramDefinitions(Histograms.definitions(includeOptin = false), useWhitelist = true)

    val fakeHisto = """{
          "sum": 100,
          "values": {"1": 0, "2": 10, "40": 100, "50": 1000, "100": 1002},
          "bucketCount": 100,
          "range": [0, 100],
          "histogramType": 2
        }"""

    val histosData = allHistogramDefs.map{
      case (name, definition) =>
        definition match {
          case LinearHistogram(keyed, _, _, _) => (name, keyed)
          case ExponentialHistogram(keyed, _, _, _) => (name, keyed)
          case EnumeratedHistogram(keyed, _) => (name, keyed)
          case BooleanHistogram(keyed) => (name, keyed)
          case other =>
            throw new UnsupportedOperationException(s"${other.toString()} histogram types are not supported")
        }
    }.filter(!_._2).map{
      case (name, _) => s""""$name": $fakeHisto"""
    }.mkString(",")

    val keyedHistosData = allHistogramDefs.map{
      case (name, definition) =>
        definition match {
          case LinearHistogram(keyed, _, _, _) => (name, keyed)
          case ExponentialHistogram(keyed, _, _, _) => (name, keyed)
          case EnumeratedHistogram(keyed, _) => (name, keyed)
          case BooleanHistogram(keyed) => (name, keyed)
          case other =>
            throw new UnsupportedOperationException(s"${other.toString()} histogram types are not supported")
        }
    }.filter(_._2).map{
      case (name, _) =>
        s"""
        "$name": {
          "key1": $fakeHisto,
          "key2": $fakeHisto,
          "key3": $fakeHisto
        }"""
    }.mkString(",")

    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" -> s"{$histosData}",
        "payload.keyedHistograms" -> s"{$keyedHistosData}"
      ),
      None);

    val expected = MainPing
      .histogramsToRow(parse(s"{$histosData}") merge parse(s"{$keyedHistosData}"), allHistogramDefs)
      .toSeq.zip(allHistogramDefs.map(e => MainSummaryView.getHistogramName(e._1, "parent")).toList)
      .map(_.swap).toMap

    val summary = MainSummaryView.messageToRow(message, scalarDefs, allHistogramDefs)

    val actual = spark
          .createDataFrame(sc.parallelize(List(summary.get)), MainSummaryView.buildSchema(scalarDefs, allHistogramDefs))
          .first
          .getValuesMap(expected.keys.toList)

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
    }
    actual should be (expected)

    spark.stop()
  }

  "Histogram filter" can "include all whitelisted histograms" in {
    val allHistogramDefs = MainSummaryView.filterHistogramDefinitions(
      Histograms.definitions(includeOptin = true),
      useWhitelist = true
    ).map(_._1).toSet

    val expectedDefs = MainSummaryView.histogramsWhitelist.toSet

    allHistogramDefs should be (expectedDefs)
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

  "Quantum Ready" should "be correct for a ping" in {
    val json0e10s = parse("true")
    val json0addons = parse("""
      {
        "addon1": {
          "isSystem": true,
          "isWebExtension": false
        },
        "addon2": {
          "isSystem": false,
          "isWebExtension": true
        }
      }"""
    )

    val json0theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json0e10s, json0addons, json0theme) should be (Some(true))

    // not quantum ready with no e10s
    val json1e10s = parse("false")
    val json1addons = parse("""
    | {
    |   "addon1": {
    |     "isSystem": true,
    |     "isWebExtension": true
    |   },
    |   "addon2": {
    |     "isSystem": true,
    |     "isWebExtension": true
    |   }
    | }""".stripMargin
    )

    val json1theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json1e10s, json1addons, json1theme) should be (Some(false))

    // not quantum ready with non-system and non-webextension addon
    val json2e10s = parse("true")
    val json2addons = parse("""
    | {
    |   "addon1": {
    |      "isSystem": true,
    |      "isWebExtension": true
    |    },
    |    "addon2": {
    |      "isSystem": false,
    |      "isWebExtension": false 
    |    }
    | }""".stripMargin
    )

    val json2theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json2e10s, json2addons, json2theme) should be (Some(false))

    // not quantum ready with non-webextension and non-system addon
    val json3e10s = parse("true")
    val json3addons = parse("""
    | {
    |   "addon1": {
    |     "isSystem": false,
    |     "isWebExtension": false
    |   },
    |   "addon2": {
    |     "isSystem": true,
    |     "isWebExtension": true
    |   }
    | }""".stripMargin
    )

    val json3theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json3e10s, json3addons, json3theme) should be (Some(false))

    // not quantum-ready with old-style theme
    val json4e10s = parse("true")
    val json4addons = parse("""
    | {
    |   "addon1": {
    |     "isSystem": true,
    |     "isWebExtension": false 
    |   },
    |   "addon2": {
    |     "isSystem": false,
    |     "isWebExtension": true
    |   }
    | }""".stripMargin
    )

    val json4theme = parse("""{"id": "old-style@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json4e10s, json4addons, json4theme) should be (Some(false))

    // not quantum-ready if addon is missing isSystem and isWebExtension
    val json5e10s = parse("true")
    val json5addons = parse("""
    | {
    |   "addon1": {
    |     "bladum": true,
    |     "terbei": "hello" 
    |   },
    |   "addon2": {
    |     "isSystem": false,
    |     "isWebExtension": true
    |   }
    | }""".stripMargin
    )

    val json5theme = parse("""{"id": "old-style@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json5e10s, json5addons, json5theme) should be (Some(false))

    // null quantum-ready if theme is missing
    val json6e10s = parse("true")
    val json6addons = parse("""
    | {
    |   "addon1": {
    |     "isSystem": true,
    |     "isWebExtension": true
    |   },
    |   "addon2": {
    |     "isSystem": true,
    |     "isWebExtension": true
    |   }
    | }""".stripMargin
    )

    val json6theme = parse("{}")

    MainSummaryView.getQuantumReady(json6e10s, json6addons, json6theme) should be (None)

    // null quantum-ready if e10s is gibberish
    val json7e10s = parse(""""fewfkew"""")
    val json7addons = parse("""
    | {
    |   "addon1": {
    |     "isSystem": true,
    |     "isWebExtension": true
    |   },
    |   "addon2": {
    |     "isSystem": true,
    |     "isWebExtension": true
    |   }
    | }""".stripMargin
    )

    val json7theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json7e10s, json7addons, json7theme) should be (None)

    // Quantum ready if no addons
    val json8e10s = parse("true")
    val json8addons = parse("{}")
    val json8theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json8e10s, json8addons, json8theme) should be (Some(true))

    // quantum-ready if an addon is missing isSystem or isWebExtension, but the other is true
    val json9e10s = parse("true")
    val json9addons = parse("""
    | {
    |   "addon1": {
    |     "isWebExtension": true
    |   },
    |   "addon2": {
    |     "isSystem": true
    |   }
    | }""".stripMargin
    )
    val json9theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    MainSummaryView.getQuantumReady(json9e10s, json9addons, json9theme) should be (Some(true))
  }

  "Quantum Readiness" can "be properly parsed" in {
    val spark = SparkSession.builder()
        .appName("test quantum readiness")
        .master("local[1]")
        .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.settings" -> """{
          "e10sEnabled": true
        }""",
        "environment.addons" -> """{
          "activeAddons": {
            "spicemustflow": {
              "isSystem": true,
              "isWebExtension": false 
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
      None);
    val summary = MainSummaryView.messageToRow(message, scalarDefs, histogramDefs)

    val expected = Map(
      "quantum_ready" -> true
    )

    val actual = spark
      .createDataFrame(sc.parallelize(List(summary.get)), MainSummaryView.buildSchema(scalarDefs, histogramDefs))
      .first
      .getValuesMap(expected.keys.toList)

    for ((f, v) <- expected) {
      withClue(s"$f:") { actual.get(f) should be (Some(v)) }
      actual.get(f) should be (Some(v))
    }

    actual should be (expected)

    spark.stop()
  }
}
