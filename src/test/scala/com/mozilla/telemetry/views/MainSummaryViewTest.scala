package com.mozilla.telemetry

import com.mozilla.telemetry.heka.{File, Message, RichMessage}
import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.utils._
import com.mozilla.telemetry.views.MainSummaryView
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class MainSummaryViewTest extends FlatSpec with Matchers {
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

  val testUserPrefs = IntegerUserPref("p1") :: BooleanUserPref("p2") :: StringUserPref("P3.MESSY") :: Nil

  val defaultSchema = MainSummaryView.buildSchema(userPrefs, scalarDefs, histogramDefs)

  val defaultMessageToRow = (m: Message) =>
    MainSummaryView.messageToRow(m, scalarDefs, histogramDefs)

  // Apply the given schema to the given potentially-generic Row.
  def applySchema(row: Row, schema: StructType): Row = new GenericRowWithSchema(row.toSeq.toArray, schema)

  def checkAddonValues(row: Row, schema: StructType, expected: Map[String, Any]) = {
    val actual = applySchema(row, schema).getValuesMap(expected.keys.toList)
    val aid = expected("addon_id")
    for ((f, v) <- expected) {
      withClue(s"$aid[$f]:") {
        actual.get(f) should be(Some(v))
      }
    }
    actual should be(expected)
  }

  def compare(message: Message,
              expected: Map[String, Any],
              userPreferences: List[UserPref] = userPrefs,
              scalarDefinitions: List[(String, ScalarDefinition)] = scalarDefs,
              histogramDefinitions: List[(String, HistogramDefinition)] = histogramDefs,
              testInvalidFields: List[String] = Nil
             ): Unit = {

    val summary = MainSummaryView.messageToRow(message, scalarDefinitions, histogramDefinitions, userPreferences)
    val applied = applySchema(summary.get, MainSummaryView.buildSchema(userPreferences, scalarDefinitions, histogramDefinitions))
    val actual = applied.getValuesMap(expected.keys.toList)

    if (!testInvalidFields.isEmpty) {
      intercept[IllegalArgumentException] {
        applied.fieldIndex("noncurrent_histogram")
      }
    }

    for ((f, v) <- expected) {
      withClue(s"$f:") {
        actual.get(f) should be(Some(v))
      }
      actual.get(f) should be(Some(v))
    }
    actual should be(expected)
  }

  "MainSummary records" can "be serialized" in {
    val spark = getOrCreateSparkSession("MainSummaryViewTest")
    spark.sparkContext.setLogLevel("WARN")

    try {
      // Use an example framed-heka message. It is based on test_main.json.gz,
      // submitted with a URL of
      //    /submit/telemetry/foo/main/Firefox/48.0a1/nightly/20160315030230
      for (hekaFileName <- List("/test_main_hindsight.heka", "/test_main.snappy.heka")) {
        val hekaURL = getClass.getResource(hekaFileName)
        val input = hekaURL.openStream()
        val rows = File.parse(input).flatMap(i => defaultMessageToRow(i))

        // Serialize this one row as Parquet
        val dataframe = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(rows.toSeq), defaultSchema)
        val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
        dataframe.write.parquet(tempFile.toString)

        // Then read it back
        val data = spark.read.parquet(tempFile.toString)

        data.count() should be(1)
        data.filter(data("document_id") === "foo").count() should be(1)
      }
    } finally {
      spark.stop()
    }
  }

  "Heka records" can "be summarized" in {
    // Use an example framed-heka message. It is based on test_main.json.gz,
    // submitted with a URL of
    //    /submit/telemetry/foo/main/Firefox/48.0a1/nightly/20160315030230
    for (hekaFileName <- List("/test_main_hindsight.heka", "/test_main.snappy.heka")) {
      val hekaURL = getClass.getResource(hekaFileName)
      val input = hekaURL.openStream()

      var count = 0
      for (message <- File.parse(input)) {
        message.timestamp should be(1460036116829920000l)
        message.`type`.get should be("telemetry")
        message.logger.get should be("telemetry")

        for (summary <- defaultMessageToRow(message)) {
          // Apply our schema to a generic Row object
          val r = applySchema(summary, defaultSchema)

          val expected = Map(
            "document_id" -> "foo",
            "client_id" -> "c4582ba1-79fc-1f47-ae2a-671118dccd8b",
            "sample_id" -> 4l,
            "channel" -> "nightly",
            "normalized_channel" -> "nightly",
            "country" -> "??",
            "city" -> "??",
            "os" -> "Darwin",
            "os_version" -> "15.3.0",
            "os_service_pack_major" -> null,
            "os_service_pack_minor" -> null,
            "windows_build_number" -> null,
            "windows_ubr" -> null,
            "install_year" -> null,
            "is_wow64" -> null,
            "memory_mb" -> 16384,
            "apple_model_id" -> null,
            "profile_creation_date" -> 16861l,
            "profile_reset_date" -> null,
            "subsession_start_date" -> "2016-03-28T00:00:00.0-03:00",
            "subsession_length" -> 14557l,
            "subsession_counter" -> 12,
            "profile_subsession_counter" -> 43,
            "creation_date" -> "2016-03-28T16:02:52.676Z",
            "distribution_id" -> null,
            "submission_date" -> "20160407",
            "sync_configured" -> false,
            "sync_count_desktop" -> null,
            "sync_count_mobile" -> null,
            "app_build_id" -> "20160315030230",
            "app_display_version" -> "48.0a1",
            "app_name" -> "Firefox",
            "app_version" -> "48.0a1",
            "timestamp" -> 1460036116829920000l,
            "env_build_id" -> "20160315030230",
            "env_build_version" -> "48.0a1",
            "env_build_arch" -> "x86-64",
            "e10s_enabled" -> true,
            "e10s_multi_processes" -> null,
            "locale" -> "en-US",
            "active_experiment_id" -> null,
            "active_experiment_branch" -> null,
            "reason" -> "gather-payload",
            "timezone_offset" -> -180,
            "plugin_hangs" -> null,
            "aborts_plugin" -> null,
            "aborts_content" -> null,
            "aborts_gmplugin" -> null,
            "crashes_detected_plugin" -> null,
            "crashes_detected_content" -> null,
            "crashes_detected_gmplugin" -> null,
            "crash_submit_attempt_main" -> null,
            "crash_submit_attempt_content" -> null,
            "crash_submit_attempt_plugin" -> null,
            "crash_submit_success_main" -> null,
            "crash_submit_success_content" -> null,
            "crash_submit_success_plugin" -> null,
            "shutdown_kill" -> null,
            "active_addons_count" -> 3l,
            "flash_version" -> null,
            "vendor" -> "Mozilla",
            "is_default_browser" -> true,
            "default_search_engine_data_name" -> "Google",
            "default_search_engine_data_load_path" -> "jar:[app]/omni.ja!browser/google.xml",
            "default_search_engine_data_origin" -> null,
            "default_search_engine_data_submission_url" -> "https://www.google.com/search?q=&ie=utf-8&oe=utf-8",
            "default_search_engine" -> "google",
            "devtools_toolbox_opened_count" -> 3,
            "client_submission_date" -> null,
            "push_api_notify" -> null,
            "web_notification_shown" -> null,
            "places_pages_count" -> 104849,
            "places_bookmarks_count" -> 183,
            "blocklist_enabled" -> true,
            "addon_compatibility_check_enabled" -> true,
            "telemetry_enabled" -> true,
            "user_prefs" -> null,
            "active_ticks" -> 17354,
            "main" -> 199,
            "first_paint" -> 1999,
            "session_restored" -> 3289,
            "total_time" -> 1027690,
            "plugins_notification_shown" -> null,
            "plugins_notification_user_action" -> null,
            "plugins_infobar_shown" -> null,
            "plugins_infobar_block" -> null,
            "plugins_infobar_allow" -> null,
            "plugins_infobar_dismissed" -> null,
            "search_cohort" -> null,
            "gfx_compositor" -> "none",
            "gc_max_pause_ms_main_above_150" -> 0,
            "gc_max_pause_ms_content_above_2500" -> 0,
            "cycle_collector_max_pause_main_above_150" -> 1416,
            "cycle_collector_max_pause_content_above_2500" -> 0,
            "input_event_response_coalesced_ms_main_above_250" -> 0,
            "input_event_response_coalesced_ms_main_above_2500" -> 0,
            "input_event_response_coalesced_ms_content_above_250" -> 0,
            "input_event_response_coalesced_ms_content_above_2500" -> 0,
            "ghost_windows_main_above_1" -> 0,
            "ghost_windows_content_above_1" -> 0,
            "user_pref_dom_ipc_processcount" -> null,
            "user_pref_extensions_allow_non_mpc_extensions" -> null,
            "user_pref_extensions_legacy_enabled" -> null,
            "scalar_parent_mock_keyed_scalar_bool" -> null,
            "scalar_parent_mock_keyed_scalar_string" -> null,
            "scalar_parent_mock_keyed_scalar_uint" -> null,
            "scalar_parent_mock_scalar_bool" -> null,
            "scalar_parent_mock_scalar_string" -> null,
            "scalar_parent_mock_scalar_uint" -> null,
            "scalar_parent_mock_uint_optin" -> null,
            "scalar_parent_mock_uint_optout" -> null,
            "experiments" -> null
          )

          val actual = r.getValuesMap(expected.keys.toList)
          for ((f, v) <- expected) {
            withClue(s"$f:") {
              actual.get(f) should be(Some(v))
            }
            actual.get(f) should be(Some(v))
          }
          actual should be(expected)

          val searchSchema = MainSummaryView.buildSearchSchema
          val searches = r.getSeq[Row](r.fieldIndex("search_counts"))
          val searchCounter = searches.map(search => {
            val sW = applySchema(search, searchSchema)
            sW.getLong(sW.fieldIndex("count"))
          }).sum
          searchCounter should be(65l)

          val popup = r.getMap[String, Row](r.fieldIndex("popup_notification_stats"))
          val expectedPopup = Map[String, Row](
            "(all)" -> Row(8, 2, 0, 0, 0, 1, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            "geolocation" -> Row(1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            "password" -> Row(5, 0, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            "web-notifications" -> Row(2, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
          popup should be(expectedPopup)

          val addonSchema = MainSummaryView.buildAddonSchema
          checkAddonValues(r.getStruct(r.fieldIndex("active_theme")), addonSchema, Map(
            "addon_id" -> "{972ce4c6-7e08-4474-a285-3208198ce6fd}",
            "blocklisted" -> false,
            "name" -> "Default",
            "user_disabled" -> false,
            "app_disabled" -> false,
            "version" -> "48.0a1",
            "scope" -> 4,
            "type" -> null,
            "foreign_install" -> false,
            "has_binary_components" -> false,
            "install_day" -> 16861,
            "update_day" -> 16875,
            "signed_state" -> null,
            "is_system" -> null,
            "is_web_extension" -> null,
            "multiprocess_compatible" -> null
          ))

          val addons = r.getSeq[Row](r.fieldIndex("active_addons"))
          addons.size should be(3)

          for (addon <- addons) {
            val a = applySchema(addon, addonSchema)
            val addonId = a.getString(a.fieldIndex("addon_id"))
            addonId match {
              case "e10srollout@mozilla.org" => checkAddonValues(addon, addonSchema, Map(
                "addon_id" -> "e10srollout@mozilla.org",
                "blocklisted" -> false,
                "name" -> "Multi-process staged rollout",
                "user_disabled" -> false,
                "app_disabled" -> false,
                "version" -> "1.0",
                "scope" -> 1,
                "type" -> "extension",
                "foreign_install" -> false,
                "has_binary_components" -> false,
                "install_day" -> 16865,
                "update_day" -> 16875,
                "signed_state" -> null,
                "is_system" -> true,
                "is_web_extension" -> null,
                "multiprocess_compatible" -> null
              ))
              case "firefox@getpocket.com" => checkAddonValues(addon, addonSchema, Map(
                "addon_id" -> "firefox@getpocket.com",
                "blocklisted" -> false,
                "name" -> "Pocket",
                "user_disabled" -> false,
                "app_disabled" -> false,
                "version" -> "1.0",
                "scope" -> 1,
                "type" -> "extension",
                "foreign_install" -> false,
                "has_binary_components" -> false,
                "install_day" -> 16861,
                "update_day" -> 16875,
                "signed_state" -> null,
                "is_system" -> true,
                "is_web_extension" -> null,
                "multiprocess_compatible" -> null
              ))
              case "loop@mozilla.org" => checkAddonValues(addon, addonSchema, Map(
                "addon_id" -> "loop@mozilla.org",
                "blocklisted" -> false,
                "name" -> "Firefox Hello Beta",
                "user_disabled" -> false,
                "app_disabled" -> false,
                "version" -> "1.1.12",
                "scope" -> 1,
                "type" -> "extension",
                "foreign_install" -> false,
                "has_binary_components" -> false,
                "install_day" -> 16861,
                "update_day" -> 16875,
                "signed_state" -> null,
                "is_system" -> true,
                "is_web_extension" -> null,
                "multiprocess_compatible" -> null
              ))
              case x => x should be("Should not have happened")
            }
          }
          count += 1
        }
      }
      input.close()
      count should be(1)
    }
  }

  "Job parameters" can "conform to expected values" in {
    MainSummaryView.jobName should be("main_summary")
    val versionPattern = "^v[0-9]+$".r
    (versionPattern findAllIn MainSummaryView.schemaVersion).mkString("Oops") should be(MainSummaryView.schemaVersion)
  }

  "User prefs" can "be extracted" in {
    // Build top-level list of user_pref_*
    val fieldNames = MainSummaryView.buildUserPrefsSchema(userPrefs).fieldNames

    def testUserPrefs(doc: JValue, oldUserPrefs: Any, userPrefs: Seq[Any]): Unit = {
      val message = RichMessage(
        "1234",
        Map(
          "documentId" -> "foo",
          "submissionDate" -> "1234",
          "environment.settings" -> compact(doc \ "environment" \ "settings")
        ),
        None
      )

      val expect = Map("user_prefs" -> oldUserPrefs) ++ fieldNames.zip(userPrefs).toMap

      compare(message, expect)
    }

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
    testUserPrefs(json1, null, Seq(null, null, null, null))

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
    testUserPrefs(json2, null, Seq(null, null, null, null))

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
        |    "extensions.allow-non-mpc-extensions": true,
        |    "browser.search.widget.inNavBar": false
        |   }
        |  }
        | }
        |}
      """.stripMargin)
    testUserPrefs(json3, Row(2, true), Seq(2, true, null, false))

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
    testUserPrefs(json4, null, Seq(null, null, null, null))

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
    testUserPrefs(json5, null, Seq(null, null, null, null))

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
    testUserPrefs(json6, Row(4, null), Seq(4, null, null, null))

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
    testUserPrefs(json7, Row(null, false), Seq(null, false, null, null))
  }

  it can "be properly shown" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.settings" ->
          """
            |{
            |  "userPrefs": {
            |    "dom.ipc.processCount": 2
            |  }
            |}""".stripMargin),
      None)

    val expected = Map("user_prefs" -> Row(2, null))

    compare(message, expected)
  }

  it can "be built" in {
    val userPrefsSchema = MainSummaryView.buildUserPrefsSchema(testUserPrefs)
    userPrefsSchema.fields.length should be (3)

    userPrefsSchema.fieldNames should be (List("user_pref_p1", "user_pref_p2", "user_pref_p3_messy"))

    userPrefsSchema.fields(0).dataType should be (IntegerType)
    userPrefsSchema.fields(1).dataType should be (BooleanType)
    userPrefsSchema.fields(2).dataType should be (StringType)
  }

  it can "be added to MainSummary schema" in {
    val schemaWithPrefs = MainSummaryView.buildSchema(testUserPrefs, List(), List())

    schemaWithPrefs.fieldNames.contains("user_pref_p3_messy") should be (true)
    schemaWithPrefs.fieldNames.contains("user_pref_p4") should be (false)
  }

  it can "be added to the top-level" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.settings" ->
          """
            |  {
            |  "userPrefs": {
            |   "p1": 10,
            |   "p2": false,
            |   "P3.MESSY": "bar"
            |  }
            |}""".stripMargin),
      None)

    val expected = Map(
      "user_pref_p1" -> 10,
      "user_pref_p2" -> false,
      "user_pref_p3_messy" -> "bar"
    )

    compare(message, expected, testUserPrefs, testInvalidFields = List("user_pref_4"))
  }

  "Stub attribution" can "be extracted" in {
    val cases = Seq(
      // Contains a single attribute
      (
        """
          |{
          |  "attribution": {
          |    "source": "sample_source"
          |  }
          |}
        """.stripMargin,
        Row("sample_source", null, null, null)
      ),
      // Contains no attributes
      (
        """
          |{
          | "attribution": {}
          |}
        """.stripMargin,
        null
      ),
      // Contains all attributes, in no particular order
      (
        """
          |{
          |  "attribution": {
          |    "content": "sample_content",
          |    "source": "sample_source",
          |    "medium": "sample_medium",
          |    "campaign": "sample_campaign"
          |  }
          |}
        """.stripMargin,
        Row("sample_source", "sample_medium", "sample_campaign", "sample_content")
      )
    )

    for ((attribution, expected) <- cases) {
      val message = RichMessage("1234",
        Map(
          "documentId" -> "foo",
          "submissionDate" -> "1234",
          "environment.settings" -> attribution
        ),
        None)

      compare(message, Map("attribution" -> expected))
    }
  }

  "MainSummary plugin counts" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" ->
          """
            |{
            |  "PLUGINS_NOTIFICATION_SHOWN":{
            |    "range":[1,2],
            |    "histogram_type":2,
            |    "values":{"1":3,"0":0,"2":0},
            |    "bucket_count":3,
            |    "sum":3
            |  },
            |  "PLUGINS_NOTIFICATION_USER_ACTION":{
            |    "range":[1,3],
            |    "histogram_type":1,
            |    "values":{"1":0,"0":3},
            |    "bucket_count":4,
            |    "sum":0
            |  },
            |  "PLUGINS_INFOBAR_SHOWN": {
            |     "range": [1,2],
            |     "histogram_type": 2,
            |     "values":{"1":12,"0":0,"2":0},
            |     "bucket_count":3,
            |     "sum":12
            |  },
            |  "PLUGINS_INFOBAR_ALLOW":{
            |    "range":[1,2],
            |    "histogram_type":2,
            |    "values":{"1":2,"0":0,"2":0},
            |    "bucket_count":3,
            |    "sum":2
            |  },
            |  "PLUGINS_INFOBAR_BLOCK":{
            |    "range":[1,2],
            |    "histogram_type":2,
            |    "values":{"1":1,"0":0,"2":0},
            |    "bucket_count":3,
            |    "sum":1
            |  },
            |  "PLUGINS_INFOBAR_DISMISSED":{
            |    "range":[1,2],
            |    "histogram_type":2,
            |    "values":{"1":1,"0":0,"2":0},
            |    "bucket_count":3,
            |    "sum":1
            |  }
            |}""".stripMargin),
      None)

    val expected = Map(
      "document_id" -> "foo",
      "plugins_notification_shown" -> 3,
      "plugins_notification_user_action" -> Row(3, 0, 0),
      "plugins_infobar_shown" -> 12,
      "plugins_infobar_allow" -> 2,
      "plugins_infobar_block" -> 1,
      "plugins_infobar_dismissed" -> 1
    )

    compare(message, expected)
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

    val expected = Map(
      "document_id" -> "foo",
      "experiments" -> Map("experiment1" -> "alpha", "experiment2" -> "beta")
    )

    compare(message, expected)
  }

  "MainSummary legacy addons" can "be summarized" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.addonDetails" ->
          """
            |{
            |  "XPI": {
            |    "some-disabled-addon-id": {
            |      "dont-care": "about-this-data",
            |      "we-discard-this": 11
            |    },
            |    "active-addon-id": {
            |      "dont-care": 12
            |    }
            |  }
            |}""".stripMargin,
        "environment.addons" ->
          """
            |{
            |  "activeAddons": {
            |    "active-addon-id": {
            |      "isSystem": false,
            |      "isWebExtension": true
            |    },
            |    "gom-jabbar": {
            |      "isSystem": false,
            |      "isWebExtension": true
            |    }
            |  },
            |  "theme": {
            |    "id": "firefox-compact-light@mozilla.org"
            |  }
            |}""".stripMargin),
      None)

    // This will make sure that:
    // - the disabled addon is in the list;
    // - active addons are filtered out.
    val expected = Map(
      "document_id" -> "foo",
      "disabled_addons_ids" -> List("some-disabled-addon-id")
    )

    compare(message, expected)
  }

  "Keyed Scalars" can "be properly shown" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" ->
          """
            |{
            | "payload": {
            |   "processes": {
            |     "parent": {
            |       "keyedScalars": {
            |         "mock.keyed.scalar.uint": {
            |           "search_enter": 1,
            |           "search_suggestion": 2
            |         }
            |       }
            |     }
            |   }
            | }
            |}""".stripMargin),
      None)

    val expected = Map(
      "scalar_parent_mock_keyed_scalar_uint" -> Map(
        "search_enter" -> 1,
        "search_suggestion" -> 2
      )
    )

    compare(message, expected)
  }


  "Search cohort" can "be properly shown" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.settings" -> """{"searchCohort": "helloworld"}"""),
      None)

    val expected = Map(
      "search_cohort" -> "helloworld"
    )

    compare(message, expected)
  }

  "Histograms" can "be stored" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" ->
          """
            |{
            |  "MOCK_EXPONENTIAL_OPTOUT": {
            |      "range": [1,100],
            |      "bucket_count": 10,
            |      "histogram_type": 0,
            |      "values": {
            |        "1": 0,
            |        "16": 1,
            |        "54": 1
            |      },
            |      "sum": 64
            |    },
            |  "MOCK_OPTOUT": {
            |    "range": [1,10],
            |    "bucket_count": 10,
            |    "histogram_type": 2,
            |    "values": {
            |      "1": 0,
            |      "3": 1,
            |      "9": 1
            |    },
            |    "sum": 12
            |  }
            |}""".stripMargin),
      None)

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

    compare(message, expected)
  }

  "Keyed Histograms" can "be stored" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.keyedHistograms" ->
          """
            |{
            |  "MOCK_KEYED_LINEAR": {
            |    "hello": {
            |      "range": [1,10],
            |      "bucket_count": 10,
            |      "histogram_type": 0,
            |      "values": {
            |        "1": 0,
            |        "3": 1,
            |        "9": 1
            |      },
            |      "sum": 12
            |    },
            |    "world": {
            |      "range": [1,10],
            |      "bucket_count": 10,
            |      "histogram_type": 0,
            |      "values": {
            |        "1": 0,
            |        "2": 1
            |      },
            |      "sum": 2
            |    }
            |  },
            |  "MOCK_KEYED_EXPONENTIAL": {
            |    "foo": {
            |      "range": [1,100],
            |      "bucket_count": 10,
            |      "histogram_type": 0,
            |      "values": {
            |        "1": 0,
            |        "16": 1,
            |        "54": 1
            |      },
            |      "sum": 64
            |    },
            |    "42": {
            |      "range": [1,100],
            |      "bucket_count": 10,
            |      "histogram_type": 0,
            |      "values": {
            |        "1": 1
            |      },
            |      "sum": 0
            |    }
            |  }
            |}""".stripMargin),
      None)

    val mock_lin_vals = Map(
      "hello" -> Map(1 -> 0, 3 -> 1, 9 -> 1),
      "world" -> Map(1 -> 0, 2 -> 1)
    )

    val mock_exp_vals = Map(
      "foo" -> Map(1 -> 0, 16 -> 1, 54 -> 1),
      "42" -> Map(1 -> 1)
    )

    val expected = Map(
      "histogram_parent_mock_keyed_linear" -> mock_lin_vals,
      "histogram_parent_mock_keyed_exponential" -> mock_exp_vals
    )

    compare(message, expected)
  }

  "Bad histograms" can "be handled" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.keyedHistograms" ->
          """
            |{
            |  "MOCK_KEYED_LINEAR": {
            |    "hello": {
            |      "theempiredidnothingwrong": true,
            |      "histogram_type": 0,
            |      "valuess": {
            |        "1": 0,
            |        "3": 1,
            |        "9": 1
            |      },
            |      "summ": 12
            |    }
            |  },
            |  "NONCURRENT_KEYED_HISTOGRAM": {
            |    "foo": {
            |      "range": [1,100],
            |      "bucket_count": 10,
            |      "histogram_type": 0,
            |      "values": {
            |        "1": 0,
            |        "16": 1,
            |        "54": 1
            |      },
            |      "sum": 64
            |    },
            |    "42": {
            |      "range": [1,100],
            |      "bucket_count": 10,
            |      "histogram_type": 0,
            |      "values": {
            |        "1": 1
            |      },
            |      "sum": 0
            |    }
            |  }
            |}""".stripMargin),
      None)

    val expected = Map(
      "histogram_parent_mock_keyed_linear" -> Map("hello" -> null)
    )

    compare(message, expected, testInvalidFields = List("noncurrent_histogram"))
  }

  "All possible histograms and scalars" can "be included" in {
    val allHistogramDefs = MainSummaryView.filterHistogramDefinitions(Histograms.definitions(includeOptin = false, nameJoiner = Histograms.prefixProcessJoiner _, includeCategorical = true), useWhitelist = true)
    val allScalarDefs = Scalars.definitions(includeOptin = true).toList.sortBy(_._1)

    val fakeHisto =
      """{
          "sum": 100,
          "values": {"1": 0, "2": 10, "40": 100, "50": 1000, "100": 1002},
          "bucketCount": 100,
          "range": [0, 100],
          "histogramType": 2
        }"""

    val histosData = allHistogramDefs.filter {
      case (name, definition) => !definition.keyed
    }.map {
      case (name, _) => s""""$name": $fakeHisto"""
    }.mkString(",")

    val keyedHistosData = allHistogramDefs.filter {
      case (name, definition) => definition.keyed
    }.map {
      case (name, _) =>
        s"""
        "$name": {
          "key1": $fakeHisto,
          "key2": $fakeHisto,
          "key3": $fakeHisto
        }"""
    }.mkString(",")

    val scalarsData = allScalarDefs
      .filter { case (n, d) => !d.keyed }
      .map {
        case (n, d) => d match {
          case _: UintScalar => (n, 1)
          case _: BooleanScalar => (n, false)
          case _: StringScalar => (n, """"tfw"""")
        }
      }.map { case (n, v) => s""""$n": $v""" }.mkString(",")

    val keyedScalarsData = allScalarDefs
      .filter { case (n, d) => d.keyed }
      .map {
        case (n, d) => d match {
          case _: UintScalar => (n, """"key1": 1, "key2": 1""")
          case _: BooleanScalar => (n, """"key1": true, "key2": false""")
          case _: StringScalar => (n, """"key1": "empire", "key2": "didnothingwrong"""")
        }
      }.map { case (n, v) => s""""$n": {$v}""" }.mkString(",")

    val processHistograms =
      s"""{
      "histograms": {$histosData},
      "keyedHistograms": {$keyedHistosData},
      "scalars": {$scalarsData},
      "keyedScalars": {$keyedScalarsData}
    }"""

    val processJSON = MainPing.ProcessTypes.map { p => s""""$p": $processHistograms""" }.mkString(", ")

    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" -> s"{$histosData}",
        "payload.keyedHistograms" -> s"{$keyedHistosData}",
        "submission" ->
          s"""{
  "payload": {
    "processes": {$processJSON}
   }
  }"""
      ),
      None)

    val expectedProcessHistos = MainPing.ProcessTypes.map { p =>
      p -> (parse(s"{$histosData}") merge parse(s"{$keyedHistosData}"))
    }.toMap

    val expectedHistos = MainPing
      .histogramsToRow(expectedProcessHistos, allHistogramDefs)
      .toSeq.zip(allHistogramDefs.map(_._1)).map(_.swap).toMap

    val expectedProcessScalars = MainPing.ProcessTypes.map { p =>
      p -> (parse(s"{$scalarsData}") merge parse(s"{$keyedScalarsData}"))
    }.toMap

    val expectedScalars = MainPing
      .scalarsToRow(expectedProcessScalars, allScalarDefs)
      .toSeq.zip(allScalarDefs.map(_._1)).map(_.swap).toMap

    val expected = expectedHistos ++ expectedScalars

    compare(message, expected, userPrefs, allScalarDefs, allHistogramDefs)
  }

  "Histogram filter" can "include all whitelisted histograms" in {
    val allHistogramDefs = MainSummaryView.filterHistogramDefinitions(
      Histograms.definitions(includeOptin = true, nameJoiner = Histograms.prefixProcessJoiner _, includeCategorical = true),
      useWhitelist = true
    ).map { case (name, definition) => definition.originalName }.toSet

    val expectedDefs = MainSummaryView.histogramsWhitelist.toSet

    allHistogramDefs should be(expectedDefs)
  }

  "Quantum Ready" should "be correct for a ping" in {

    def testQuantumReady(e10s: JValue, addons: JValue, theme: JValue, expect: Any): Unit = {
      val message = RichMessage(
        "1234",
        Map(
          "documentId" -> "foo",
          "submissionDate" -> "1234",
          "environment.settings" -> s"""{"e10sEnabled": ${compact(e10s)} }""",
          "environment.addons" -> compact(("activeAddons" -> addons) ~ ("theme" -> theme))
        ),
        None
      )

      compare(message, Map("quantum_ready" -> expect))
    }

    val json0e10s = parse("true")
    val json0addons = parse(
      """
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

    val json0theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    testQuantumReady(json0e10s, json0addons, json0theme, true)

    // not quantum ready with no e10s
    val json1e10s = parse("false")
    val json1addons = parse(
      """
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

    testQuantumReady(json1e10s, json1addons, json1theme, false)

    // not quantum ready with non-system and non-webextension addon
    val json2e10s = parse("true")
    val json2addons = parse(
      """
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

    testQuantumReady(json2e10s, json2addons, json2theme, false)

    // not quantum ready with non-webextension and non-system addon
    val json3e10s = parse("true")
    val json3addons = parse(
      """
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

    testQuantumReady(json3e10s, json3addons, json3theme, false)

    // not quantum-ready with old-style theme
    val json4e10s = parse("true")
    val json4addons = parse(
      """
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

    testQuantumReady(json4e10s, json4addons, json4theme, false)

    // not quantum-ready if addon is missing isSystem and isWebExtension
    val json5e10s = parse("true")
    val json5addons = parse(
      """
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

    testQuantumReady(json5e10s, json5addons, json5theme, false)

    // null quantum-ready if theme is missing
    val json6e10s = parse("true")
    val json6addons = parse(
      """
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

    testQuantumReady(json6e10s, json6addons, json6theme, null)

    // null quantum-ready if e10s is gibberish
    val json7e10s = parse(""""fewfkew"""")
    val json7addons = parse(
      """
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

    testQuantumReady(json7e10s, json7addons, json7theme, null)

    // Quantum ready if no addons
    val json8e10s = parse("true")
    val json8addons = parse("{}")
    val json8theme = parse("""{"id": "firefox-compact-light@mozilla.org"}""")

    testQuantumReady(json8e10s, json8addons, json8theme, true)

    // quantum-ready if an addon is missing isSystem or isWebExtension, but the other is true
    val json9e10s = parse("true")
    val json9addons = parse(
      """
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

    testQuantumReady(json9e10s, json9addons, json9theme, true)
  }

  "Process Histograms" can "be stored" in {
    val message = RichMessage(
      "2235",
      Map(
        "documentId" -> "foobar",
        "submissionDate" -> "12345",
        "submission" ->
          """
            |{
            |  "payload": {
            |    "processes": {
            |      "content": {
            |        "histograms": {
            |          "MOCK_EXPONENTIAL_OPTOUT": {
            |            "range": [1,100],
            |            "bucket_count": 10,
            |            "histogram_type": 0,
            |            "values": {
            |              "1": 0,
            |              "16": 1,
            |              "54": 1
            |            },
            |            "sum": 64
            |          },
            |          "MOCK_OPTOUT": {
            |            "range": [1,10],
            |            "bucket_count": 10,
            |            "histogram_type": 2,
            |            "values": {
            |              "1": 0,
            |              "3": 1,
            |              "9": 1
            |            },
            |            "sum": 12
            |          }
            |        },
            |        "keyedHistograms": {
            |          "MOCK_KEYED_LINEAR": {
            |            "foo": {
            |              "range": [1,100],
            |              "bucket_count": 10,
            |              "histogram_type": 0,
            |              "values": {
            |                "1": 0,
            |                "16": 1,
            |                "54": 1
            |              },
            |              "sum": 64
            |            },
            |            "bar": {
            |              "range": [1,100],
            |              "bucket_count": 10,
            |              "histogram_type": 0,
            |              "values": {
            |                "1": 1
            |              },
            |              "sum": 0
            |            }
            |          }
            |        }
            |      },
            |      "gpu": {
            |        "histograms": {
            |          "MOCK_EXPONENTIAL_OPTOUT": {
            |            "range": [1,100],
            |            "bucket_count": 10,
            |            "histogram_type": 0,
            |            "values": {
            |              "1": 0,
            |              "16": 1,
            |              "54": 1
            |            },
            |            "sum": 64
            |          },
            |          "MOCK_OPTOUT": {
            |            "range": [1,10],
            |            "bucket_count": 10,
            |            "histogram_type": 2,
            |            "values": {
            |              "1": 0,
            |              "3": 1,
            |              "9": 1
            |            },
            |            "sum": 12
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin),
      None)

    val expected = Map(
      "histogram_content_mock_exponential_optout" -> Map(1 -> 0, 16 -> 1, 54 -> 1),
      "histogram_gpu_mock_exponential_optout" -> Map(1 -> 0, 16 -> 1, 54 -> 1),
      "histogram_content_mock_optout" -> Map(1 -> 0, 3 -> 1, 9 -> 1),
      "histogram_gpu_mock_optout" -> Map(1 -> 0, 3 -> 1, 9 -> 1),
      "histogram_content_mock_keyed_linear" -> Map("foo" -> Map(1 -> 0, 16 -> 1, 54 -> 1), "bar" -> Map(1 -> 1))
    )

    compare(message, expected)
  }

  "Process Scalars" can "be properly shown" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" ->
          """
            |{
            |  "payload": {
            |    "processes": {
            |      "content": {
            |        "keyedScalars": {
            |          "mock.keyed.scalar.uint": {
            |            "search_enter": 1,
            |            "search_suggestion": 2
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin),
      None)

    val expected = Map(
      "scalar_content_mock_keyed_scalar_uint" -> Map(
        "search_enter" -> 1,
        "search_suggestion" -> 2
      )
    )

    compare(message, expected)
  }

  "Migrated scalar values" can "select the correct scalar when both are present" in {
    val migratedScalarsUrl = (a: String, b: String) => Source.fromFile("src/test/resources/ScalarsFromSimpleMeasures.yaml")
    val migratedScalars = new ScalarsClass {
      override protected val getURL = migratedScalarsUrl
    }
    val scalarsDef = migratedScalars.definitions(includeOptin = true).toList

    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.simpleMeasurements" ->
          """{"activeTicks": 111,
                "firstPaint": 222}""",
        "submission" ->
          """
            |{
            |  "payload": {
            |    "processes": {
            |      "parent": {
            |        "scalars": {
            |          "browser.engagement.active_ticks": 888,
            |          "timestamps.first_paint": 999
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
      ),
      None
    )

    val expected = Map(
      "active_ticks" -> 888,
      "first_paint" -> 999
    )

    compare(message, expected, scalarDefinitions = scalarsDef)
  }

  it can "fall back to simple measurements values" in {
    val migratedScalarsUrl = (a: String, b: String) => Source.fromFile("src/test/resources/ScalarsFromSimpleMeasures.yaml")
    val migratedScalars = new ScalarsClass {
      override protected val getURL = migratedScalarsUrl
    }
    val scalarsDef = migratedScalars.definitions(includeOptin = true).toList

    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.simpleMeasurements" ->
          """
            |{
            |  "activeTicks": 111,
            |  "firstPaint": 222
            |}""".stripMargin
      ),
      None)

    val expected = Map(
      "active_ticks" -> 111,
      "first_paint" -> 222
    )

    compare(message, expected, scalarDefinitions = scalarsDef)
  }

  "Simple measurements values" can "be selected in the absence of a scalar definition." in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.simpleMeasurements" ->
          """
            |{
            |  "activeTicks": 111,
            |  "firstPaint": 222
            |}""".stripMargin
      ),
      None)

    val expected = Map(
      "active_ticks" -> 111,
      "first_paint" -> 222
    )

    compare(message, expected, scalarDefinitions = List())
  }

  "Main Summary" can "store categorical histograms" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" ->
          """
            |{
            |  "MOCK_CATEGORICAL": {
            |    "range": [1,100],
            |    "bucket_count": 51,
            |    "histogram_type": 1,
            |    "values": {
            |      "1": 0,
            |      "2": 1,
            |      "3": 1,
            |      "5": 1
            |    },
            |    "sum": 10
            |  }
            |}""".stripMargin),
      None)
    val expected = Map(
      "histogram_parent_mock_categorical" -> Map("am" -> 0, "a" -> 1, "strange" -> 1, CategoricalHistogram.SpillBucketName -> 1)
    )

    compare(message, expected)
  }

  it can "store keyed categorical histograms" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" ->
          """
            |{
            |  "MOCK_KEYED_CATEGORICAL": {
            |    "gaius": {
            |      "range": [1,20],
            |      "bucket_count": 51,
            |      "histogram_type": 1,
            |      "values": {
            |        "0": 1,
            |        "1": 1,
            |        "2": 1,
            |        "6": 1
            |      },
            |      "sum": 9
            |    }
            |  }
            |}""".stripMargin),
      None)
    val expected = Map(
      "histogram_parent_mock_keyed_categorical" -> Map("gaius" -> Map("all" -> 1, "of" -> 1, "this" -> 1, CategoricalHistogram.SpillBucketName -> 1))
    )

    compare(message, expected)
  }

  it can "handle incorrect categorical histogram buckets" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "payload.histograms" ->
          """
            |{
            |  "MOCK_CATEGORICAL": {
            |    "range": [1,100],
            |    "bucket_count": 51,
            |    "histogram_type": 1,
            |    "values": {
            |      "1": 0,
            |      "2": 1,
            |      "3": 1,
            |      "5": 1,
            |      "10": 1,
            |      "100": 1
            |    },
            |    "sum": 10
            |  }
            |}""".stripMargin),
      None)
    val expected = Map(
      "histogram_parent_mock_categorical" -> Map("am" -> 0, "a" -> 1, "strange" -> 1, CategoricalHistogram.SpillBucketName -> 3)
    )

    compare(message, expected)
  }

  it can "properly show e10s_multi_processes" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.settings" ->
          """{
          "e10sMultiProcesses": 12
        }"""),
      None)

    val expected = Map(
      "e10s_multi_processes" -> 12
    )

    compare(message, expected)
  }

  it can "store multi-process events" in {
    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "submission" ->
          """
            |{
            |  "payload": {
            |    "processes": {
            |      "parent": {
            |        "events": [[81994404, "navigation", "search", "searchbar"]]
            |      },
            |      "content": {
            |        "events": [[81994404, "navigation", "search", "searchbar"]]
            |      },
            |      "dynamic": {
            |        "events": [[81994404, "navigation", "search", "searchbar"]]
            |      }
            |    }
            |  }
            |}""".stripMargin),
      None);
    val summary = defaultMessageToRow(message)
    val expected = Set(
      Row(81994404, "navigation", "search", "searchbar", null, Map("telemetry_process" -> "dynamic")),
      Row(81994404, "navigation", "search", "searchbar", null, Map("telemetry_process" -> "content")),
      Row(81994404, "navigation", "search", "searchbar", null, Map("telemetry_process" -> "parent"))
    )

    val actual = applySchema(summary.get, MainSummaryView.buildSchema(userPrefs, scalarDefs, histogramDefs))
      .getValuesMap[List[Row]](List("events"))

    actual.get("events").orNull should contain theSameElementsAs (expected)
  }

  "Security" can "be properly shown" in {

    val message = RichMessage(
      "1234",
      Map(
        "documentId" -> "foo",
        "submissionDate" -> "1234",
        "environment.system" ->
          """
            |{
            |  "sec": {
            |    "antivirus": ["av_1"],
            |    "antispyware": ["asw_1", "asw_2"],
            |    "firewall": null
            |  }
            |}""".stripMargin),
      None)

    val expected = Map(
      "antivirus" -> Seq("av_1"),
      "antispyware" -> Seq("asw_1", "asw_2"),
      "firewall" -> null
    )
    compare(message, expected)
  }
}
