package com.mozilla.telemetry.views

import com.mozilla.telemetry.parquet.ParquetFile
import com.mozilla.telemetry.scalars._
import com.mozilla.telemetry.views.LongitudinalView
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray

class LongitudinalTest extends FlatSpec with Matchers with PrivateMethodTester {
  val fixture = {
    def createPayload(idx: Int): Map[String, Any] = {
      val histograms =
        ("TELEMETRY_TEST_FLAG" ->
          ("values" -> ("0" -> 0)) ~
          ("sum" -> 0)) ~
        ("DEVTOOLS_WEBIDE_CONNECTION_RESULT" ->
          ("values" -> ("0" -> 42)) ~
          ("sum" -> 0)) ~
        ("UPDATE_CHECK_NO_UPDATE_EXTERNAL" ->
          ("values" -> ("0" -> 42)) ~
          ("sum" -> 42)) ~
        ("PLACES_BACKUPS_DAYSFROMLAST" ->
          ("values" -> ("1" -> 42)) ~
          ("sum" -> 42)) ~
        ("GC_BUDGET_MS" ->
          ("values" -> ("1" -> 42)) ~
          ("sum" -> 42)) ~
        ("GC_MS" ->
          ("values" -> ("1" -> 42)) ~
          ("sum" -> 42))

      val keyedHistograms =
        ("ADDON_SHIM_USAGE" ->
          ("foo" ->
            ("values" -> ("1" -> 42)) ~
            ("sum" -> 42))) ~
        ("SEARCH_COUNTS" ->
          ("foo" ->
            ("values" -> ("0" -> 42)) ~
            ("sum" -> 42))) ~
        ("DEVTOOLS_PERFTOOLS_SELECTED_VIEW_MS" ->
          ("foo" ->
            ("values" -> ("1" -> 42)) ~
            ("sum" -> 42)))

      val scalars =
        ("telemetry.test.unsigned_int_kind" -> 37 ) ~
        ("mock.scalar.uint" -> 3) ~
        ("mock.scalar.bool" -> true) ~
        ("mock.scalar.string" -> "a nice string scalar")

      val keyedScalars =
        ("mock.keyed.scalar.uint" ->
          ("a_key" -> 37) ~
          ("second_key" -> 42)) ~
        ("mock.keyed.scalar.bool" ->
          ("foo" -> true) ~
          ("bar" -> false)) ~
        ("mock.keyed.scalar.string" ->
          ("fizz" -> "buzz") ~
          ("other" -> "some"))

      var pingPayload =
        if (idx == 1) {
          // Skip the scalar section for the first payload.
          ("processes" ->
            ("parent" ->
              ("bogus" -> "other") ~
              ("keyedScalars" -> keyedScalars)))
        } else {
          ("processes" ->
            ("parent" ->
              ("scalars" -> scalars) ~
              ("keyedScalars" -> keyedScalars)))
        }

      val simpleMeasurements = "uptime" -> 18L

      val build =
        ("applicationId" -> "{ec8030f7-c20a-464f-9b0e-13a3a9e97384}") ~
        ("applicationName" -> "Firefox") ~
        ("architecture" -> "x86-64") ~
        ("buildId" -> "20160101001100") ~
        ("version" -> "46.0a2") ~
        ("vendor" -> "Mozilla") ~
        ("platformVersion" -> "46.0a2") ~
        ("xpcomAbi" -> "x86_64-gcc3")

      val partner = "partnerNames" -> List("A", "B", "C")

      val profile =
        ("creationDate" -> 16122) ~
        ("resetDate" -> 16132)

      val settings =
        ("e10sEnabled" -> true) ~
        ("userPrefs" -> Map("browser.download.lastDir" -> "/home/anthony/Desktop"))

      val system =
        ("memoryMB" -> 2048) ~
        ("cpu" ->
          ("count" -> 4)) ~
        ("device" ->
          ("model" -> "SHARP")) ~
        ("os" ->
          ("name" -> "Windows_NT") ~
          ("locale" -> "en_US") ~
          ("windowsBuildNumber" -> 10586) ~
          ("windowsUBR" -> 446) ~
          ("installYear" -> 2016) ~
          ("version" -> "6.1")) ~
        ("hdd" ->
          ("profile" ->
            ("revision" -> "12345") ~
            ("model" -> "SAMSUNG X"))) ~
        ("gfx" ->
          ("adapters" -> List(
            ("RAM" -> 1024) ~ ("description" -> "FOO1") ~ ("deviceID" -> "1") ~ ("vendorID" -> "Vendor1") ~ ("GPUActive" -> true),
            ("RAM" -> 1024) ~ ("description" -> "FOO2") ~ ("deviceID" -> "2") ~ ("vendorID" -> "Vendor2") ~ ("GPUActive" -> false))))

      val addons =
        ("activeAddons" -> Map(
          "jid0-edalmuivkozlouyij0lpdx548bc@jetpack" ->
            ("name" -> "geckoprofiler") ~
            ("version" -> "1.16.14") ~
            ("isSystem" -> true))) ~
        ("theme" ->
          ("id" -> "{972ce4c6-7e08-4474-a285-3208198ce6fd}") ~
          ("description" -> "The default theme.")) ~
        ("activePlugins" -> List(
          ("blocklisted" -> false) ~
          ("description" -> "Adobe PDF Plug-In For Firefox and Netscape 10.1.16") ~
          ("clicktoplay" -> true))) ~
        ("activeGMPlugins" -> Map(
          "gmp-eme-adobe" ->
            ("applyBackgroundUpdates" -> 1) ~
            ("userDisabled" -> false),
          "gmp-gmpopenh264" ->
            ("applyBackgroundUpdates" -> 1) ~
            ("userDisabled" -> false))) ~
        ("activeExperiment" ->
          ("id" -> "A") ~
          ("branch" -> "B"))

      val info =
        ("subsessionStartDate" -> "2015-12-09T00:00:00.0-14:00") ~
        ("profileSubsessionCounter" -> (1000 - idx)) ~
        ("reason" -> "shutdown")

      Map("clientId" -> "26c9d181-b95b-4af5-bb35-84ebf0da795d",
        "os" -> "Windows_NT",
        "normalizedChannel" -> "aurora",
        "documentId" -> idx.toString,
        "submissionDate" -> "20160128",
        "sampleId" -> 42.0,
        "Size" -> 93691.0,
        "creationTimestamp" -> 1.45393974518300006E18,
        "geoCountry" -> "US",
        "geoCity" -> "New York",
        "DNT" -> "1",
        "payload.info" -> compact(render(info)),
        "payload.simpleMeasurements" -> compact(render(simpleMeasurements)),
        "payload.histograms" -> compact(render(histograms)),
        "payload.keyedHistograms" -> compact(render(keyedHistograms)),
        "payload" -> render(pingPayload),
        "environment.build" -> compact(render(build)),
        "environment.partner" -> compact(render(partner)),
        "environment.profile" -> compact(render(profile)),
        "environment.settings" -> compact(render(settings)),
        "environment.system" -> compact(render(system)),
        "environment.addons" -> compact(render(addons)))
    }

    new {
      // Mock the scalars definitions to ease testing.
      Scalars.definitions =
        Map(
          ("mock.scalar.uint", UintScalar(false)),
          ("mock.scalar.bool", BooleanScalar(false)),
          ("mock.scalar.string", StringScalar(false)),
          ("mock.keyed.scalar.uint", UintScalar(true)),
          ("mock.keyed.scalar.bool", BooleanScalar(true)),
          ("mock.keyed.scalar.string", StringScalar(true))
        )

      private val buildSchema = PrivateMethod[Schema]('buildSchema)
      private val buildRecord = PrivateMethod[Option[GenericRecord]]('buildRecord)

      private val schema = LongitudinalView invokePrivate buildSchema()
      val payloads = for (i <- 1 to 10) yield createPayload(i)
      private val dupes = for (i <- 1 to 10) yield createPayload(1)
      private val record = (LongitudinalView  invokePrivate buildRecord(payloads ++ dupes, schema)).get
      private val path = ParquetFile.serialize(List(record).toIterator, schema)
      private val filename = path.toString.replace("file:", "")

      private val sparkConf = new SparkConf().setAppName("Longitudinal")
      sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
      private val sc = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      private val sqlContext = new SQLContext(sc)
      val rows = sqlContext.read.load(filename).collect()
      val row =  rows(0)
      sc.stop()
    }
  }

  "Scalar fields" must "be converted correctly" in {
    val stringFields = Array(
      "submission_date"       -> "2016-01-28T00:00:00.000Z",
      "geo_country"           -> "US",
      "geo_city"              -> "New York",
      "dnt_header"            -> "1",
      "subsession_start_date" -> "2015-12-09T12:00:00.000-02:00",
      "profile_creation_date" -> "2014-02-21T00:00:00.000Z",
      "profile_reset_date"    -> "2014-03-03T00:00:00.000Z"
    )

    val floatFields = Array(
      "sample_id"             -> 42.0,
      "size"                  -> 93691.0
    )

    def compareFields[T](fields: Array[(String, T)]) {
      for ((key, reference) <- fields) {
        val records = fixture.row.getList[T](fixture.row.fieldIndex(key))
        assert(records.size == fixture.payloads.size)
        records.foreach(x => assert(x == reference))
      }
    }

    assert(fixture.rows.length == 1)
    compareFields(stringFields)
    compareFields(floatFields)
  }

  "Top-level measurements" must "be converted correctly" in {
    assert(fixture.row.getAs[String]("client_id") == fixture.payloads(0)("clientId"))
    assert(fixture.row.getAs[String]("os") == fixture.payloads(0)("os"))
    assert(fixture.row.getAs[String]("normalized_channel") == fixture.payloads(0)("normalizedChannel"))
  }

  "payload.info" must "be converted correctly" in {
    val reasonRecords = fixture.row.getList[String](fixture.row.fieldIndex("reason"))
    assert(reasonRecords.length == fixture.payloads.length)
    reasonRecords.foreach(x => assert(x == "shutdown"))
  }

  "environment.build" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("build"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("build_id") == "20160101001100"))
  }

  "environment.partner" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("partner"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val partner_names = x.getList[String](x.fieldIndex("partner_names"))
      assert(partner_names.toList == List("A", "B", "C"))
    }
  }

  "environment.system" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("system"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Int]("memory_mb") == 2048))
  }

  "environment.system/cpu" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("system_cpu"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Int]("count") == 4))
  }

  "environment.system/device" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("system_device"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("model") == "SHARP"))
  }

  "environment.system/os" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("system_os"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("name") == "Windows_NT"))
  }

  "environment.system/os windows fields" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("system_os"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Int]("windows_build_number") == 10586))
    records.foreach(x => assert(x.getAs[Int]("windows_ubr") == 446))
    records.foreach(x => assert(x.getAs[Int]("install_year") == 2016))
  }

  "environment.system/hdd" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("system_hdd"))
    assert(records.length == fixture.payloads.length)
    records.foreach { x =>
      val p = x.getAs[Row]("profile")
      assert(p.getAs[String]("revision") == "12345")
    }
  }

  "environment.system/gfx" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("system_gfx"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val a = x.getList[Row](x.fieldIndex("adapters"))(0)
      assert(a.getAs[Int]("ram") == 1024)
    }
  }

  "environment.settings" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("settings"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Boolean]("e10s_enabled")))
  }

  "environment.addons.activeAddons" must "be converted correctly" in {
    val records = fixture.row.getList[Map[String, Row]](fixture.row.fieldIndex("active_addons"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val addon = x.get("jid0-edalmuivkozlouyij0lpdx548bc@jetpack").get
      assert(addon.getAs[String]("name") == "geckoprofiler")
      assert(addon.getAs[Boolean]("is_system"))
    }
  }

  "environment.addons.theme" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("theme"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("description") == "The default theme."))
  }

  "environment.addons.activePlugins" must "be converted correctly" in {
    val records = fixture.row.getList[WrappedArray[Row]](fixture.row.fieldIndex("active_plugins"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      assert(!x(0).getAs[Boolean]("blocklisted"))
    }
  }

  "environment.addons.activeGMPlugins" must "be converted correctly" in {
    val records = fixture.row.getList[Map[String, Row]](fixture.row.fieldIndex("active_gmp_plugins"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val plugin = x.get("gmp-eme-adobe").get
      assert(plugin.getAs[Int]("apply_background_updates") == 1)
    }
  }

  "environment.addons.activeExperiment" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("active_experiment"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("id") == "A"))
  }

  "payload.simpleMeasurements" must "be converted correctly" in {
    val records = fixture.row.getList[Row](fixture.row.fieldIndex("simple_measurements"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Long]("uptime") == 18))
  }

  "Flag histograms" must "be converted correctly" in {
    val histograms = fixture.row.getList[Boolean](fixture.row.fieldIndex("telemetry_test_flag"))
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(x => assert(x))
  }

  "Boolean histograms" must "be converted correctly" in {
    val histograms = fixture.row.getList[WrappedArray[Long]](fixture.row.fieldIndex("devtools_webide_connection_result"))
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(x => assert(x.toList == List(42, 0)))
  }

  "Count histograms" must "be converted correctly" in {
    val histograms = fixture.row.getList[Int](fixture.row.fieldIndex("update_check_no_update_external"))
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(x => assert(x == 42))
  }

  "Enumerated histograms" must "be converted correctly" in {
    val histograms = fixture.row.getList[WrappedArray[Int]](fixture.row.fieldIndex("places_backups_daysfromlast"))
    assert(histograms.length == fixture.payloads.length)

    for (h <- histograms) {
      assert(h.length == 16)

      for ((value, key) <- h.zipWithIndex) {
        if (key == 1)
          assert(value == 42)
        else
          assert(value == 0)
      }
    }
  }

  "Linear histograms" must "be converted correctly" in {
    val histograms = fixture.row.getList[Row](fixture.row.fieldIndex("gc_budget_ms"))
    assert(histograms.length == fixture.payloads.length)

    val reference = List(0, 42, 0, 0, 0, 0, 0, 0, 0, 0)
    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == 42L)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference)
    }
  }

  "Exponential histograms" must "be converted correctly" in {
    val histograms = fixture.row.getList[Row](fixture.row.fieldIndex("gc_ms"))
    assert(histograms.length == fixture.payloads.length)

    val reference = Array.fill(50){0}
    reference(1) = 42

    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == 42L)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference.toList)
    }
  }

  "Keyed enumerated histograms" must "be converted correctly" in {
    val entries = fixture.row.getMap[String, WrappedArray[WrappedArray[Int]]](fixture.row.fieldIndex("addon_shim_usage"))
    assert(entries.size == 1)

    for (h <- entries("foo")) {
      assert(h.length == 16)

      for ((value, key) <- h.zipWithIndex) {
        if (key == 1)
          assert(value == 42)
        else
          assert(value == 0)
      }
    }
  }

  "Keyed count histograms" must "be converted correctly" in {
    val entries = fixture.row.getMap[String, WrappedArray[Int]](fixture.row.fieldIndex("search_counts"))
    assert(entries.size == 1)
    assert(entries("foo").size == fixture.payloads.length)
    entries("foo").foreach(x => assert(x == 42))
  }

  "Keyed exponential histograms" must "be converted correctly" in {
    val entries = fixture.row.getMap[String, WrappedArray[Row]](fixture.row.fieldIndex("devtools_perftools_selected_view_ms"))
    assert(entries.size == 1)

    val histograms = entries("foo")
    assert(histograms.length == fixture.payloads.length)

    val reference = Array.fill(20){0}
    reference(1) = 42

    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == 42L)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference.toList)
    }
  }

  "ClientIterator" should "not trim histories of size < 1000" in {
    val template = ("foo", Map("client" -> "foo"))
    val history = List.fill(42)(template)
    val split_history = new ClientIterator(history.iterator).toList
    assert(split_history.length == 1)
    split_history(0).length === 42
  }

  it should "not trim histories of size 1000" in {
    val template = ("foo", Map("client" -> "foo"))
    val history = List.fill(1000)(template)
    val split_history = new ClientIterator(history.iterator).toList
    assert(split_history.length == 1)
    split_history(0).length === 1000
  }

  it should "trim histories of size > 1000" in {
    val template1 = ("foo", Map("client" -> "foo"))
    val template2 = ("bar", Map("client" -> "bar"))
    val history = List.fill(2000)(template1) ++ List.fill(2000)(template2)
    val split_history = new ClientIterator(history.iterator).toList
    assert(split_history.length == 2)
    split_history.map(x => assert(x.length == 1000))
  }

  "Unsigned scalars" must "be converted correctly" in {
    val scalars = fixture.row.getList[Long](fixture.row.fieldIndex("scalar_parent_mock_scalar_uint"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach {
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain the default value for it.
      case (x, index) => if (index == 0) assert(x == 0) else assert(x == 3)
    }
  }

  "Boolean scalars" must "be converted correctly" in {
    val scalars = fixture.row.getList[Boolean](fixture.row.fieldIndex("scalar_parent_mock_scalar_bool"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach {
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain the default value for it.
      case (x, index) => if (index == 0) assert(x == false) else assert(x == true)
    }
  }

  "String scalars" must "be converted correctly" in {
    val scalars = fixture.row.getList[String](fixture.row.fieldIndex("scalar_parent_mock_scalar_string"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach {
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain the default value for it.
      case (x, index) => if (index == 0) assert(x == "") else assert(x == "a nice string scalar")
    }
  }

  "Keyed unsigned scalars" must "be converted correctly" in {
    val entries =
      fixture.row.getMap[String, WrappedArray[Long]](fixture.row.fieldIndex("scalar_parent_mock_keyed_scalar_uint"))
    assert(entries.size == 2)
    assert(entries("a_key").size == fixture.payloads.length)
    entries("a_key").foreach(x => assert(x == 37))
    assert(entries("second_key").size == fixture.payloads.length)
    entries("second_key").foreach(x => assert(x == 42))
  }

  "Keyed boolean scalars" must "be converted correctly" in {
    val entries =
      fixture.row.getMap[String, WrappedArray[Boolean]](fixture.row.fieldIndex("scalar_parent_mock_keyed_scalar_bool"))
    assert(entries.size == 2)
    assert(entries("foo").size == fixture.payloads.length)
    entries("foo").foreach(x => assert(x == true))
    assert(entries("bar").size == fixture.payloads.length)
    entries("bar").foreach(x => assert(x == false))
  }

  "Keyed string scalars" must "be converted correctly" in {
    val entries =
      fixture.row.getMap[String, WrappedArray[String]](fixture.row.fieldIndex("scalar_parent_mock_keyed_scalar_string"))
    assert(entries.size == 2)
    assert(entries("fizz").size == fixture.payloads.length)
    entries("fizz").foreach(x => assert(x == "buzz"))
    assert(entries("other").size == fixture.payloads.length)
    entries("other").foreach(x => assert(x == "some"))
  }

  "Test scalars" must "not be adedd to the dataset" in {
    intercept[IllegalArgumentException] {
      fixture.row.fieldIndex("scalar_parent_telemetry_test_unsigned_int_kind")
    }
  }
}
