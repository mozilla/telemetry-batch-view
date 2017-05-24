package com.mozilla.telemetry.views
import java.io.PrintWriter

import com.mozilla.telemetry.parquet.ParquetFile
import com.mozilla.telemetry.scalars.ScalarsClass
import com.mozilla.telemetry.histograms.HistogramsClass
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray
import scala.io.Source

import org.apache.avro.io.EncoderFactory
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import java.io.ByteArrayOutputStream

class LongitudinalTest extends FlatSpec with Matchers with PrivateMethodTester {
  val parentConstant = 42
  val contentConstant = 24
  val gpuConstant = 17

  val fixture = {
    def createPayload(idx: Int): Map[String, Any] = {
      val histograms = (constant: Int) => {
        ("FIPS_ENABLED" ->
          ("values" -> ("0" -> 0)) ~
          ("sum" -> 0)) ~
        ("BROWSER_IS_USER_DEFAULT" ->
          ("values" -> ("0" -> constant)) ~
          ("sum" -> 0)) ~
        ("PUSH_API_NOTIFY" ->
          ("values" -> ("0" -> constant)) ~
          ("sum" -> 0)) ~
        ("GFX_CRASH" ->
          ("values" -> ("1" -> constant)) ~
          ("sum" -> constant)) ~
        ("SEARCH_SERVICE_ENGINE_COUNT" ->
          ("values" -> ("1" -> constant)) ~
          ("sum" -> constant)) ~
        ("FX_TAB_SWITCH_TOTAL_MS" ->
          ("values" -> ("1" -> constant)) ~
          ("sum" -> constant)) ~
        ("GC_MS" ->
          ("values" -> ("1" -> constant)) ~
          ("sum" -> constant)) ~
        ("MOCK_OPTIN" ->
          ("values" -> ("1" -> constant)) ~
          ("sum" -> constant)) ~
        ("MOCK_OPTOUT" ->
          ("values" -> ("1" -> constant)) ~
          ("sum" -> constant))
      }

      val keyedHistograms = (constant: Int) => {
        ("FX_MIGRATION_ERRORS" ->
          ("foo" ->
            ("values" -> ("1" -> constant)) ~
            ("sum" -> constant))) ~
        ("SEARCH_COUNTS" ->
          ("foo" ->
            ("values" -> ("0" -> constant)) ~
            ("sum" -> constant))) ~
        ("FX_MIGRATION_LOGINS_IMPORT_MS" ->
          ("foo" ->
            ("values" -> ("1" -> constant)) ~
            ("sum" -> constant)))
      }

      val scalars = (constant: Int) => {
        ("telemetry.test.unsigned_int_kind" -> constant ) ~
        ("mock.scalar.uint" -> constant) ~
        ("mock.scalar.bool" -> true) ~
        ("mock.scalar.string" -> constant.toString) ~
        ("mock.uint.optin" -> constant) ~
        ("mock.uint.optout" -> constant)
      }

      val keyedScalars = (constant: Int) => {
        ("mock.keyed.scalar.uint" ->
          ("a_key" -> constant) ~
          ("second_key" -> constant)) ~
        ("mock.keyed.scalar.bool" ->
          ("foo" -> true) ~
          ("bar" -> false)) ~
        ("mock.keyed.scalar.string" ->
          ("fizz" -> constant.toString) ~
          ("other" -> constant.toString))
      }

      val parent =
        if (idx == 1) {
          // Skip the scalar section for the first payload.
          ("bogus" -> "other") ~
          ("keyedScalars" -> keyedScalars(parentConstant))
        } else {
          ("scalars" -> scalars(parentConstant)) ~
          ("keyedScalars" -> keyedScalars(parentConstant))
        }

      val pingPayload =
        ("processes" ->
          ("parent" -> parent) ~
          ("content" ->
            ("histograms" -> histograms(contentConstant)) ~
            ("keyedHistograms" -> keyedHistograms(contentConstant))
          ) ~
          ("gpu" ->
            ("histograms" -> histograms(gpuConstant)) ~
            ("keyedHistograms" -> keyedHistograms(gpuConstant))
          )
        )

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
        ("userPrefs" ->
          ("network.proxy.http" -> "proxy http") ~
          ("dom.ipc.processCount" -> 2) ~
          ("browser.zoom.full" -> false) ~
          ("extensions.blocklist.url" -> "www.test.com") ~
          ("browser.startup.homepage" -> "homepage") ~
          ("browser.startup.page" -> 4) ~
          ("privacy.trackingprotection.enabled" -> true) ~
          ("layers.prefer-opengl" -> true)
        )

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
        ("sessionStartDate" -> "2015-12-09T00:00:00.0-14:00") ~
        ("profileSubsessionCounter" -> (1000 - idx)) ~
        ("reason" -> "shutdown")

      val experiments = Map(
        "experiment1" -> Map("branch" -> "branch1"),
        "experiment2" -> Map("branch" -> "control")
      )

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
        "payload.histograms" -> compact(render(histograms(parentConstant))),
        "payload.keyedHistograms" -> compact(render(keyedHistograms(parentConstant))),
        "payload" -> compact(render(pingPayload)),
        "environment.build" -> compact(render(build)),
        "environment.partner" -> compact(render(partner)),
        "environment.profile" -> compact(render(profile)),
        "environment.settings" -> compact(render(settings)),
        "environment.system" -> compact(render(system)),
        "environment.addons" -> compact(render(addons)),
        "environment.experiments" -> compact(render(experiments)))
    }

    new {
      val scalarUrlMock = (a: String, b: String) => Source.fromFile("src/test/resources/Scalars.yaml")
      val histogramUrlMock = (a: String, b: String) => Source.fromFile("src/test/resources/ShortHistograms.json")

      val scalars =  new ScalarsClass {
        override protected val getURL = scalarUrlMock
      }

      val histograms = new HistogramsClass {
        override protected val getURL = histogramUrlMock
      }

      private val buildSchema = PrivateMethod[Schema]('buildSchema)
      private val buildRecord = PrivateMethod[Option[GenericRecord]]('buildRecord)

      val payloads = for (i <- 1 to 10) yield createPayload(i)
      private val dupes = for (i <- 1 to 10) yield createPayload(1)

      private val sparkConf = new SparkConf().setAppName("Longitudinal")
      sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))

      private val sc = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")

      private val sqlContext = new SQLContext(sc)

      // Opt-out only schema
      private val optoutHistogramDefs = histograms.definitions(includeOptin = false)
      private val optoutScalarDefs = scalars.definitions(includeOptin = false)

      private val optoutSchema = LongitudinalView invokePrivate buildSchema(optoutHistogramDefs, optoutScalarDefs, None)
      private val optoutRecord = (LongitudinalView  invokePrivate buildRecord(payloads ++ dupes, optoutSchema, optoutHistogramDefs, optoutScalarDefs, None)).get
      private val optoutPath = ParquetFile.serialize(List(optoutRecord).toIterator, optoutSchema)
      private val optoutFilename = optoutPath.toString.replace("file:", "")

      val optoutRows = sqlContext.read.load(optoutFilename).collect()
      val optoutRow =  optoutRows(0)

      // Opt-out and Opt-in schema
      private val optinHistogramDefs = histograms.definitions(includeOptin = true)
      private val optinScalarDefs = scalars.definitions(includeOptin = true)

      private val optinSchema = LongitudinalView invokePrivate buildSchema(optinHistogramDefs, optinScalarDefs, None)
      private val optinRecord = (LongitudinalView  invokePrivate buildRecord(payloads ++ dupes, optinSchema, optinHistogramDefs, optinScalarDefs, None)).get
      private val optinPath = ParquetFile.serialize(List(optinRecord).toIterator, optinSchema)
      private val optinFilename = optinPath.toString.replace("file:", "")

      val optinRows = sqlContext.read.load(optinFilename).collect()
      val optinRow =  optinRows(0)

      private val experimentsSchema = LongitudinalView invokePrivate buildSchema(optoutHistogramDefs, optoutScalarDefs, Some("experiment1"))
      private val experimentsRecord = (LongitudinalView  invokePrivate buildRecord(payloads ++ dupes, experimentsSchema, optoutHistogramDefs, optoutScalarDefs, Some("experiment1"))).get
      private val experimentsPath = ParquetFile.serialize(List(experimentsRecord).toIterator, experimentsSchema)
      private val experimentsFilename = experimentsPath.toString.replace("file:", "")

      val experimentsRows = sqlContext.read.load(experimentsFilename).collect()
      val experimentsRow =  experimentsRows(0)

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
      "session_start_date"    -> "2015-12-09T12:00:00.000-02:00",
      "profile_creation_date" -> "2014-02-21T00:00:00.000Z",
      "profile_reset_date"    -> "2014-03-03T00:00:00.000Z"
    )

    val floatFields = Array(
      "sample_id"             -> 42.0,
      "size"                  -> 93691.0
    )

    def compareFields[T](fields: Array[(String, T)]) {
      for ((key, reference) <- fields) {
        val records = fixture.optoutRow.getList[T](fixture.optoutRow.fieldIndex(key))
        assert(records.size == fixture.payloads.size)
        records.foreach(x => assert(x == reference))
      }
    }

    assert(fixture.optoutRows.length == 1)
    compareFields(stringFields)
    compareFields(floatFields)
  }

  "Top-level measurements" must "be converted correctly" in {
    assert(fixture.optoutRow.getAs[String]("client_id") == fixture.payloads(0)("clientId"))
    assert(fixture.optoutRow.getAs[String]("os") == fixture.payloads(0)("os"))
    assert(fixture.optoutRow.getAs[String]("normalized_channel") == fixture.payloads(0)("normalizedChannel"))
  }

  "payload.info" must "be converted correctly" in {
    val reasonRecords = fixture.optoutRow.getList[String](fixture.optoutRow.fieldIndex("reason"))
    assert(reasonRecords.length == fixture.payloads.length)
    reasonRecords.foreach(x => assert(x == "shutdown"))
  }

  "environment.build" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("build"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("build_id") == "20160101001100"))
  }

  "environment.partner" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("partner"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val partner_names = x.getList[String](x.fieldIndex("partner_names"))
      assert(partner_names.toList == List("A", "B", "C"))
    }
  }

  "environment.system" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("system"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Int]("memory_mb") == 2048))
  }

  "environment.system/cpu" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("system_cpu"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Int]("count") == 4))
  }

  "environment.system/device" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("system_device"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("model") == "SHARP"))
  }

  "environment.system/os" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("system_os"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("name") == "Windows_NT"))
  }

  "environment.system/os windows fields" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("system_os"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Int]("windows_build_number") == 10586))
    records.foreach(x => assert(x.getAs[Int]("windows_ubr") == 446))
    records.foreach(x => assert(x.getAs[Int]("install_year") == 2016))
  }

  "environment.system/hdd" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("system_hdd"))
    assert(records.length == fixture.payloads.length)
    records.foreach { x =>
      val p = x.getAs[Row]("profile")
      assert(p.getAs[String]("revision") == "12345")
    }
  }

  "environment.system/gfx" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("system_gfx"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val a = x.getList[Row](x.fieldIndex("adapters"))(0)
      assert(a.getAs[Int]("ram") == 1024)
    }
  }

  "environment.settings" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("settings"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Boolean]("e10s_enabled")))
  }

  "environment.settings.userPrefs" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("settings"))
    val prefs  = records.map(
      record => record.getAs[Map[String, String]](record.fieldIndex("user_prefs"))
    )
    prefs.foreach(x => assert(x("browser.zoom.full") == "false"))
    prefs.foreach(x => assert(x("browser.startup.homepage") == "homepage"))
    prefs.foreach(x => assert(x("browser.startup.page") == "4"))
  }

  "environment.addons.activeAddons" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Map[String, Row]](fixture.optoutRow.fieldIndex("active_addons"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val addon = x.get("jid0-edalmuivkozlouyij0lpdx548bc@jetpack").get
      assert(addon.getAs[String]("name") == "geckoprofiler")
      assert(addon.getAs[Boolean]("is_system"))
    }
  }

  "environment.addons.theme" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("theme"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("description") == "The default theme."))
  }

  "environment.addons.activePlugins" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[WrappedArray[Row]](fixture.optoutRow.fieldIndex("active_plugins"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      assert(!x(0).getAs[Boolean]("blocklisted"))
    }
  }

  "environment.addons.activeGMPlugins" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Map[String, Row]](fixture.optoutRow.fieldIndex("active_gmp_plugins"))
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val plugin = x.get("gmp-eme-adobe").get
      assert(plugin.getAs[Int]("apply_background_updates") == 1)
    }
  }

  "environment.addons.activeExperiment" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("active_experiment"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[String]("id") == "A"))
  }

  "payload.simpleMeasurements" must "be converted correctly" in {
    val records = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("simple_measurements"))
    assert(records.length == fixture.payloads.length)
    records.foreach(x => assert(x.getAs[Long]("uptime") == 18))
  }

  "Flag histograms" must "be converted correctly" in {
    val histograms = fixture.optoutRow.getList[Boolean](fixture.optoutRow.fieldIndex("fips_enabled"))
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(x => assert(x))
  }

  "Boolean histograms" must "be converted correctly" in {
    val histograms = fixture.optoutRow.getList[WrappedArray[Long]](fixture.optoutRow.fieldIndex("browser_is_user_default"))
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(x => assert(x.toList == List(parentConstant, 0)))
  }

  "Count histograms" must "be converted correctly" in {
    val histograms = fixture.optoutRow.getList[Int](fixture.optoutRow.fieldIndex("push_api_notify"))
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(x => assert(x == parentConstant))
  }

  "Enumerated histograms" must "be converted correctly" in {
    val histograms = fixture.optoutRow.getList[WrappedArray[Int]](fixture.optoutRow.fieldIndex("gfx_crash"))
    assert(histograms.length == fixture.payloads.length)

    for (h <- histograms) {
      assert(h.length == 101)

      for ((value, key) <- h.zipWithIndex) {
        if (key == 1)
          assert(value == parentConstant)
        else
          assert(value == 0)
      }
    }
  }

  "Linear histograms" must "be converted correctly" in {
    val histograms = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("search_service_engine_count"))
    assert(histograms.length == fixture.payloads.length)

    val reference = List(0, parentConstant) ++ List.fill(48)(0)
    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == parentConstant.toLong)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference)
    }
  }

  "Exponential histograms" must "be converted correctly" in {
    val histograms = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("fx_tab_switch_total_ms"))
    assert(histograms.length == fixture.payloads.length)

    val reference = List(0, parentConstant) ++ List.fill(18)(0)
    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == parentConstant.toLong)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference.toList)
    }
  }

  "Keyed enumerated histograms" must "be converted correctly" in {
    val entries = fixture.optoutRow.getMap[String, WrappedArray[WrappedArray[Int]]](fixture.optoutRow.fieldIndex("fx_migration_errors"))
    assert(entries.size == 1)

    for (h <- entries("foo")) {
      assert(h.length == 13)

      for ((value, key) <- h.zipWithIndex) {
        if (key == 1)
          assert(value == parentConstant)
        else
          assert(value == 0)
      }
    }
  }

  "Keyed count histograms" must "be converted correctly" in {
    val entries = fixture.optoutRow.getMap[String, WrappedArray[Int]](fixture.optoutRow.fieldIndex("search_counts"))
    assert(entries.size == 1)
    assert(entries("foo").size == fixture.payloads.length)
    entries("foo").foreach(x => assert(x == parentConstant))
  }

  "Keyed exponential histograms" must "be converted correctly" in {
    val entries = fixture.optoutRow.getMap[String, WrappedArray[Row]](fixture.optoutRow.fieldIndex("fx_migration_logins_import_ms"))
    assert(entries.size == 1)

    val histograms = entries("foo")
    assert(histograms.length == fixture.payloads.length)

    val reference = List(0, parentConstant) ++ List.fill(68)(0)
    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == parentConstant.toLong)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference.toList)
    }
  }

  "Keyed Content Histograms" must "be included" in {
    val entries = fixture.optoutRow.getMap[String, WrappedArray[Int]](fixture.optoutRow.fieldIndex("search_counts_content"))
    assert(entries.size == 1)
    assert(entries("foo").size == fixture.payloads.length)
    entries("foo").foreach(x => assert(x == contentConstant))
  }

  "Opt-in Histograms" must "be ignored for optout only" in {
    intercept[IllegalArgumentException](fixture.optoutRow.fieldIndex("mock_optin"))
  }

  "Opt-in Histograms" must "not be ignored when opt-in included" in {
    val histograms = fixture.optinRow.getList[Row](fixture.optinRow.fieldIndex("mock_optin"))
    assert(histograms.length == fixture.payloads.length)

    val reference = List(0, parentConstant) ++ List.fill(8)(0)
    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == parentConstant.toLong)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference.toList)
    }
   }

  "Opt-out Histograms" must "not be ignored when opt-in included" in {
    val histograms = fixture.optinRow.getList[Row](fixture.optinRow.fieldIndex("mock_optout"))
    assert(histograms.length == fixture.payloads.length)

    val reference = List(0, parentConstant) ++ List.fill(8)(0)
    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == parentConstant.toLong)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference.toList)
    }
   }

  "Content Histograms" must "be included" in {
    val histograms = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("mock_optout_content"))
    assert(histograms.length == fixture.payloads.length)

    val reference = List(0, contentConstant) ++ List.fill(8)(0)
    histograms.foreach{ x =>
      assert(x.getAs[Long]("sum") == contentConstant.toLong)
      assert(x.getList[Int](x.fieldIndex("values")).toList == reference.toList)
    }
  }

  "ClientIterator" should "not trim histories of size < 1000" in {
    val template = ("foo", Map("client" -> "foo"))
    val history = List.fill(parentConstant)(template)
    val split_history = new ClientIterator(history.iterator).toList
    assert(split_history.length == 1)
    split_history(0).length === parentConstant
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
    val scalars = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("scalar_parent_mock_scalar_uint"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach { case (x, index) =>
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain null for it.
      Option(x.getAs[Long]("value")) match {
        case Some(value) => assert(value == parentConstant)
        case None => assert(index == 0)
      }
    }
  }

  "Boolean scalars" must "be converted correctly" in {
    val scalars = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("scalar_parent_mock_scalar_bool"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach { case (x, index) =>
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain null for it.
      Option(x.getAs[Boolean]("value")) match {
        case Some(value) => assert(value == true)
        case None => assert(index == 0)
      }
    }
  }

  "String scalars" must "be converted correctly" in {
    val scalars = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("scalar_parent_mock_scalar_string"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach { case (x, index) =>
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain null for it.
      Option(x.getAs[String]("value")) match {
        case Some(value) => assert(value == parentConstant.toString)
        case None => assert(index == 0)
      }
    }
  }

  "Keyed unsigned scalars" must "be converted correctly" in {
    val entries =
      fixture.optoutRow.getMap[String, WrappedArray[Row]](fixture.optoutRow.fieldIndex("scalar_parent_mock_keyed_scalar_uint"))
    assert(entries.size == 2)
    assert(entries("a_key").size == fixture.payloads.length)
    entries("a_key").foreach(x => assert(x.getAs[Long]("value") == parentConstant))
    assert(entries("second_key").size == fixture.payloads.length)
    entries("second_key").foreach(x => assert(x.getAs[Long]("value") == parentConstant))
  }

  "Keyed boolean scalars" must "be converted correctly" in {
    val entries =
      fixture.optoutRow.getMap[String, WrappedArray[Row]](fixture.optoutRow.fieldIndex("scalar_parent_mock_keyed_scalar_bool"))
    assert(entries.size == 2)
    assert(entries("foo").size == fixture.payloads.length)
    entries("foo").foreach(x => assert(x.getAs[Boolean]("value") == true))
    assert(entries("bar").size == fixture.payloads.length)
    entries("bar").foreach(x => assert(x.getAs[Boolean]("value") == false))
  }

  "Keyed string scalars" must "be converted correctly" in {
    val entries =
      fixture.optoutRow.getMap[String, WrappedArray[Row]](fixture.optoutRow.fieldIndex("scalar_parent_mock_keyed_scalar_string"))
    assert(entries.size == 2)
    assert(entries("fizz").size == fixture.payloads.length)
    entries("fizz").foreach(x => assert(x.getAs[String]("value") == parentConstant.toString))
    assert(entries("other").size == fixture.payloads.length)
    entries("other").foreach(x => assert(x.getAs[String]("value") == parentConstant.toString))
  }

  "Test scalars" must "not be added to the dataset" in {
    intercept[IllegalArgumentException] {
      fixture.optoutRow.fieldIndex("scalar_parent_telemetry_test_unsigned_int_kind")
    }
  }

  "Opt-in scalars" must "not be added to opt-out dataset" in {
    intercept[IllegalArgumentException] {
      fixture.optoutRow.fieldIndex("scalar_parent_mock_uint_optin")
    }
  }

  "Opt-in scalars" must "be added to opt-in dataset" in {
    val scalars = fixture.optinRow.getList[Row](fixture.optinRow.fieldIndex("scalar_parent_mock_uint_optin"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach { case (x, index) =>
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain null for it.
      Option(x.getAs[Long]("value")) match {
        case Some(value) => assert(value == parentConstant)
        case None => assert(index == 0)
      }
    }
  }

  "Opt-out scalars" must "be added to opt-out dataset" in {
    val scalars = fixture.optoutRow.getList[Row](fixture.optoutRow.fieldIndex("scalar_parent_mock_uint_optout"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach { case (x, index) =>
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain null for it.
      Option(x.getAs[Long]("value")) match {
        case Some(value) => assert(value == parentConstant)
        case None => assert(index == 0)
      }
    }
  }

  "Opt-out scalars" must "be added to opt-in dataset" in {
    val scalars = fixture.optinRow.getList[Row](fixture.optinRow.fieldIndex("scalar_parent_mock_uint_optout"))
    assert(scalars.length == fixture.payloads.length)
    scalars.zipWithIndex.foreach { case (x, index) =>
      // The first payload in the fixture is missing the scalars section. The scalars
      // must contain null for it.
      Option(x.getAs[Long]("value")) match {
        case Some(value) => assert(value == parentConstant)
        case None => assert(index == 0)
      }
    }
  }

  "No experiments column" must "be present in normal longitudinal dataset" in {
    intercept[IllegalArgumentException] {
      fixture.optoutRow.getAs[String]("experiment_id")
    }
    intercept[IllegalArgumentException] {
      fixture.optoutRow.getAs[String]("experiment_branch")
    }
  }

  "Experiments column" must "be added to experiments dataset" in {
    assert(fixture.experimentsRow.getAs[String]("experiment_id") == "experiment1")
    assert(fixture.experimentsRow.getAs[String]("experiment_branch") == "branch1")
  }
}
