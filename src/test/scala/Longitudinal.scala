package telemetry.test

import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import telemetry.streams.Longitudinal
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericData, GenericRecordBuilder}
import org.apache.avro.generic.GenericData.Record
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import telemetry.parquet.ParquetFile

class LongitudinalTest extends FlatSpec with Matchers with PrivateMethodTester{
  def fixture = {
    def createPayload(idx: Int): Map[String, Any] = {
      // TODO: Use Scala Map and List directly?
      val histograms =
        ("TELEMETRY_TEST_FLAG" ->
           ("values" -> ("0" -> 0)) ~
           ("sum"    -> 0)) ~
        ("DEVTOOLS_TOOLBOX_OPENED_BOOLEAN" ->
           ("values" -> ("0" -> 42)) ~
           ("sum"    -> 0)) ~
        ("UPDATE_CHECK_NO_UPDATE_EXTERNAL" ->
           ("values" -> ("0" -> 42)) ~
           ("sum"    -> 42)) ~
        ("PLACES_BACKUPS_DAYSFROMLAST" ->
           ("values" -> ("1" -> 42)) ~
           ("sum"    -> 42)) ~
        ("GC_BUDGET_MS" ->
           ("values" -> ("1" -> 42)) ~
           ("sum"    -> 42)) ~
        ("GC_MS" ->
           ("values" -> ("1" -> 42)) ~
           ("sum"    -> 42))

      val keyedHistograms =
        ("ADDON_SHIM_USAGE" ->
           ("foo" ->
             ("values" -> ("1" -> 42)) ~
             ("sum"    -> 42))) ~
        ("SEARCH_COUNTS" ->
           ("foo" ->
              ("values" -> ("0" -> 42)) ~
              ("sum"    -> 42))) ~
        ("DEVTOOLS_PERFTOOLS_SELECTED_VIEW_MS" ->
           ("foo" ->
              ("values" -> ("1" -> 42)) ~
              ("sum"    -> 42)))

      val simpleMeasurements =
        ("uptime" -> 18L)

      val threadHangStats =
        List(
          ("name" -> "Gecko") ~
          ("activity" ->
            ("ranges" -> List(0, 1, 3, 7, 15)) ~
            ("values" -> ("0" -> 1) ~ ("1" -> 0)) ~
            ("sum"    -> 0)) ~
          ("hangs" ->
            List(
              ("histogram" ->
                ("ranges" -> List(0, 1, 3, 7, 15)) ~
                ("values" -> ("0" -> 1) ~ ("1" -> 0)) ~
                ("sum"    -> 0)) ~
              ("stack" -> List("A", "B", "C"))
            ))
        )

      val build =
        ("applicationId"   -> "{ec8030f7-c20a-464f-9b0e-13a3a9e97384}") ~
        ("applicationName" -> "Firefox") ~
        ("architecture"    -> "x86-64") ~
        ("buildId"         -> "20160101001100") ~
        ("version"         -> "46.0a2") ~
        ("vendor"          -> "Mozilla") ~
        ("platformVersion" -> "46.0a2") ~
        ("xpcomAbi"        -> "x86_64-gcc3")

      val partner =
        ("partnerNames" -> List("A", "B", "C"))

      val profile =
        ("creationDate" -> 16122) ~
        ("resetDate"    -> 16132)

      val settings =
        ("e10sEnabled" -> true) ~
        ("userPrefs" -> Map("browser.download.lastDir" -> JString("/home/anthony/Desktop"), "browser.startup.page" -> JInt(3)))

      val system =
          ("memoryMB" -> 2048) ~
          ("cpu" -> ("count" -> 4)) ~
          ("device" ->
             ("model" -> "SHARP")) ~
          ("os" ->
             ("name"    -> "Windows_NT") ~
             ("locale"  -> "en_US") ~
             ("version" -> "6.1")) ~
          ("hdd" ->
             ("profile" ->
                ("revision" -> "12345") ~
                ("model"    -> "SAMSUNG X"))) ~
          ("gfx" ->
             ("adapters" -> List(
                ("RAM" -> 1024) ~ ("description" -> "FOO1") ~ ("deviceID" -> "1") ~ ("vendorID" -> "Vendor1") ~ ("GPUActive" -> true),
                ("RAM" -> 1024) ~ ("description" -> "FOO2") ~ ("deviceID" -> "2") ~ ("vendorID" -> "Vendor2") ~ ("GPUActive" -> false)
              )))

      val addons =
          ("activeAddons" -> Map(
            "jid0-edalmuivkozlouyij0lpdx548bc@jetpack" ->
              ("name" -> "geckoprofiler") ~ ("version" -> "1.16.14")
          )) ~
          ("theme" ->
            ("id"          -> "{972ce4c6-7e08-4474-a285-3208198ce6fd}") ~
            ("description" -> "The default theme.")) ~
          ("activePlugins" -> List(
            ("blocklisted" -> false) ~
            ("description" -> "Adobe PDF Plug-In For Firefox and Netscape 10.1.16") ~
            ("clicktoplay" -> true)
          )) ~
          ("activeGMPlugins" -> Map(
            "gmp-eme-adobe" ->
              ("applyBackgroundUpdates" -> 1) ~ ("userDisabled" -> false),
            "gmp-gmpopenh264" ->
              ("applyBackgroundUpdates" -> 1) ~ ("userDisabled" -> false)
          )) ~
          ("activeExperiment" ->
            ("id" -> "A") ~
            ("branch" -> "B"))

      val info =
        ("subsessionStartDate"      -> "2015-12-09T00:00:00.0-14:00") ~
        ("profileSubsessionCounter" -> (1000 - idx)) ~
        ("flashVersion"             -> "19.0.0.226") ~
        ("reason"                   -> "shutdown")

      Map("clientId"                   -> "26c9d181-b95b-4af5-bb35-84ebf0da795d",
          "os"                         -> "Windows_NT",
          "normalizedChannel"          -> "aurora",
          "documentId"                 -> idx.toString,
          "submissionDate"             -> "20160128",
          "sampleId"                   -> 42.0,
          "Size"                       -> 93691.0,
          "creationTimestamp"          -> 1.45393974518300006E18,
          "geoCountry"                 -> "US",
          "geoCity"                    -> "New York",
          "DNT"                        -> "1",
          "payload.info"               -> compact(render(info)),
          "payload.simpleMeasurements" -> compact(render(simpleMeasurements)),
          "payload.histograms"         -> compact(render(histograms)),
          "payload.keyedHistograms"    -> compact(render(keyedHistograms)),
          "payload.threadHangStats"    -> compact(render(threadHangStats)),
          "environment.build"          -> compact(render(build)),
          "environment.partner"        -> compact(render(partner)),
          "environment.profile"        -> compact(render(profile)),
          "environment.settings"       -> compact(render(settings)),
          "environment.system"         -> compact(render(system)),
          "environment.addons"         -> compact(render(addons)))
    }

    new {
      private val view = Longitudinal()

      private val buildSchema = PrivateMethod[Schema]('buildSchema)
      private val buildRecord = PrivateMethod[Option[GenericRecord]]('buildRecord)

      val schema = view invokePrivate buildSchema()
      val payloads = for (i <- 1 to 10) yield createPayload(i)
      val dupes = for (i <- 1 to 10) yield createPayload(1)
      val record = (view invokePrivate buildRecord((payloads ++ dupes).toIterable, schema)).get
    }
  }

  "Records" can "be serialized" in {
    ParquetFile.serialize(List(fixture.record).toIterator, fixture.schema)
  }

  "payload.threadHangStats" must "be converted correctly" in {
    val activity = fixture.record.get("thread_hang_activity").asInstanceOf[Array[java.util.Map[String, GenericData.Record]]].toList
    assert(activity.length == fixture.payloads.length)
    activity.foreach{ x =>
      val histogram = x.get("Gecko").get("values")
      assert(histogram.asInstanceOf[Array[Int]].toList == List(1, 0, 0, 0, 0))
    }

    val hangs = fixture.record.get("thread_hang_stacks").asInstanceOf[Array[java.util.Map[String, java.util.Map[String, GenericData.Record]]]].toList
    assert(hangs.length == fixture.payloads.length)
    hangs.foreach{ x =>
      val histogram = x.get("Gecko").get("A\nB\nC").get("values")
      assert(histogram.asInstanceOf[Array[Int]].toList == List(1, 0, 0, 0, 0))
    }
  }

  "top level fields" must "be converted correctly" in {
    val fieldValues = Array(
      "submission_date"       -> "2016-01-28T00:00:00.000Z",
      "sample_id"             -> 42.0,
      "size"                  -> 93691.0,
      "geo_country"           -> "US",
      "geo_city"              -> "New York",
      "dnt_header"            -> "1",
      "subsession_start_date" -> "2015-12-09T12:00:00.000-02:00",
      "profile_creation_date" -> "2014-02-21T00:00:00.000Z",
      "profile_reset_date"    -> "2014-03-03T00:00:00.000Z"
    )
    for ((key, value) <- fieldValues) {
      val records = fixture.record.get(key).asInstanceOf[Array[Any]].toList
      assert(records.length == fixture.payloads.length)
      records.foreach{ x =>
        assert(x == value)
      }
    }
  }

  "payload.info" must "be converted correctly" in {
    val flashRecords = fixture.record.get("flash_version").asInstanceOf[Array[Any]].toList
    assert(flashRecords.length == fixture.payloads.length)
    flashRecords.foreach{ x =>
      val record = x.asInstanceOf[String]
      assert(record == "19.0.0.226")
    }

    val reasonRecords = fixture.record.get("reason").asInstanceOf[Array[Any]].toList
    assert(reasonRecords.length == fixture.payloads.length)
    reasonRecords.foreach{ x =>
      val record = x.asInstanceOf[String]
      assert(record == "shutdown")
    }
  }

  "environment.build" must "be converted correctly" in {
    val records = fixture.record.get("build").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("build_id") == "20160101001100")
    }
  }

  "environment.partner" must "be converted correctly" in {
    val records = fixture.record.get("partner").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("partner_names").asInstanceOf[Array[Any]].toList == List("A", "B", "C"))
    }
  }

  "environment.system" must "be converted correctly" in {
    val records = fixture.record.get("system").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("memory_mb") == 2048)
    }
  }

  "environment.system/cpu" must "be converted correctly" in {
    val records = fixture.record.get("system_cpu").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("count") == 4)
    }
  }

  "environment.system/device" must "be converted correctly" in {
    val records = fixture.record.get("system_device").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("model") == "SHARP")
    }
  }

  "environment.system/os" must "be converted correctly" in {
    val records = fixture.record.get("system_os").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("name") == "Windows_NT")
    }
  }

  "environment.system/hdd" must "be converted correctly" in {
    val records = fixture.record.get("system_hdd").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("profile").asInstanceOf[Record].get("revision") == "12345")
    }
  }

  "environment.system/gfx" must "be converted correctly" in {
    val records = fixture.record.get("system_gfx").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record].get("adapters").asInstanceOf[Array[Any]](0).asInstanceOf[Record]
      assert(record.get("ram") == 1024)
    }
  }

  "environment.settings" must "be converted correctly" in {
    val records = fixture.record.get("settings").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("e10s_enabled") == true)
      assert(record.get("user_prefs").asInstanceOf[java.util.Map[String, Any]].get("browser.download.lastDir") == "/home/anthony/Desktop")
      assert(record.get("user_prefs").asInstanceOf[java.util.Map[String, Any]].get("browser.startup.page") == 3)
    }
  }

  "environment.addons.activeAddons" must "be converted correctly" in {
    val records = fixture.record.get("active_addons").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[java.util.Map[String, Any]]
      assert(record.get("jid0-edalmuivkozlouyij0lpdx548bc@jetpack").asInstanceOf[Record].get("name") == "geckoprofiler")
    }
  }

  "environment.addons.theme" must "be converted correctly" in {
    val records = fixture.record.get("theme").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("description") == "The default theme.")
    }
  }

  "environment.addons.activePlugins" must "be converted correctly" in {
    val records = fixture.record.get("active_plugins").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Array[Any]](0).asInstanceOf[Record]
      assert(record.get("blocklisted") == false)
    }
  }

  "environment.addons.activeGMPlugins" must "be converted correctly" in {
    val records = fixture.record.get("active_gmp_plugins").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[java.util.Map[String, Any]]
      assert(record.get("gmp-eme-adobe").asInstanceOf[Record].get("apply_background_updates") == 1)
    }
  }

  "environment.addons.activeExperiment" must "be converted correctly" in {
    val records = fixture.record.get("active_experiment").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("id") == "A")
    }
  }

   "Top-level measurements" must "be converted correctly" in {
    assert(fixture.record.get("client_id") == fixture.payloads(0)("clientId"))
    assert(fixture.record.get("os") == fixture.payloads(0)("os"))
    assert(fixture.record.get("normalized_channel") == fixture.payloads(0)("normalizedChannel"))
  }

  "payload.simpleMeasurements" must "be converted correctly" in {
    val records = fixture.record.get("simple_measurements").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("uptime").asInstanceOf[Long] == 18)
    }
  }

  "Flag histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("telemetry_test_flag").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(true)).foreach{case (x, y) => assert(x == y)}
  }

  "Boolean histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("devtools_toolbox_opened_boolean").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(h => assert(h.asInstanceOf[Array[Int]].toList == List(42, 0)))
  }

  "Count histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("update_check_no_update_external").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(42)).foreach{case (x, y) => assert(x== y)}
  }

  "Enumerated histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("places_backups_daysfromlast").asInstanceOf[Array[Any]]
    assert(histograms.length == fixture.payloads.length)
    for(h <- histograms) {
      val histogram = h.asInstanceOf[Array[Int]]
      assert(histogram.length == 16)

      for((value, key) <- histogram.zipWithIndex) {
        if (key == 1)
          assert(value == 42)
        else
          assert(value == 0)
      }
    }
  }

  "Linear histograms" must "be converted correctly" in {
    val records = fixture.record.get("gc_budget_ms").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)

    val reference = Array(0, 42, 0, 0, 0, 0, 0, 0, 0, 0)
    records.foreach{ x =>
      val tmp = x.asInstanceOf[Record]
      assert(tmp.get("sum") == 42L)
      assert(tmp.get("values").asInstanceOf[Array[Int]].toList == reference.toList)
    }
  }

  "Exponential histograms" must "be converted correctly" in {
    val records = fixture.record.get("gc_ms").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)

    val reference = Array.fill(50){0}
    reference(1) = 42

    records.foreach{ x =>
      val tmp = x.asInstanceOf[Record]
      assert(tmp.get("sum") == 42L)
      assert(tmp.get("values").asInstanceOf[Array[Int]].toList == reference.toList)
    }
  }

  "Keyed enumerated histograms" must "be converted correctly" in {
    // Keyed boolean histograms follow a similar structure
    val records = fixture.record.get("addon_shim_usage").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
    assert(records.size == 1)

    for(h <- records("foo")) {
      val histogram = h.asInstanceOf[Array[Int]]
      assert(histogram.length == 16)

      for((value, key) <- histogram.zipWithIndex) {
        if (key == 1)
          assert(value == 42)
        else
          assert(value == 0)
      }
    }
  }

  "Keyed count histograms" must "be converted correctly" in {
    // Keyed flag histograms follow a similar structure
    val searchCounts = fixture.record.get("search_counts").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
    assert(searchCounts.size == 1)

    val histograms = searchCounts("foo")
    assert(histograms.length == fixture.payloads.length)

    histograms.zip(Stream.continually(42)).foreach{case (x, y) => assert(x== y)}
  }

  "Keyed exponential histograms" must "be converted correctly" in {
    // Keyed linear histograms follow a similar structure
    val records = fixture.record.get("devtools_perftools_selected_view_ms").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
    assert(records.size == 1)

    val histograms = records("foo")
    val reference = Array.fill(20){0}
    reference(1) = 42

    histograms.foreach{ x =>
      val tmp = x.asInstanceOf[Record]
      assert(tmp.get("sum") == 42L)
      assert(tmp.get("values").asInstanceOf[Array[Int]].toList == reference.toList)
    }
  }
}
