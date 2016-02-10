package telemetry.test

import org.json4s.JsonDSL._
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
           ("values" -> ("0" -> 1) ~ ("1" -> 0)) ~
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
        ("creationDate" -> 1453615112) ~
        ("resetDate"    -> 1454615112)

      val settings =
        ("e10sEnabled" -> true) ~
        ("userPrefs" -> Map("browser.download.lastDir" -> "/home/anthony/Desktop"))

      val system =
          ("cpu" -> ("count" -> 4)) ~
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
        ("subsessionStartDate" -> "2015-12-09T00:00:00.0-08:00") ~
        ("profileSubsessionCounter" -> (1000 - idx))

      Map("clientId"                   -> "26c9d181-b95b-4af5-bb35-84ebf0da795d",
          "os"                         -> "Windows_NT",
          "normalizedChannel"          -> "aurora",
          "documentId"                 -> idx.toString,
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
    val activity = fixture.record.get("threadHangActivity").asInstanceOf[Array[java.util.Map[String, GenericData.Record]]].toList
    assert(activity.length == fixture.payloads.length)
    activity.foreach{ x =>
      val histogram = x.get("Gecko").get("values")
      assert(histogram.asInstanceOf[Array[Int]].toList == List(1, 0, 0, 0, 0))
    }

    val hangs = fixture.record.get("threadHangStacks").asInstanceOf[Array[java.util.Map[String, java.util.Map[String, GenericData.Record]]]].toList
    assert(hangs.length == fixture.payloads.length)
    hangs.foreach{ x =>
      val histogram = x.get("Gecko").get("A\nB\nC").get("values")
      assert(histogram.asInstanceOf[Array[Int]].toList == List(1, 0, 0, 0, 0))
    }
  }

  "environment.build" must "be converted correctly" in {
    val records = fixture.record.get("build").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("buildId") == "20160101001100")
    }
  }

  "environment.profile" must "be converted correctly" in {
    val records = fixture.record.get("profile").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("creationDate") == 1453615112)
    }
  }

  "environment.partner" must "be converted correctly" in {
    val records = fixture.record.get("partner").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("partnerNames").asInstanceOf[Array[Any]].toList == List("A", "B", "C"))
    }
  }

  "environment.system" must "be converted correctly" in {
    val records = fixture.record.get("system").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("cpu").asInstanceOf[Record].get("count") == 4)
    }
  }

  "environment.settings" must "be converted correctly" in {
    val records = fixture.record.get("settings").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("e10sEnabled") == true)
    }
  }

  "environment.addons.activeAddons" must "be converted correctly" in {
    val records = fixture.record.get("activeAddons").asInstanceOf[Array[Any]].toList
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
    val records = fixture.record.get("activePlugins").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Array[Any]](0).asInstanceOf[Record]
      assert(record.get("blocklisted") == false)
    }
  }

  "environment.addons.activeGMPlugins" must "be converted correctly" in {
    val records = fixture.record.get("activeGMPlugins").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[java.util.Map[String, Any]]
      assert(record.get("gmp-eme-adobe").asInstanceOf[Record].get("applyBackgroundUpdates") == 1)
    }
  }

  "environment.addons.activeExperiment" must "be converted correctly" in {
    val records = fixture.record.get("activeExperiment").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("id") == "A")
    }
  }

   "Top-level measurements" must "be converted correctly" in {
    assert(fixture.record.get("clientId") == fixture.payloads(0)("clientId"))
    assert(fixture.record.get("os") == fixture.payloads(0)("os"))
    assert(fixture.record.get("normalizedChannel") == fixture.payloads(0)("normalizedChannel"))
  }

  "payload.simpleMeasurements" must "be converted correctly" in {
    val records = fixture.record.get("simpleMeasurements").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)
    records.foreach{ x =>
      val record = x.asInstanceOf[Record]
      assert(record.get("uptime").asInstanceOf[Long] == 18)
    }
  }

  "Flag histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("TELEMETRY_TEST_FLAG").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(true)).foreach{case (x, y) => assert(x == y)}
  }

  "Boolean histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("DEVTOOLS_TOOLBOX_OPENED_BOOLEAN").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.foreach(h => assert(h.asInstanceOf[Array[Int]].toList == List(42, 0)))
  }

  "Count histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("UPDATE_CHECK_NO_UPDATE_EXTERNAL").asInstanceOf[Array[Any]].toList
    assert(histograms.length == fixture.payloads.length)
    histograms.zip(Stream.continually(42)).foreach{case (x, y) => assert(x== y)}
  }

  "Enumerated histograms" must "be converted correctly" in {
    val histograms = fixture.record.get("PLACES_BACKUPS_DAYSFROMLAST").asInstanceOf[Array[Any]]
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
    val records = fixture.record.get("GC_BUDGET_MS").asInstanceOf[Array[Any]].toList
    assert(records.length == fixture.payloads.length)

    val reference = Array(0, 42, 0, 0, 0, 0, 0, 0, 0, 0)
    records.foreach{ x =>
      val tmp = x.asInstanceOf[Record]
      assert(tmp.get("sum") == 42L)
      assert(tmp.get("values").asInstanceOf[Array[Int]].toList == reference.toList)
    }
  }

  "Exponential histograms" must "be converted correctly" in {
    val records = fixture.record.get("GC_MS").asInstanceOf[Array[Any]].toList
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
    val records = fixture.record.get("ADDON_SHIM_USAGE").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
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
    val searchCounts = fixture.record.get("SEARCH_COUNTS").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
    assert(searchCounts.size == 1)

    val histograms = searchCounts("foo")
    assert(histograms.length == fixture.payloads.length)

    histograms.zip(Stream.continually(42)).foreach{case (x, y) => assert(x== y)}
  }

  "Keyed exponential histograms" must "be converted correctly" in {
    // Keyed linear histograms follow a similar structure
    val records = fixture.record.get("DEVTOOLS_PERFTOOLS_SELECTED_VIEW_MS").asInstanceOf[java.util.Map[String, Array[Any]]].asScala
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
