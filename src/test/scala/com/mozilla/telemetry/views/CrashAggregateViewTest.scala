package com.mozilla.telemetry

import com.mozilla.telemetry.utils.getOrCreateSparkSession
import com.mozilla.telemetry.views.CrashAggregateView
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConversions._

class CrashAggregateViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val pingDimensions = List(
    ("submission_date",   List("20160305", "20160607")),
    ("activity_date",     List("2016-03-02T00:00:00.0-03:00", "2016-06-01T00:00:00.0-03:00")),
    ("application",       List("Firefox", "Fennec")),
    ("doc_type",          List("main", "crash")),
    ("channel",           List("nightly", "aurora")),
    ("build_version",     List("45.0a1", "45")),
    ("build_id",          List("20160301000000", "20160302000000")),
    ("os_name",           List("Linux", "Windows_NT")),
    ("os_version",        List("6.1", "3.1.12")),
    ("architecture",      List("x86", "x86-64")),
    ("country",           List("US", "UK")),
    ("experiment_id",     List(null, "displayport-tuning-nightly@experiments.mozilla.org")),
    ("experiment_branch", List("control", "experiment")),
    ("e10s",              List(true, false)),
    ("gfx_compositor",    List("simple", "none", null))
  )

  var spark: SparkSession = _
  override def beforeAll(configMap: org.scalatest.ConfigMap) {
    spark = getOrCreateSparkSession("KPI")
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(configMap: org.scalatest.ConfigMap) {
    spark.stop()
  }

  lazy val fixture = {
    def cartesianProduct(dimensions: List[(String, List[Any])]): Iterable[Map[String, Any]] = {
      dimensions match {
        case Nil => List(Map[String, Any]())
        case (dimensionName, dimensionValues) :: rest =>
          dimensionValues.flatMap(dimensionValue =>
            cartesianProduct(dimensions.tail).map(
              configuration => configuration + (dimensionName -> dimensionValue)
            )
          )
      }
    }

    def createPing(dimensions: Map[String, Any]): Map[String, Any] = {
      val SCALAR_VALUE = 42
      val keyedHistograms =
        ("SUBPROCESS_CRASHES_WITH_DUMP" ->
          ("content" ->
            ("bucket_count" -> 3) ~
            ("histogram_type" -> 4) ~
            ("range" -> List(1, 2)) ~
            ("sum" -> SCALAR_VALUE) ~
            ("values" -> Map("0" -> SCALAR_VALUE, "1" -> 0))
          ) ~
          ("plugin" ->
            ("bucket_count" -> 3) ~
            ("histogram_type" -> 4) ~
            ("range" -> List(1, 2)) ~
            ("sum" -> SCALAR_VALUE) ~
            ("values" -> Map("0" -> SCALAR_VALUE, "1" -> 0))
          ) ~
          ("gpu" ->
            ("bucket_count" -> 3) ~
            ("histogram_type" -> 4) ~
            ("range" -> List(1, 2)) ~
            ("sum" -> SCALAR_VALUE) ~
            ("values" -> Map("0" -> SCALAR_VALUE, "1" -> 0))
            ) ~
          ("gmplugin" ->
            ("bucket_count" -> 3) ~
            ("histogram_type" -> 4) ~
            ("range" -> List(1, 2)) ~
            ("sum" -> SCALAR_VALUE) ~
            ("values" -> Map("0" -> SCALAR_VALUE, "1" -> 0))
          )
        ) ~
        ("SUBPROCESS_KILL_HARD" ->
          ("ShutDownKill" ->
            ("bucket_count" -> 3) ~
            ("histogram_type" -> 4) ~
            ("range" -> List(1, 2)) ~
            ("sum" -> SCALAR_VALUE) ~
            ("values" -> Map("0" -> SCALAR_VALUE, "1" -> 0))
          )
        )

      val isMain = dimensions("doc_type") == "main"
      val info = if (isMain)
        ("subsessionLength" -> JInt(SCALAR_VALUE)) ~
        ("subsessionStartDate" -> JString(dimensions("activity_date").asInstanceOf[String]))
      else JObject()
      val system =
        ("os" ->
          ("name" -> dimensions("os_name").asInstanceOf[String]) ~
          ("version" -> dimensions("os_version").asInstanceOf[String])) ~
        ("gfx" ->
          ("features" ->
            ("compositor" -> dimensions("gfx_compositor").asInstanceOf[String])))
      val settings =
        ("e10sEnabled" -> dimensions("e10s").asInstanceOf[Boolean])
      val build =
        ("version" -> dimensions("build_version").asInstanceOf[String]) ~
        ("buildId" -> dimensions("build_id").asInstanceOf[String]) ~
        ("architecture" -> dimensions("architecture").asInstanceOf[String])
      val addons =
        "activeExperiment" ->
          ("id" -> dimensions("experiment_id").asInstanceOf[String]) ~
          ("branch" -> dimensions("experiment_branch").asInstanceOf[String])
      val payload = if (isMain) "{}" else compact(render(
          "payload" ->
            ("crashDate" -> dimensions("activity_date").asInstanceOf[String].substring(0, 10)) ~
            ("processType" -> "main")))


      implicit val formats = DefaultFormats

      Map(
        "submissionDate" -> dimensions("submission_date").asInstanceOf[String],
        "docType" -> dimensions("doc_type").asInstanceOf[String],
        "geoCountry" -> dimensions("country").asInstanceOf[String],
        "normalizedChannel" -> dimensions("channel").asInstanceOf[String],
        "appName" -> dimensions("application").asInstanceOf[String],
        "payload.keyedHistograms" -> compact(render(keyedHistograms)),
        "payload.info" -> compact(render(info)),
        "payload" -> parse(payload),
        "environment.system" -> compact(render(system)),
        "environment.settings" -> compact(render(settings)),
        "environment.build" -> compact(render(build)),
        "environment.addons" -> compact(render(addons))
      )
    }

    new {
      val pings: List[Map[String, Any]] = (for (configuration <- cartesianProduct(pingDimensions)) yield createPing(configuration)).toList
      val sampleCrashPings = pings.filter(p =>
        p.get("docType") match {
          case Some("crash") => true
          case _ => false
        }
      ).take(10)
      val (
        rowRDD,
        mainProcessedAccumulator, mainIgnoredAccumulator,
        crashProcessedAccumulator, crashIgnoredAccumulator, contentCrashIgnoredAccumulator
        ) = CrashAggregateView.compareCrashes(spark.sparkContext, spark.sparkContext.parallelize(pings))
      val schema = CrashAggregateView.buildSchema()
      val records = spark.createDataFrame(rowRDD, schema)
      records.count() // Spark is pretty lazy; kick it so it'll update our accumulators properly
    }
  }

  "Records" must "have the correct lengths" in {
    // the number of aggregates is half of the number of pings originally - this is because pings vary all their dimensions, including the doc type
    // when the doc type is "crash", the ping gets treated the same as if it was a "main" ping that also contains a main process crash
    // we basically "fold" the doc type dimension into the aggregates
    // UPDATE: we also need to consider gfx_backend = [null, "none"] as a unique value, so we end up with 2 out of 3
    // possible combinations.
    assert(fixture.records.count() == fixture.pings.length / 3)
    assert(fixture.mainProcessedAccumulator.value == fixture.pings.length / 2)
    assert(fixture.mainIgnoredAccumulator.value == 0)
    assert(fixture.crashProcessedAccumulator.value == fixture.pings.length / 2)
    assert(fixture.crashIgnoredAccumulator.value == 0)
    assert(fixture.contentCrashIgnoredAccumulator.value == 0)
  }

  "activity date" must "be in a fixed set of dates" in {
    val validValues = List(
      "2016-03-02", "2016-06-01", // these are directly from the dataset
      "2016-03-05", "2016-05-31" // these are bounded to be around the submission date
    )
    for (row <- fixture.records.select("activity_date").collect()) {
      assert(validValues contains row(0))
    }
  }

  "dimensions" must "be converted correctly" in {
    val dimensionValues = pingDimensions.toMap
    for (row <- fixture.records.select("dimensions").collect()) {
      val dimensions = row.getJavaMap[String, String](0)
      assert(dimensionValues("build_version")     contains dimensions.getOrElse("build_version", null))
      assert(dimensionValues("build_id")          contains dimensions.getOrElse("build_id", null))
      assert(dimensionValues("channel")           contains dimensions.getOrElse("channel", null))
      assert(dimensionValues("application")       contains dimensions.getOrElse("application", null))
      assert(dimensionValues("os_name")           contains dimensions.getOrElse("os_name", null))
      assert(dimensionValues("os_version")        contains dimensions.getOrElse("os_version", null))
      assert(dimensionValues("architecture")      contains dimensions.getOrElse("architecture", null))
      assert(dimensionValues("country")           contains dimensions.getOrElse("country", null))
      assert(dimensionValues("experiment_id")     contains dimensions.getOrElse("experiment_id", null))
      assert(dimensionValues("experiment_branch") contains dimensions.getOrElse("experiment_branch", null))
      assert(List("True", "False")                contains dimensions.getOrElse("e10s_enabled", null))
      assert(dimensionValues("gfx_compositor")    contains dimensions.getOrElse("gfx_compositor", null))
    }
  }

  "gfx_backend" must "be either a string or null" in {
    val values = fixture.records.select("dimensions.gfx_compositor").distinct().rdd.map(r => r(0)).collect()
    assert(values.toSet == Set("simple", null))
  }

  "crash rates" must "be converted correctly" in {
    for (row <- fixture.records.select("stats", "dimensions.gfx_compositor").collect()) {
      val stats = row.getJavaMap[String, Double](0)
      val gfx_compositor = row.getString(1)
      val has_gfx_compositor = gfx_compositor match {
        case null => false
        case s: String => true
      }
      // We need a special case for gfx_compositor, since it has 2 ('none', null) values that count as one.
      // Because of that, all the stats for gfx_compositor == null need to be doubled.
      if (has_gfx_compositor) {
        assert(stats("ping_count")                       == 1)
        assert(stats("usage_hours")                      == 42 / 3600.0)
        assert(stats("main_crashes")                     == 1)
        assert(stats("content_crashes")                  == 42 * 2)
        assert(stats("plugin_crashes")                   == 42 * 2)
        assert(stats("gmplugin_crashes")                 == 42 * 2)
        assert(stats("content_shutdown_crashes")         == 42 * 2)
        assert(stats("gpu_crashes")                      == 42 * 2)
        assert(stats("usage_hours_squared")              == scala.math.pow(42 / 3600.0, 2))
        assert(stats("main_crashes_squared")             == scala.math.pow(1, 2))
        assert(stats("content_crashes_squared")          == scala.math.pow(42, 2) * 2)
        assert(stats("plugin_crashes_squared")           == scala.math.pow(42, 2) * 2)
        assert(stats("gmplugin_crashes_squared")         == scala.math.pow(42, 2) * 2)
        assert(stats("content_shutdown_crashes_squared") == scala.math.pow(42, 2) * 2)
        assert(stats("gpu_crashes_squared")              == scala.math.pow(42, 2) * 2)
      } else {
        assert(stats("ping_count")                       == 2)
        assert(stats("usage_hours")                      == (42 / 3600.0) * 2)
        assert(stats("main_crashes")                     == 1 * 2)
        assert(stats("content_crashes")                  == 42 * 2 * 2)
        assert(stats("plugin_crashes")                   == 42 * 2 * 2)
        assert(stats("gmplugin_crashes")                 == 42 * 2 * 2)
        assert(stats("content_shutdown_crashes")         == 42 * 2 * 2)
        assert(stats("gpu_crashes")                      == 42 * 2 * 2)
        assert(stats("usage_hours_squared")              == scala.math.pow(42 / 3600.0, 2) * 2)
        assert(stats("main_crashes_squared")             == scala.math.pow(1, 2) * 2)
        assert(stats("content_crashes_squared")          == scala.math.pow(42, 2) * 2 * 2)
        assert(stats("plugin_crashes_squared")           == scala.math.pow(42, 2) * 2 * 2)
        assert(stats("gmplugin_crashes_squared")         == scala.math.pow(42, 2) * 2 * 2)
        assert(stats("content_shutdown_crashes_squared") == scala.math.pow(42, 2) * 2 * 2)
        assert(stats("gpu_crashes_squared")              == scala.math.pow(42, 2) * 2 * 2)
      }
    }
  }

  "content crash pings" must "be ignored"  in {
    val contentCrashPings = fixture.sampleCrashPings.map(p => {
      val payload = p.get("payload") match {
        case Some(s: JObject) => s.replace(List("payload", "processType"), "content")
        case _ =>
      }
      p.updated("payload", payload)
    })
    val (
      rowRDD,
      mainProcessedAccumulator, mainIgnoredAccumulator,
      crashProcessedAccumulator, crashIgnoredAccumulator, contentCrashIgnoredAccumulator
      ) = CrashAggregateView.compareCrashes(spark.sparkContext, spark.sparkContext.parallelize(contentCrashPings))
    rowRDD.collect()
    assert(crashProcessedAccumulator.value == 0)
    assert(contentCrashIgnoredAccumulator.value == contentCrashPings.length)
  }

  "main crash pings" must "not be ignored"  in {
    val browserCrashPings = fixture.sampleCrashPings.map(
      p => p + ("processType" -> "main")
    )
    val (
      rowRDD,
      mainProcessedAccumulator, mainIgnoredAccumulator,
      crashProcessedAccumulator, crashIgnoredAccumulator, contentCrashIgnoredAccumulator
      ) = CrashAggregateView.compareCrashes(spark.sparkContext, spark.sparkContext.parallelize(browserCrashPings))
    rowRDD.collect()
    assert(crashProcessedAccumulator.value == 10)
    assert(contentCrashIgnoredAccumulator.value == 0)
  }

  "old pings" must "not be ignored"  in {
    val oldPings = fixture.sampleCrashPings.take(10)
    val (
      rowRDD,
      mainProcessedAccumulator, mainIgnoredAccumulator,
      crashProcessedAccumulator, crashIgnoredAccumulator, contentCrashIgnoredAccumulator
      ) = CrashAggregateView.compareCrashes(spark.sparkContext, spark.sparkContext.parallelize(oldPings))
    rowRDD.collect()
    assert(crashProcessedAccumulator.value == 10)
    assert(contentCrashIgnoredAccumulator.value == 0)
  }
}
