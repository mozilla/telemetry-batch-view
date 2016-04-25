package telemetry.test

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.{SparkConf, SparkContext, Accumulator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester, BeforeAndAfterAll}
import telemetry.views.CrashAggregateView
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericData, GenericRecordBuilder}
import org.apache.avro.generic.GenericData.Record
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import telemetry.parquet.ParquetFile

class CrashAggregateViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val pingDimensions = List(
    ("submission_date",   List("20160305", "20160607")),
    ("activity_date",     List(1456906203503000000.0, 1464768617492000000.0)),
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
    ("e10s_cohort",       List("control", "test"))
  )

  var sc: Option[SparkContext] = None
  var sqlContext: Option[SQLContext] = None

  override def beforeAll(configMap: org.scalatest.ConfigMap) {
    // set up and configure Spark
    val sparkConf = new SparkConf().setAppName("KPI")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    sc = Some(new SparkContext(sparkConf))
    sqlContext = Some(new SQLContext(sc.get))
  }

  override def afterAll(configMap: org.scalatest.ConfigMap) {
    sc.get.stop()
  }

  def fixture = {
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
          ("gmplugin" ->
            ("bucket_count" -> 3) ~
            ("histogram_type" -> 4) ~
            ("range" -> List(1, 2)) ~
            ("sum" -> SCALAR_VALUE) ~
            ("values" -> Map("0" -> SCALAR_VALUE, "1" -> 0))
          )
        )
      val info =
        ("subsessionLength" -> SCALAR_VALUE)
      val system =
        ("os" ->
          ("name" -> dimensions("os_name").asInstanceOf[String]) ~
          ("version" -> dimensions("os_version").asInstanceOf[String])
        )
      val settings =
        ("e10sEnabled" -> dimensions("e10s").asInstanceOf[Boolean]) ~
        ("e10sCohort" -> dimensions("e10s_cohort").asInstanceOf[String])
      val build =
        ("version" -> dimensions("build_version").asInstanceOf[String]) ~
        ("buildId" -> dimensions("build_id").asInstanceOf[String]) ~
        ("architecture" -> dimensions("architecture").asInstanceOf[String])
      val addons =
        ("activeExperiment" ->
          ("id" -> dimensions("experiment_id").asInstanceOf[String]) ~
          ("branch" -> dimensions("experiment_branch").asInstanceOf[String])
        )
      implicit val formats = DefaultFormats
      Map(
        "creationTimestamp" -> dimensions("activity_date").asInstanceOf[Double],
        "submissionDate" -> dimensions("submission_date").asInstanceOf[String],
        "docType" -> dimensions("doc_type").asInstanceOf[String],
        "geoCountry" -> dimensions("country").asInstanceOf[String],
        "normalizedChannel" -> dimensions("channel").asInstanceOf[String],
        "appName" -> dimensions("application").asInstanceOf[String],
        "payload.keyedHistograms" -> compact(render(keyedHistograms)),
        "payload.info" -> compact(render(info)),
        "environment.system" -> compact(render(system)),
        "environment.settings" -> compact(render(settings)),
        "environment.build" -> compact(render(build)),
        "environment.addons" -> compact(render(addons))
      )
    }

    new {
      val pings: List[Map[String, Any]] = (for (configuration <- cartesianProduct(pingDimensions)) yield createPing(configuration)).toList

      val (
        rowRDD,
        mainProcessedAccumulator, mainIgnoredAccumulator,
        crashProcessedAccumulator, crashIgnoredAccumulator
      ) = CrashAggregateView.compareCrashes(sc.get, sc.get.parallelize(pings))
      val schema = CrashAggregateView.buildSchema()
      val records = sqlContext.get.createDataFrame(rowRDD, schema)
      records.count() // Spark is pretty lazy; kick it so it'll update our accumulators properly
    }
  }

  "Records" must "have the correct lengths" in {
    // the number of aggregates is half of the number of pings originally - this is because pings vary all their dimensions, including the doc type
    // when the doc type is "crash", the ping gets treated the same as if it was a "main" ping that also contains a main process crash
    // we basically "fold" the doc type dimension into the aggregates
    assert(fixture.records.count() == fixture.pings.length / 2)
    assert(fixture.mainProcessedAccumulator.value == fixture.pings.length / 2)
    assert(fixture.mainIgnoredAccumulator.value == 0)
    assert(fixture.crashProcessedAccumulator.value == fixture.pings.length / 2)
    assert(fixture.crashIgnoredAccumulator.value == 0)
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
      assert(dimensionValues("e10s_cohort")       contains dimensions.getOrElse("e10s_cohort", null))
    }
  }

  "crash rates" must "be converted correctly" in {
    for (row <- fixture.records.select("stats").collect()) {
      val stats = row.getJavaMap[String, Double](0)
      assert(stats("ping_count")               == 2)
      assert(stats("usage_hours")              == 42 * 2 / 3600.0)
      assert(stats("main_crashes")             == 1)
      assert(stats("content_crashes")          == 42 * 2)
      assert(stats("plugin_crashes")           == 42 * 2)
      assert(stats("gmplugin_crashes")         == 42 * 2)
      assert(stats("usage_hours_squared")      == 0.00027222222222222226)
      assert(stats("main_crashes_squared")     == 1)
      assert(stats("content_crashes_squared")  == 3528)
      assert(stats("plugin_crashes_squared")   == 3528)
      assert(stats("gmplugin_crashes_squared") == 3528)
    }
  }
}
