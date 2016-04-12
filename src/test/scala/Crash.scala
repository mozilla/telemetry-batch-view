package telemetry.test

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.{SparkConf, SparkContext, Accumulator}
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import telemetry.streams.Crash
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericData, GenericRecordBuilder}
import org.apache.avro.generic.GenericData.Record
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import telemetry.parquet.ParquetFile

class CrashTest extends FlatSpec with Matchers with PrivateMethodTester {
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
    ("e10s",              List(true, false))
  )

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
        ("e10sEnabled" -> dimensions("e10s").asInstanceOf[Boolean])
      val build =
        ("version" -> dimensions("build_version").asInstanceOf[String]) ~
        ("buildId" -> dimensions("build_id").asInstanceOf[String]) ~
        ("architecture" -> dimensions("architecture").asInstanceOf[String])
      val addons =
        ("activeExperiment" ->
          ("id" -> dimensions("experiment_id").asInstanceOf[String]) ~
          ("branch" -> dimensions("experiment_branch").asInstanceOf[String])
        )
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
      private val view = Crash()

      private val buildSchema = PrivateMethod[Schema]('buildSchema)
      private val compareCrashes = PrivateMethod[(RDD[GenericRecord], Accumulator[Int], Accumulator[Int])]('compareCrashes)

      val pings: List[Map[String, Any]] = (for (configuration <- cartesianProduct(pingDimensions)) yield createPing(configuration)).toList

      val sparkConf = new SparkConf().setAppName("KPI")
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
      sparkConf.registerKryoClasses(Array(classOf[org.apache.avro.generic.GenericData.Record])) // this is necessary in order to be able to transfer records between workers and the master
      sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
      val sc = new SparkContext(sparkConf)
      val (recordsRDD, processed, ignored) = view invokePrivate compareCrashes(sc, sc.parallelize(pings))
      val records = recordsRDD.collect()
      sc.stop()
      val schema = view invokePrivate buildSchema()
    }
  }

  "Records" can "be serialized" in {
    ParquetFile.serialize(fixture.records.iterator, fixture.schema)
  }

  "Records" must "have the correct lengths" in {
    assert(fixture.pings.length == 16384)
    //assert(fixture.records.length == fixture.pings.length / 2)
    assert(fixture.processed.value == fixture.pings.length)
    assert(fixture.ignored.value == 0)
  }

  "activity date" must "be in a fixed set of dates" in {
    val validValues = List(
      "2016-03-02", "2016-06-01", // these are directly from the dataset
      "2016-03-05", "2016-05-31" // these are bounded to be around the submission date
    )
    for (record <- fixture.records) {
      assert(validValues contains record.get("activity_date").toString)
    }
  }

  "dimensions" must "be converted correctly" in {
    val dimensionValues = pingDimensions.toMap
    for (record <- fixture.records) {
      val dimensionFields = record.get("dimensions").asInstanceOf[java.util.Map[org.apache.avro.util.Utf8, org.apache.avro.util.Utf8]]
      val dimensions = (for ((key, value) <- dimensionFields) yield (key.toString, value.toString)).toMap
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
    }
  }

  "crash rates" must "be converted correctly" in {
    for (record <- fixture.records) {
      val statsFields = record.get("stats").asInstanceOf[java.util.Map[org.apache.avro.util.Utf8, Double]]
      val stats = (for ((key, value) <- statsFields) yield (key.toString, value)).toMap
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
