package com.mozilla.telemetry.ml

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, PrivateMethodTester}
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.Dataset

import scala.collection.Map

private case class TestBuildData(application_name: Option[String])
private case class TestActiveAddonData(blocklisted: Option[Boolean],
                                       description: Option[String],
                                       name: Option[String],
                                       user_disabled: Option[Boolean],
                                       app_disabled: Option[Boolean],
                                       version: Option[String],
                                       scope: Option[Int],
                                       `type`: Option[String],
                                       foreign_install: Option[Boolean],
                                       has_binary_components: Option[Boolean],
                                       install_day: Option[Long],
                                       update_day: Option[Long],
                                       signed_state: Option[Int],
                                       is_system: Option[Boolean])
private case class TestLongitudinalRow(client_id: Option[String],
                                       active_addons: Option[Seq[Map[String, TestActiveAddonData]]],
                                       build: Option[Seq[TestBuildData]])

class AddonRecommenderTest extends FlatSpec with BeforeAndAfterAll with Matchers with PrivateMethodTester{
  private var amoDB: Map[String, AMOAddonInfo] = Map[String, AMOAddonInfo]()
  private val spark = getOrCreateSparkSession("AddonRecommenderTest", enableHiveSupport = true)
  import spark.implicits._

  override def beforeAll() {
    // Generate a sample AMO database cache file and save it.
    val sampleAMODatabase =
      """
        |{
        | "{webext-addon-guid}":
        |   {
        |    "guid": "{webext-addon-guid}",
        |    "categories": {
        |      "firefox": ["cat1"]
        |    },
        |    "default_locale": "en-UK",
        |    "description": "desc",
        |    "name": {
        |      "en-UK": "A modern add-on"
        |    },
        |    "current_version": {
        |      "files": [
        |        {
        |          "id": 123,
        |          "platform": "all",
        |          "status": "public",
        |          "is_webextension": true
        |        }
        |      ]
        |    },
        |    "ratings": {
        |      "bayesian_average": 0.0
        |    },
        |    "summary": {
        |      "en-UK": "A summary description"
        |    },
        |    "tags": [],
        |    "weekly_downloads": 1
        |   },
        | "{legacy-addon-guid}":
        |   {
        |    "guid": "{legacy-addon-guid}",
        |    "categories": {
        |      "firefox": ["cat1"]
        |    },
        |    "default_locale": "en-UK",
        |    "description": "desc",
        |    "name": {
        |      "en-UK": "A legacy add-on"
        |    },
        |    "current_version": {
        |      "files": [
        |        {
        |          "id": 123,
        |          "platform": "all",
        |          "status": "public",
        |          "is_webextension": false
        |        }
        |      ]
        |    },
        |    "ratings": {
        |      "bayesian_average": 0.0
        |    },
        |    "summary": {
        |      "en-UK": "A summary description"
        |    },
        |    "tags": [],
        |    "weekly_downloads": 1
        |   },
        | "{blacklisted-addon-guid}":
        |   {
        |    "guid": "{blacklisted-addon-guid}",
        |    "categories": {
        |      "firefox": ["cat1"]
        |    },
        |    "default_locale": "en-UK",
        |    "description": "desc",
        |    "name": {
        |      "en-UK": "A blacklisted add-on"
        |    },
        |    "current_version": {
        |      "files": [
        |        {
        |          "id": 123,
        |          "platform": "all",
        |          "status": "public",
        |          "is_webextension": true
        |        }
        |      ]
        |    },
        |    "ratings": {
        |      "bayesian_average": 0.0
        |    },
        |    "summary": {
        |      "en-UK": "A summary description"
        |    },
        |    "tags": [],
        |    "weekly_downloads": 1
        |   }
        |}
      """.stripMargin

    // Write the cache file.
    val cacheFile = AMODatabase.getLocalCachePath()
    Files.write(cacheFile, sampleAMODatabase.getBytes(StandardCharsets.UTF_8))

    // Init the AMO Database.
    amoDB = AMODatabase.getAddonMap()
  }

  override def afterAll(): Unit = {
    Files.deleteIfExists(AMODatabase.getLocalCachePath())
    spark.stop()
  }

  "hash" must "return positive hash codes" in {
    val hash = PrivateMethod[Int]('hash)

    val testString = "test-string"
    assert(testString.hashCode() < 0)
    assert((AddonRecommender invokePrivate hash(testString)) > 0)
  }

  "getAddonData" must "filter undesired addons" in {
    val dataset = List(
      // This user has an add-on which is not on AMO and a webextension one.
      TestLongitudinalRow(Some("foo_id"),
        Some(List(Map(
          "{unlisted-addon-guid}" -> TestActiveAddonData(
            Some(false), Some("Nice desc"), Some("addon name"), Some(false), Some(false), Some("1.0"),
            Some(1), Some("extension"), Some(false), Some(false), Some(1), Some(1), Some(2), Some(true)),
          "{webext-addon-guid}" -> TestActiveAddonData(
            Some(false), Some("Nice desc"), Some("addon name"), Some(false), Some(false), Some("1.0"),
            Some(1), Some("extension"), Some(false), Some(false), Some(1), Some(1), Some(2), Some(true))))
        ),
        Some(List(TestBuildData(Some("Firefox"))))
      ),
      // User with a legacy add-on and a blacklisted one.
      TestLongitudinalRow(Some("legacy_user_id"),
        Some(List(Map(
          "{legacy-addon-guid}" -> TestActiveAddonData(
            Some(false), Some("Nice desc"), Some("addon name"), Some(false), Some(false), Some("1.0"),
            Some(1), Some("extension"), Some(false), Some(false), Some(1), Some(1), Some(2), Some(true)),
          "{blacklisted-addon-guid}" -> TestActiveAddonData(
            Some(false), Some("Nice desc"), Some("addon name"), Some(false), Some(false), Some("1.0"),
            Some(1), Some("extension"), Some(false), Some(false), Some(1), Some(1), Some(2), Some(true))))
        ),
        Some(List(TestBuildData(Some("Firefox"))))
      ),
      // User with a series of disabled add-ons.
      TestLongitudinalRow(Some("disabled_user_id"),
        Some(List(Map(
          "{legacy-addon-guid}" -> TestActiveAddonData(
            Some(false), Some("Nice desc"), Some("addon name"), Some(true), Some(false), Some("1.0"),
            Some(1), Some("extension"), Some(false), Some(false), Some(1), Some(1), Some(2), Some(true)),
          "{webext-addon-guid}" -> TestActiveAddonData(
            Some(false), Some("Nice desc"), Some("addon name"), Some(false), Some(true), Some("1.0"),
            Some(1), Some("extension"), Some(false), Some(false), Some(1), Some(1), Some(2), Some(true))))
        ),
        Some(List(TestBuildData(Some("Firefox"))))
      )
    ).toDS()

    dataset.createOrReplaceTempView("longitudinal")

    // Declare the private methods from AddonRecommender so that we can call them.
    val getAddonData = PrivateMethod[Dataset[(String, String, Int, Int)]]('getAddonData)
    val hash = PrivateMethod[Int]('hash)

    // Filter the dataframe, collect and validate the results.
    val df = AddonRecommender invokePrivate getAddonData(spark, List("{blacklisted-addon-guid}"), amoDB)

    val data = df.collect()
    assert(data.length == 2)

    // Check the first expected user.
    assert(data(0)._1 == "foo_id")
    assert(data(0)._2 == "{webext-addon-guid}")
    assert(data(0)._3 == (AddonRecommender invokePrivate hash("foo_id")))
    assert(data(0)._4 == (AddonRecommender invokePrivate hash("{webext-addon-guid}")))

    // Check the second expected user.
    assert(data(1)._1 == "legacy_user_id")
    assert(data(1)._2 == "{legacy-addon-guid}")
    assert(data(1)._3 == (AddonRecommender invokePrivate hash("legacy_user_id")))
    assert(data(1)._4 == (AddonRecommender invokePrivate hash("{legacy-addon-guid}")))
  }
}
