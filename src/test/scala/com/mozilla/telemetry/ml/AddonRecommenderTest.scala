/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.ml

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.Map


private case class TestAddonData(addon_id: String,
                                 app_disabled: Boolean = false,
                                 blocklisted: Boolean = false,
                                 foreign_install: Boolean = false,
                                 has_binary_components: Boolean = false,
                                 install_day: Long = 1,
                                 is_system: Boolean = false,
                                 is_web_extension: Boolean = true,
                                 multiprocess_compatible: Boolean = true,
                                 name: String = "addon name",
                                 scope: Long = 1,
                                 signed_state: Long = 2,
                                 `type`: String = "extension",
                                 update_day: Long = 1,
                                 user_disabled: Boolean = false,
                                 version: String = "1.0")

class AddonRecommenderTest extends FlatSpec with Matchers with DataFrameSuiteBase with BeforeAndAfterAll {
  private var amoDB: Map[String, AMOAddonInfo] = Map[String, AMOAddonInfo]()

  override def beforeAll() {
    super.beforeAll()

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
    super.afterAll()
    Files.deleteIfExists(AMODatabase.getLocalCachePath())
  }

  "hash" must "return positive hash codes" in {
    val testString = "test-string"
    assert(testString.hashCode() < 0)
    assert(AddonRecommender.hash(testString) > 0)
  }

  "getAddonData" must "filter undesired addons" in {
    import spark.implicits._
    val dataset = List(
      // This user has an add-on which is not on AMO and a webextension one.
      clientsDailyRow("foo_id",
        Array(
          TestAddonData("{unlisted-addon-guid}", is_web_extension = false),
          TestAddonData("{webext-addon-guid}")
        )
      ),
      // User with a legacy add-on and a blacklisted one.
      clientsDailyRow("legacy_user_id",
        Array(
          TestAddonData("{legacy-addon-guid}", is_web_extension = false),
          TestAddonData("{blacklisted-addon-guid}")
        )
      ),
      // User with a series of disabled add-ons.
      clientsDailyRow("disabled_user_id",
        Array(
          TestAddonData("{legacy-addon-guid}", is_web_extension = false, user_disabled = true),
          TestAddonData("{webext-addon-guid}", app_disabled = true)
        )
      )
    ).toDS()

    dataset.createOrReplaceTempView("clients_daily")

    // Filter the dataframe, collect and validate the results.
    val df = AddonRecommender.getAddonData(spark, List("{webext-addon-guid}", "{legacy-addon-guid}"), amoDB, "clients_daily", "20190101", 100)

    val data = df.collect()
    assert(data.length == 2)

    // Check the first expected user.
    assert(data(0)._1 == "foo_id")
    assert(data(0)._2 == "{webext-addon-guid}")
    assert(data(0)._3 == AddonRecommender.hash("foo_id"))
    assert(data(0)._4 == AddonRecommender.hash("{webext-addon-guid}"))

    // Check the second expected user.
    assert(data(1)._1 == "legacy_user_id")
    assert(data(1)._2 == "{legacy-addon-guid}")
    assert(data(1)._3 == AddonRecommender.hash("legacy_user_id"))
    assert(data(1)._4 == AddonRecommender.hash("{legacy-addon-guid}"))
  }

  private def clientsDailyRow(clientId: String, activeAddons: Array[TestAddonData]): ClientsDailyRow = {
    implicit def toOption[T](any: T) = Option(any)
    val addons = activeAddons.map { case TestAddonData(addon_id, app_disabled, blocklisted, foreign_install,
    has_binary_components, install_day, is_system, is_web_extension, multiprocess_compatible, name, scope,
    signed_state, _type, update_day, user_disabled, version) =>
      ActiveAddon(addon_id, app_disabled, blocklisted, foreign_install,
        has_binary_components, install_day, is_system, is_web_extension, multiprocess_compatible, name, scope,
        signed_state, _type, update_day, user_disabled, version)
    }
    ClientsDailyRow(clientId, "Firefox", addons, "release", "1", "20190110")
  }
}
