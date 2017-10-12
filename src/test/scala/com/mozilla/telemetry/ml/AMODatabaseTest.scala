package com.mozilla.telemetry.ml

import java.nio.file.Files

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import org.json4s.MappingException
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class AMODatabaseTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  val Port = 3785
  val Host = "localhost"
  val wireMockServer = new WireMockServer(wireMockConfig().port(Port))

  override def beforeEach {
    wireMockServer.start()
    WireMock.configureFor(Host, Port)
  }

  override def afterEach {
    wireMockServer.stop()
    // Delete the test cache.
    Files.deleteIfExists(AMODatabase.getLocalCachePath())
  }

  "AMODatabase" must "fail with incorrect data" in {
    val path = "/api/v3/addons/search/"
    // Stub the API and return the sample JSON response when its hit.
    stubFor(get(urlMatching(path + "\\?.*"))
      .withQueryParam("app", equalTo("firefox"))
      .withQueryParam("sort", equalTo("created"))
      .withQueryParam("type", equalTo("extension"))
      .willReturn(okJson("{\"result\": {}}")))

    // Point the AMODatabase to the stub server.
    intercept[MappingException] {
      AMODatabase.init(s"http://$Host:$Port$path")
    }

    // Using any function should not throw an exception.
    val unknownAddonId = "{unknown-guid}"
    assert(!AMODatabase.contains(unknownAddonId))
    assert(AMODatabase.getAddonNameById(unknownAddonId).isEmpty)
    assert(AMODatabase.isWebextension(unknownAddonId).isEmpty)
  }

  "AMODatabase" must "parse responses correctly" in {
    val path = "/api/v3/addons/search/"
    val sampleResponse =
      """
        |{
        | "previous": null,
        | "next": null,
        | "results": [
        |   {
        |    "guid": "{addon-guid}",
        |    "categories": {
        |      "firefox": ["cat1", "cat2"]
        |    },
        |    "default_locale": "en-UK",
        |    "description": "desc",
        |    "name": {
        |      "en-UK": "Nice addon name"
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
        |    "tags": ["tag1", "tag2"],
        |    "weekly_downloads": 12
        |   }
        | ]
        |}
      """.stripMargin

    // Stub the API and return the sample JSON response when its hit.
    stubFor(get(urlMatching(path + "\\?.*"))
      .withQueryParam("app", equalTo("firefox"))
      .withQueryParam("sort", equalTo("created"))
      .withQueryParam("type", equalTo("extension"))
      .willReturn(okJson(sampleResponse)))

    // Point the AMODatabase to the stub server.
    AMODatabase.init(s"http://$Host:$Port$path")

    // Verify that the response is correctly parsed and that the expected
    // addon is returned.
    val existingAddonId = "{addon-guid}"
    assert(AMODatabase.contains(existingAddonId))
    assert(AMODatabase.getAddonNameById(existingAddonId).contains("Nice addon name"))
    assert(AMODatabase.isWebextension(existingAddonId).contains(true))

    // Verify that the functions behave correctly for unknown addons.
    val unknownAddonId = "{unknown-guid}"
    assert(!AMODatabase.contains(unknownAddonId))
    assert(AMODatabase.getAddonNameById(unknownAddonId).isEmpty)
    assert(AMODatabase.isWebextension(unknownAddonId).isEmpty)
  }
}

