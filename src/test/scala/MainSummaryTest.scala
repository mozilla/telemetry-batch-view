package telemetry.test

import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import utils.TelemetryUtils

class MainSummaryTest extends FlatSpec with Matchers{
  val testPayload = """
{
 "environment": {
  "addons": {
   "activeAddons": {
    "addon 1": {
      "blocklisted": false,
      "description": "First example addon.",
      "name": "Example 1",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16861,
      "updateDay": 16875,
      "isSystem": true
    },
    "addon 2": {
      "blocklisted": false,
      "description": "Second example addon.",
      "name": "Example 2",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16862,
      "updateDay": 16880,
      "isSystem": false
    },
    "addon 3": {
      "blocklisted": false,
      "description": "Third example addon.",
      "name": "Example 3",
      "userDisabled": false,
      "appDisabled": false,
      "version": "1.0",
      "scope": 1,
      "type": "extension",
      "foreignInstall": false,
      "hasBinaryComponents": false,
      "installDay": 16865,
      "updateDay": 16890,
      "isSystem": false
    }
   },
   "activePlugins": [
    {
     "name": "Default Browser Helper",
     "version": "601",
     "description": "Provides information about the default web browser",
     "blocklisted": false,
     "disabled": false,
     "clicktoplay": true,
     "mimeTypes": ["application/apple-default-browser"],
     "updateDay": 16780
    },
    {
     "name": "Java Applet Plug-in",
     "version": "Java 8 Update 73 build 02",
     "description": "Displays Java applet content, or a placeholder if Java is not installed.",
     "blocklisted": false,
     "disabled": false,
     "clicktoplay": true,
     "mimeTypes": [
      "application/x-java-applet;jpi-version=1.8.0_73",
      "application/x-java-applet;version=1.5"
     ],
     "updateDay": 16829
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 1",
     "version": "19.0.0.226"
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 2",
     "version": "19.0.0.225"
    },
    {
     "name": "Shockwave Flash",
     "description": "Example Flash 3",
     "version": "9.9.9.227"
    }
   ]
  }
 },
 "payload": {
  "emptyKey": {}
 }
}
"""
  "A json object's keys" can "be counted" in {
    val json = parse(testPayload)

    TelemetryUtils.countKeys(json \ "environment" \ "addons" \ "activeAddons").get should be (3)
    TelemetryUtils.countKeys(json).get should be (2)
    TelemetryUtils.countKeys(json \ "payload").get should be (1)
    TelemetryUtils.countKeys(json \ "payload" \ "emptyKey").get should be (0)
    TelemetryUtils.countKeys(json \ "dummy") should be (None)
  }

  "Latest flash version" can "be extracted" in {
    val json = parse(testPayload)

    TelemetryUtils.getFlashVersion(json \ "environment" \ "addons").get should be ("19.0.0.226")
    TelemetryUtils.getFlashVersion(json \ "environment") should be (None)
  }

  "Flash versions" can "be compared" in {
    TelemetryUtils.compareFlashVersions(Some("1.2.3.4"), Some("1.2.3.4")).get should be (0)
    TelemetryUtils.compareFlashVersions(Some("1.2.3.5"), Some("1.2.3.4")).get should be (1)
    TelemetryUtils.compareFlashVersions(Some("1.2.3.4"), Some("1.2.3.5")).get should be (-1)
    TelemetryUtils.compareFlashVersions(Some("10.2.3.5"), Some("9.3.4.8")).get should be (1)
    TelemetryUtils.compareFlashVersions(Some("foo"), Some("1.2.3.4")).get should be (1)
    TelemetryUtils.compareFlashVersions(Some("1.2.3.4"), Some("foo")).get should be (-1)
    TelemetryUtils.compareFlashVersions(Some("foo"), Some("bar")) should be (None)
    // Equal but bogus values are equal (for efficiency).
    TelemetryUtils.compareFlashVersions(Some("foo"), Some("foo")).get should be (0)

    // Something > Nothing
    TelemetryUtils.compareFlashVersions(Some("1.2.3.5"), None).get should be (1)
    TelemetryUtils.compareFlashVersions(None, Some("1.2.3.5")).get should be (-1)
  }
}
