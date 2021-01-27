/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils

import org.apache.hadoop.io.compress.{BZip2Codec}
import org.apache.hadoop.conf.Configuration
import java.time.{LocalDate, OffsetDateTime, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import org.scalatest.{FlatSpec, Matchers}

class UtilsTest extends FlatSpec with Matchers {

  private def parseUTCDateTime(ds: String): OffsetDateTime = {
    OffsetDateTime.parse(ds, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

  "camelize" should "camelize strings" in {
    camelize("") should be ("")
    camelize("foo") should be ("foo")
    camelize("foo_bar") should be ("fooBar")
  }

  "uncamelize" should "uncamelize strings" in {
    uncamelize("") should be ("")
    uncamelize("foo") should be ("foo")
    uncamelize("fooBar") should be ("foo_bar")
  }

  "normalizeISOTimestamp" should "handle valid timestamps" in {
    val cases = Seq(
      "2018-09-01T08:00:00.0-08:00",
      "2018-09-01T08:00:00.0+00:00",
      "2018-09-01T08:00:00.0+08:00",
      "2018-09-01T08:00:00.0-12:00",
      "2018-09-01T08:00:00.0+14:00")

    for (datestring <- cases) {
      val expect = parseUTCDateTime(datestring)
      val actual = parseUTCDateTime(normalizeISOTimestamp(datestring))

      actual should be (expect)
    }
  }

  it should "wrap the timezone" in {
    val cases = Seq(
      ("2018-09-01T08:00:00.0-08:00", ZoneOffset.ofHours(-8)),
      ("2018-09-01T08:00:00.0-00:00", ZoneOffset.ofHours(0)),
      ("2018-09-01T08:00:00.0-12:00", ZoneOffset.ofHours(-12)),
      ("2018-09-01T08:00:00.0-13:00", ZoneOffset.ofHours(-1)),
      ("2018-09-01T08:00:00.0+14:00", ZoneOffset.ofHours(14)),
      ("2018-09-01T08:00:00.0+15:00", ZoneOffset.ofHours(3)))

    for ((datestring, zone) <- cases) {
      val expect = zone
      val actual = parseUTCDateTime(normalizeISOTimestamp(datestring)).getOffset()

      actual should be (expect)
    }
  }

  "normalizeYYYYMMDDTimestamp" should "handle valid dates" in {
    normalizeYYYYMMDDTimestamp("20180901") should be ("2018-09-01T00:00:00Z")
  }

  // scalastyle:off
  it should "throw exception on invalid dates" in {
    a [DateTimeParseException] should be thrownBy normalizeYYYYMMDDTimestamp("2018901")
    a [DateTimeParseException] should be thrownBy normalizeYYYYMMDDTimestamp("201809")
    a [DateTimeParseException] should be thrownBy normalizeYYYYMMDDTimestamp("2018-09-01")
  }
  // scalastyle:on

  "normalizeEpochTimestamp" should "handle valid timestamps" in {
    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val expect = LocalDate.parse("2018-09-01", format).atStartOfDay(ZoneOffset.UTC)
    normalizeEpochTimestamp(expect.toInstant.toEpochMilli) == expect.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

  "getOrCreateSparkSession" should "add extra configs correctly" in {
    val spark = getOrCreateSparkSession("Test Job",
      extraConfigs = Map("hello" -> "world","firefox" -> "rocks"))

    spark.conf.get("hello") should be ("world")
    spark.conf.get("firefox") should be ("rocks")
  }

  "writeTextFile" should "be readable by hadoopRead with compression" in {
    val testStr = "{\"test\": 123}"
    val path = "./test.json.bz"
    val compressionCodec = new BZip2Codec()
    compressionCodec.setConf(new Configuration())

    writeTextFile(path, testStr, Some(compressionCodec))
    val result = hadoopRead(path, Some(compressionCodec))

    result should be (testStr)
  }


}
