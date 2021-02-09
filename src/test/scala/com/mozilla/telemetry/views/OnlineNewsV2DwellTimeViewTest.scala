/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.views.pioneer

import com.mozilla.telemetry.views.pioneer.OnlineNewsV2DwellTimeView.DwellTime
import org.scalatest.{FlatSpec, Matchers}

class OnlineNewsV2DwellTimeViewTest extends FlatSpec with Matchers {
  //scalastyle:off
  def defaultEntry(entry_timestamp: Long, branch: String, details: String, url: String, ping_timestamp: Long = 12345,
                   document_id: String = "doc_id", pioneer_id: String = "p_id", study_name: String = "study_1",
                   geo_city: String = "Chicago", geo_country: String = "US",
                   submission_date_s3: String = "20190408"): ExplodedEntry = {
    //scalastyle:on
    ExplodedEntry(ping_timestamp, document_id, pioneer_id, study_name, geo_city, geo_country, submission_date_s3,
      entry_timestamp, branch, details, url)
  }

  "entriesToVisits" can "create simple visits" in {
    val entries = Seq(
      defaultEntry(1554754930, "test_branch", "focus-start", "http://hello.com/test"),
      defaultEntry(1554754931, "test_branch", "focus-start", "http://hello.com/page"),
      defaultEntry(1554754932, "test_branch", "idle-start", "http://hello.com/test"),
      defaultEntry(1554754933, "test_branch", "idle-end", "http://hello.com/page"),
      defaultEntry(1554754934, "test_branch", "focus-end", "http://hello.com/test")
    )

    val actual = OnlineNewsV2DwellTimeView.entriesToVisits("p_id", entries)
    actual should equal(Seq(
      DwellTime("p_id", "test_branch", List("doc_id"), new java.sql.Date(1554754930L * 1000), "hello.com",
        1554754930L, 4L, 2L, 5, 0, entries.map(_.toLogEvent).toList)
    ))
  }

  "entriesToVisits" can "create multiple visits" in {
    val entries = Seq(
      defaultEntry(1554754930, "test_branch", "focus-start", "http://hello.com/test"),
      defaultEntry(1554754931, "test_branch", "focus-start", "http://hello.com/page"),
      defaultEntry(1554754932, "test_branch", "focus-start", "http://world.com/test"),
      defaultEntry(1554754933, "test_branch", "focus-start", "http://world.com/page"),
      defaultEntry(1554754934, "test_branch", "focus-end", "http://world.com/test")
    )

    val actual = OnlineNewsV2DwellTimeView.entriesToVisits("p_id", entries)
    actual should equal(Seq(
      DwellTime("p_id", "test_branch", List("doc_id"), new java.sql.Date(1554754930L * 1000), "hello.com",
        1554754930L, 1L, 0L, 2, 0, entries.map(_.toLogEvent).filter(_.url.contains("hello.com")).toList),
      DwellTime("p_id", "test_branch", List("doc_id"), new java.sql.Date(1554754932L * 1000), "world.com",
        1554754932L, 2L, 0L, 3, 0, entries.map(_.toLogEvent).filter(_.url.contains("world.com")).toList)
    ))
  }

  "entriesToVisits" can "parse country codes correctly" in {
    val entries = Seq(
      defaultEntry(1554754930, "test_branch", "focus-start", "http://hello.co.uk/test"),
      defaultEntry(1554754931, "test_branch", "focus-start", "http://hello.co.uk/page"),
      defaultEntry(1554754932, "test_branch", "focus-start", "http://world.co.uk/test"),
      defaultEntry(1554754933, "test_branch", "focus-start", "http://world.co.uk/page"),
      defaultEntry(1554754934, "test_branch", "focus-end", "http://world.co.uk/test")
    )

    val actual = OnlineNewsV2DwellTimeView.entriesToVisits("p_id", entries)
    actual should equal(Seq(
      DwellTime("p_id", "test_branch", List("doc_id"), new java.sql.Date(1554754930L * 1000), "hello.co.uk",
        1554754930L, 1L, 0L, 2, 0, entries.map(_.toLogEvent).filter(_.url.contains("hello.co.uk")).toList),
      DwellTime("p_id", "test_branch", List("doc_id"), new java.sql.Date(1554754932L * 1000), "world.co.uk",
        1554754932L, 2L, 0L, 3, 0, entries.map(_.toLogEvent).filter(_.url.contains("world.co.uk")).toList)
    ))
  }
}
