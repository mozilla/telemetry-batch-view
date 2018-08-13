/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.dau

import java.util.UUID.randomUUID

import com.holdenkarau.spark.testing.Utils.createTempDir

class DesktopActiveDauViewTest extends GenericDauTraitTest {
  private val tempDir = createTempDir().toString

  "main" must "generate active dau" in {
    import spark.implicits._
    val mau = (1 to 28).filter(_ % 10 >= 5).sum
    val smoothed_dau = average((22 to 28).filter(_ % 10 >= 5))
    testGenericDauTraitMain(
      DesktopActiveDauView,
      args = Array(
        "--to", "20180128",
        "--bucket", tempDir,
        "--bucket-protocol", ""
      ),
      // don't include day 1 to ensure we can handle missing days
      input = (1 to 28).flatMap{ d =>
        // d clients per day, to ensure mau and smoothed_dau work properly
        (1 to d).map{_=>(f"201801$d%02d", randomUUID.toString, d % 10)}
      }.toDF("submission_date_s3", "client_id", "scalar_parent_browser_engagement_total_uri_count_sum"),
      expect = List(
        (28, mau, smoothed_dau, smoothed_dau/mau, "20180128")
      ).toDF("dau", "mau", "smoothed_dau", "er", "submission_date_s3")
    )
  }
}
