/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.dau

import java.util.UUID.randomUUID

import com.holdenkarau.spark.testing.Utils.createTempDir
import com.mozilla.spark.sql.hyperloglog.functions.hllCreate
import org.apache.spark.sql.{functions=>F}

class DesktopDauViewTest extends GenericDauTraitTest {
  private val tempDir = createTempDir().toString

  "main" must "generate correct dau" in {
    import spark.implicits._
    spark.udf.register("hll_create", hllCreate _)
    val mau = (2 to 28).sum
    val smoothed_dau = (22 to 28).sum/7.0
    testGenericDauTraitMain(
      DesktopDauView,
      args = Array(
        "--to", "20180128",
        "--bucket", tempDir,
        "--bucket-protocol", ""
      ),
      // don't include day 1 to ensure we can handle missing days
      input = (2 to 28).flatMap{ d =>
        // d clients per day, to ensure mau and smoothed_dau work properly
        (1 to d).map{_=>(f"201801$d%02d", randomUUID.toString)}
      }
        .toDF("submission_date", "hll")
        .withColumn("hll", F.expr("hll_create(hll, 12)")),
      expect = List(
        (28, mau, smoothed_dau, smoothed_dau/mau, "20180128")
      ).toDF("dau", "mau", "smoothed_dau", "er", "submission_date_s3")
    )
  }
}
