/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

class ClientsDailyViewTest extends FlatSpec with Matchers with DataFrameSuiteBase {

  "aggregates" must "aggregate properly" in {
    import spark.implicits._
    ClientsDailyViewTestPayloads.genericTests.foreach { pair =>
      val (table, expect) = pair
      val aggregated = ClientsDailyView.extractDayAggregates(table.toDF)
      expect.foreach { pair =>
        (pair._1, aggregated.selectExpr(pair._1).collect.last(0)) should be (pair)
      }
    }
  }
}
