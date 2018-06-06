package com.mozilla.telemetry.views

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.tags.ClientsDailyBuild
import org.scalatest.{FlatSpec, Matchers}

class ClientsDailyViewTest extends FlatSpec with Matchers with DataFrameSuiteBase {

  "aggregates" must "aggregate properly" taggedAs (ClientsDailyBuild) in {
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
