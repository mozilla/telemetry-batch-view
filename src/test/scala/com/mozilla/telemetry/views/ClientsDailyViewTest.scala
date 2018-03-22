package com.mozilla.telemetry.views

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class ClientsDailyViewTest extends FlatSpec with Matchers {
  private val spark = SparkSession
    .builder()
    .appName("ClientsDailyViewTest")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()

  import spark.implicits._

  "aggregates" must "aggregate properly" in {
    ClientsDailyViewTestPayloads.genericTests.foreach { pair =>
      val (table, expect) = pair
      val aggregated = ClientsDailyView.extractDayAggregates(table.toDF)
      expect.foreach { pair =>
        (pair._1, aggregated.selectExpr(pair._1).collect.last(0)) should be (pair)
      }
    }
  }
}
