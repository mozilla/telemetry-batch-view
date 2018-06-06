/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.utils.UDFs._
import com.mozilla.telemetry.views.RetentionView
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

case class ProcessedRetentionRow(
                                  client_id: String,
                                  subsession_start: String,
                                  profile_creation: String,
                                  days_since_creation: Long,
                                  channel: String,
                                  app_version: String,
                                  geo: String,
                                  distribution_id: String,
                                  is_funnelcake: Boolean,
                                  source: String,
                                  medium: String,
                                  campaign: String,
                                  content: String,
                                  sync_usage: String,
                                  is_active: Boolean,
                                  usage_hours: Double,
                                  sum_squared_usage_hours: Double,
                                  total_uri_count: Long,
                                  unique_domains_count: Long
                                )


class RetentionViewTest extends FlatSpec with Matchers with DataFrameSuiteBase with BeforeAndAfterAll {
  val sample = ProcessedRetentionRow(
    client_id = "1",
    subsession_start = "2017-01-15",
    profile_creation = "2017-01-12",
    days_since_creation = 3,
    channel = "release",
    app_version = "57.0.0",
    geo = "US",
    distribution_id = "mozilla57",
    is_funnelcake = true,
    source = "source-value",
    medium = "medium-value",
    campaign = "campaign-value",
    content = "content-value",
    sync_usage = "multiple",
    is_active = true,
    usage_hours = 1.0,
    sum_squared_usage_hours = 1.0,
    total_uri_count = 20,
    unique_domains_count = 3
  )

  val predata = Seq(
    sample.copy(client_id = "1_dup", channel = "release"),
    sample.copy(client_id = "1_dup", channel = "release", unique_domains_count = 0),
    sample.copy(client_id = "2", channel = "release"),
    sample.copy(client_id = "3", channel = "release"),
    sample.copy(client_id = "4", channel = "beta"),
    sample.copy(client_id = "5_dup", channel = "beta"),
    sample.copy(client_id = "5_dup", channel = "beta"),
    sample.copy(client_id = "5_dup", channel = "beta"),
    sample.copy(client_id = "5_dup", channel = "beta")
  )

  // Fix the value of the number of HLL bits for this test
  val test_transform: (DataFrame) =>  DataFrame = RetentionView.transform(_: DataFrame, 13)

  "unique_domains_count" can "be counted" in {
    import spark.implicits._

    val data = predata.toDS().toDF()
    val res = test_transform(data).where(col("channel") === "release")
    // (3 pings each with with 3 unique domains) / 4 pings
    // The average is on a per-session basis as opposed to per-client
    res.head.getAs[Double]("unique_domains_count") should be(2.25)
  }

  "Client aggregates" can "be counted without duplicates" in {
    import spark.implicits._

    val data = predata.toDS().toDF()

    val grouped =
      test_transform(data)
        .groupBy("channel")
        .agg(HllMerge(col("hll")).alias("hll"))
        .select(expr(s"$HllCardinality(hll)"))

    grouped.where(col("channel") === "release").head.getLong(0) should be(3)
    grouped.where(col("channel") === "beta").head.getLong(0) should be(2)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.registerUDFs
  }
}
