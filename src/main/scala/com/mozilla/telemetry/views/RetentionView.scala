package com.mozilla.telemetry.views


import com.mozilla.telemetry.utils.UDFs._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.rogach.scallop._


object RetentionView {
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val date = opt[String]("date", descr = "Run date for this job", required = true)
    val input = opt[String]("input", descr = "Source for parquet data", required = true)
    val bucket = opt[String]("bucket", descr = "output bucket", required = true)
    val prefix = opt[String]("prefix",
      descr = "output prefix",
      required = false,
      default = Some("retention/v1")
    )
    val hllBits = opt[Int](
      "hll-bits",
      descr = "Number of bits to use for hll. 13 bits is 8192 bytes with an error of 0.0115. Defaults to 13.",
      required = false,
      default = Some(13)
    )
    verify()
  }

  val dimensions: List[String] = List(
    "subsession_start",
    "profile_creation",
    "days_since_creation",
    "channel",
    "app_version",
    "geo",
    "distribution_id",
    "is_funnelcake",
    "source",
    "medium",
    "content",
    "sync_usage",
    "is_active"
  )

  val metrics: List[String] = List(
    "usage_hours",
    "sum_squared_usage_hours",
    "total_uri_count",
    "unique_domains_count"
  )


  def transform(dataframe: DataFrame, hllBits: Int): DataFrame = {
    val expr = List(s"hll_create(client_id, $hllBits) as hll") ++ dimensions ++ metrics

    dataframe.selectExpr(expr:_*)
      .groupBy(dimensions.head, dimensions.tail:_*)
      .agg(
        HllMerge(col("hll")).as("hll"),
        sum("usage_hours").as("usage_hours"),
        sum("sum_squared_usage_hours").as("sum_squared_usage_hours"),
        sum("total_uri_count").as("total_uri_count"),
        avg("unique_domains_count").as("unique_domains_count")
      )
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .appName(s"Retention")
      .getOrCreate()

    spark.registerUDFs

    val date = conf.date()
    val df = spark.read.parquet(conf.input())

    val result = transform(df, conf.hllBits())

    result
      .write
      .mode("overwrite")
      .parquet(s"s3://${conf.bucket()}/${conf.prefix()}/start_date=${conf.date()}")

    spark.stop()
  }
}
