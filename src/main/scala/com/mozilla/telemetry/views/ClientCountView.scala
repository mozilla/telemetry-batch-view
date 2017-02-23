package com.mozilla.telemetry.views

import com.github.nscala_time.time.Imports._
import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.rogach.scallop._

object ClientCountView {
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    verify()
  }

  private val hllMerge = new HyperLogLogMerge

  private val base = List(
    "normalized_channel",
    "country",
    "locale",
    "app_name",
    "app_version",
    "e10s_enabled",
    "e10s_cohort",
    "os",
    "os_version")

  // 12 bits corresponds to an error of 0.0163
  private val selection =
    "hll_create(client_id, 12) as hll" ::
    "substr(subsession_start_date, 0, 10) as activity_date" ::
    "devtools_toolbox_opened_count > 0 as devtools_toolbox_opened" ::
    "case when distribution_id in ('canonical', 'MozillaOnline', 'yandex') " +
      "then distribution_id else null end as top_distribution_id" ::
    base

  val dimensions =
    "activity_date" ::
    "devtools_toolbox_opened" ::
    "top_distribution_id" ::
    base

  def aggregate(frame: DataFrame): DataFrame = {
    frame
      .where("client_id IS NOT NULL")
      .selectExpr(selection:_*)
      .groupBy(dimensions.head, dimensions.tail:_*)
      .agg(hllMerge(col("hll")).as("hll"))
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)
    val fmt = DateTimeFormat.forPattern("yyyyMMdd")

    val to = conf.to.get match {
      case Some(t) => t
      case _ => fmt.print(DateTime.now.minusDays(1))
    }

    val from = conf.from.get match {
      case Some(t) => t
      case _ => fmt.print(fmt.parseDateTime(to).minusMonths(6))
    }

    val sparkConf = new SparkConf().setAppName("ClientCountView")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sqlContext.udf.register("hll_create", hllCreate _)

    val df = sqlContext.read.load("s3://telemetry-parquet/main_summary/v3")
    val subset = df.where(s"submission_date_s3 >= $from and submission_date_s3 <= $to")
    val aggregates = aggregate(subset).coalesce(32)

    aggregates.write.parquet(s"s3://${conf.outputBucket()}/client_count/v$from$to")
    sc.stop()
  }
}
