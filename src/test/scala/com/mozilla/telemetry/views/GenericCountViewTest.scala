package com.mozilla.telemetry

import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._
import com.mozilla.telemetry.views.{ClientCountView, GenericCountView}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class GenericCountTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val spark = SparkSession
    .builder()
    .appName("ClientCountViewTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.udf.register("hll_cardinality", hllCardinality _)
  private val sc = spark.sparkContext

  //setup data table
  private val tableName = "randomtablename"
  sc.parallelize(Submission.randomList).toDF.registerTempTable(tableName)

  //setup options
  private val base =
    "normalized_channel" ::
    "country" ::
    "locale" ::
    "app_name" ::
    "app_version" ::
    "e10s_enabled" ::
    "e10s_cohort" ::
    "os" ::
    "os_version" :: Nil

  private val select =
    "substr(subsession_start_date, 0, 10) as activity_date" ::
    "devtools_toolbox_opened_count > 0 as devtools_toolbox_opened" ::
    "case when distribution_id in ('canonical', 'MozillaOnline', 'yandex') " +
      "then distribution_id else null end as top_distribution_id" :: base

  private val dimensions =
    "activity_date" ::
    "devtools_toolbox_opened" ::
    "top_distribution_id" :: base

  private val args =
    "--from" :: "20160101" ::
    "--to" :: "20170101" ::
    "--tablename" :: tableName ::
    "--submission-date-col" :: "submission_date" ::
    "--count-column" :: "client_id" ::
    "--select" :: select.mkString(",") ::
    "--grouping-columns" :: dimensions.mkString(",") ::
    "--where" :: "client_id IS NOT NULL" ::
    "--output" :: "telemetry-test-bucket/client_count" :: Nil

  // make client count dataset
  val conf = new GenericCountView.Conf(args.toArray)
  private val aggregates = GenericCountView.aggregate(sc, conf)

  "Input data" can "be aggregated" in {
    val dimensions = Set(ClientCountView.dimensions: _*) -- Set("client_id")
    (Set(aggregates.columns: _*) -- Set("client_id", "hll", "sum")) should be (dimensions)

    val estimates = aggregates.select(expr("hll_cardinality(hll)")).collect()
    estimates.foreach { x =>
      x(0) should be (Submission.dimensions("client_id").count(x => x != null))
    }

    val hllMerge = new HyperLogLogMerge
    val count = aggregates
      .select(col("hll"))
      .agg(hllMerge(col("hll")).as("hll"))
      .select(expr("hll_cardinality(hll)")).collect()

    count.length should be (1)
    count(0)(0) should be (Submission.dimensions("client_id").count(x => x != null))
  }

  "Only top distributions" should "be considered" in {
    val distributionIdCount = aggregates
      .groupBy("top_distribution_id")
      .agg(countDistinct($"top_distribution_id"))
      .collect()

    distributionIdCount.length should be (4)
  }

  override def afterAll() = {
    spark.stop()
  }
}
