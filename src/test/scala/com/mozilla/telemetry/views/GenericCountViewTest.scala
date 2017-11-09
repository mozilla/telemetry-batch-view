package com.mozilla.telemetry

import com.mozilla.telemetry.utils.UDFs._
import com.mozilla.telemetry.views.GenericCountView
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

case class Submission(client_id: String,
                      app_name: String,
                      app_version: String,
                      normalized_channel: String,
                      submission_date: String,
                      subsession_start_date: String,
                      country: String,
                      locale: String,
                      e10s_enabled: Boolean,
                      os: String,
                      os_version: String,
                      devtools_toolbox_opened_count: Int,
                      distribution_id: String)

object Submission{
  val dimensions = Map(
    "client_id" -> List("x", "y", "z", null),
    "app_name" -> List("Firefox", "Fennec"),
    "app_version" -> List("44.0"),
    "normalized_channel" -> List("release", "nightly"),
    "submission_date" -> List("20160107", "20160106"),
    "subsession_start_date" -> List("2016-03-13T00:00:00.0+01:00"),
    "country" -> List("IT", "US"),
    "locale" -> List("en-US"),
    "e10s_enabled" -> List(true, false),
    "os" -> List("Windows", "Darwin"),
    "os_version" -> List("1.0", "1.1"),
    "devtools_toolbox_opened_count" -> List(0, 42),
    "distribution_id" -> List("canonical", "MozillaOnline", "yandex", "foo", "bar"))

  def randomList: List[Submission] = {
    for {
      clientId <- dimensions("client_id")
      appName <- dimensions("app_name")
      appVersion <- dimensions("app_version")
      normalizedChannel <- dimensions("normalized_channel")
      submissionDate <- dimensions("submission_date")
      subsessionStartDate <- dimensions("subsession_start_date")
      country <- dimensions("country")
      locale <- dimensions("locale")
      e10sEnabled <- dimensions("e10s_enabled")
      os <- dimensions("os")
      osVersion <- dimensions("os_version")
      devtoolsToolboxOpenedCount <- dimensions("devtools_toolbox_opened_count")
      distributionId <- dimensions("distribution_id")
    } yield {
      Submission(clientId.asInstanceOf[String],
                 appName.asInstanceOf[String],
                 appVersion.asInstanceOf[String],
                 normalizedChannel.asInstanceOf[String],
                 submissionDate.asInstanceOf[String],
                 subsessionStartDate.asInstanceOf[String],
                 country.asInstanceOf[String],
                 locale.asInstanceOf[String],
                 e10sEnabled.asInstanceOf[Boolean],
                 os.asInstanceOf[String],
                 osVersion.asInstanceOf[String],
                 devtoolsToolboxOpenedCount.asInstanceOf[Int],
                 distributionId.asInstanceOf[String])
    }
  }
}

class GenericCountTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val spark = SparkSession
    .builder()
    .appName("ClientCountViewTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.registerUDFs

  //setup data table
  private val tableName = "randomtablename"
  spark.sparkContext.parallelize(Submission.randomList).toDF.registerTempTable(tableName)

  //setup options
  private val base =
    "normalized_channel" ::
    "country" ::
    "locale" ::
    "app_name" ::
    "app_version" ::
    "e10s_enabled" ::
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
  private val aggregates = GenericCountView.aggregate(spark, conf)

  "Input data" can "be aggregated" in {
    val dims = Set(dimensions: _*) -- Set("client_id")
    (Set(aggregates.columns: _*) -- Set("client_id", "hll", "sum")) should be (dims)

    val estimates = aggregates.select(expr(s"$HllCardinality(hll)")).collect()
    estimates.foreach { x =>
      x(0) should be (Submission.dimensions("client_id").count(x => x != null))
    }

    val count = aggregates
      .select(col("hll"))
      .agg(HllMerge(col("hll")).as("hll"))
      .select(expr(s"$HllCardinality(hll)")).collect()

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
