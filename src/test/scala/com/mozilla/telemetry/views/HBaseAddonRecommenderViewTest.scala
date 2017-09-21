package com.mozilla.telemetry.views

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.tagobjects.Slow
import unicredit.spark.hbase._

case class TestMainSummaryPing(client_id: Option[String],
                               subsession_start_date: Option[String],
                               channel: String,
                               city: Option[String] = None,
                               locale: Option[String] = None,
                               os: Option[String] = None,
                               places_bookmarks_count: Option[Long] = None,
                               scalar_parent_browser_engagement_tab_open_event_count: Option[Long] = None,
                               scalar_parent_browser_engagement_total_uri_count: Option[Long] = None,
                               scalar_parent_browser_engagement_unique_domains_count: Option[Long] = None,
                               active_addons: Option[String] = None,
                               disabled_addons_ids: Option[String] = None)

class HBaseAddonRecommenderViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val tableName = HBaseAddonRecommenderView.tableName
  val columnFamily = HBaseAddonRecommenderView.columnFamily
  val column = HBaseAddonRecommenderView.column

  implicit lazy val hbaseConfig = HBaseConfig("hbase.fs.tmp.dir" -> "/tmp/hbase-test")
  lazy val admin = new HBaseAdmin(hbaseConfig.get)

  implicit lazy val spark = SparkSession.builder().master("local[*]").appName("HBaseAddonRecommenderView").getOrCreate()
  import spark.implicits._

  val now = DateTime.now
  val start_date_iso = now.toString()
  val start_date_abbrev = now.toString("YYYYMMdd")

  "The ETL job" should "create a HBase table if one doesn't exist" taggedAs(Slow) in {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    HBaseAddonRecommenderView.createHBaseTable(useCompression = false)
    assert(admin.tableExists(tableName))
  }

  it should "not fail if the HBase table already exists" taggedAs(Slow) in {
    HBaseAddonRecommenderView.createHBaseTable(useCompression = false)
  }

  it should "load main summary pings into HBase" taggedAs(Slow) in {
    // The dataset contains data for a single user. The first two entries are legit,
    // with the first being the most recent (and thus the one making it to the HBase
    // table). The other ones are corrupted and won't make it to the table.
    val dataset = Seq(
      TestMainSummaryPing(Some("foo_id"), Some(start_date_iso), "release",
        Some("Rome"), Some("IT_it"), Some("Windows"), Some(1), Some(2), Some(3),
        Some(4), Some("active-addon-test"), Some("disabled-addon-test")),
      TestMainSummaryPing(Some("foo_id"), Some(now.minusMonths(4).toString), "release"),
      TestMainSummaryPing(Some("foo_broken_id"), Some("corrupted"), "release"),
      TestMainSummaryPing(None, Some("corrupted"), "release"))
      .toDS()

    dataset.createOrReplaceTempView("main_summary")
    HBaseAddonRecommenderView.etl(DateTime.now(), DateTime.now(), date => spark.sql("select * from main_summary"))

    val rdd = spark.sparkContext.hbase[String](tableName, Map(columnFamily -> Set(column)))
    val table = rdd.collect().toList

    // We just expect one entry in the HBase table.
    assert(table.size == 1)
    assert(table(0)._1 == s"foo_id")

    // Validate the payload of the expected entry.
    assert(table(0)._2("cf")("payload") ==
      s"""{"city":"Rome",
         |"subsession_start_date":"$start_date_iso",
         |"locale":"IT_it",
         |"os":"Windows",
         |"places_bookmarks_count":1,
         |"scalar_parent_browser_engagement_tab_open_event_count":2,
         |"scalar_parent_browser_engagement_total_uri_count":3,
         |"scalar_parent_browser_engagement_unique_domains_count":4,
         |"active_addons":"active-addon-test",
         |"disabled_addons_ids":"disabled-addon-test"}""".stripMargin.filter(_ >= ' '))
  }

  it should "overwrite entries when backfilling" taggedAs(Slow) in {
    val dataset = Seq(
      TestMainSummaryPing(Some("foo_id"), Some(start_date_iso), "release",
        Some("Naples"), Some("IT_it"), Some("Windows")))
      .toDS()

    dataset.createOrReplaceTempView("main_summary")
    HBaseAddonRecommenderView.etl(DateTime.now(), DateTime.now(), date => spark.sql("select * from main_summary"))

    val rdd = spark.sparkContext.hbase[String](tableName, Map(columnFamily -> Set(column)))
    val table = rdd.collect().toList

    assert(table.size == 1)
    assert(table(0)._1 == s"foo_id")
    assert(table(0)._2("cf")("payload") ==
      s"""{"city":"Naples","subsession_start_date":"$start_date_iso","locale":"IT_it","os":"Windows"}""")
  }

  override def afterAll(): Unit = {
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
    spark.stop()
  }
}
