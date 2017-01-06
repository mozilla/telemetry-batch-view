package com.mozilla.telemetry.views

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.tagobjects.Slow
import unicredit.spark.hbase._

case class MainSummaryPing(client_id: Option[String], document_id: Option[String], subsession_start_date: Option[String], channel: String)

class HBaseMainSummaryViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val tableName = HBaseMainSummaryView.tableName
  val columnFamily = HBaseMainSummaryView.columnFamily
  val column = HBaseMainSummaryView.column

  implicit lazy val hbaseConfig = HBaseConfig("hbase.fs.tmp.dir" -> "/tmp/hbase-test")
  lazy val admin = new HBaseAdmin(hbaseConfig.get)

  implicit lazy val spark = SparkSession.builder().master("local[*]").appName("HBaseMainSummaryView").getOrCreate()
  import spark.implicits._

  "The ETL job" should "create a HBase table if one doesn't exist" taggedAs(Slow) in {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    HBaseMainSummaryView.createHBaseTable(useCompression = false)
    assert(admin.tableExists(tableName))
  }

  it should "not fail if the HBase table already exists" taggedAs(Slow) in {
    HBaseMainSummaryView.createHBaseTable(useCompression = false)
  }

  it should "load main summary pings into HBase" taggedAs(Slow) in {
    val dataset = Seq(
      MainSummaryPing(Some("foo"), Some("bar"), Some("2016-12-28T00:00:00.0+00:00"), "release"),
      MainSummaryPing(Some("foo"), Some("bar"), Some("corrupted"), "release"),
      MainSummaryPing(None, Some("bar"), Some("corrupted"), "release"),
      MainSummaryPing(Some("foo"), None, Some("corrupted"), "release"))
      .toDS()

    dataset.createOrReplaceTempView("main_summary")
    HBaseMainSummaryView.etl(DateTime.now(), DateTime.now(), date => spark.sql("select * from main_summary"))

    val rdd = spark.sparkContext.hbase[String](tableName, Map(columnFamily -> Set(column)))
    val table = rdd.collect().toList

    assert(table.size == 1)
    assert(table(0)._1 == "foo:20161228:bar")
    assert(table(0)._2("cf")("payload") == """{"subsession_start_date":"2016-12-28T00:00:00.0+00:00","channel":"release"}""")
  }

  it should "overwrite entries when backfilling" taggedAs(Slow) in {
    val dataset = Seq(MainSummaryPing(Some("foo"), Some("bar"), Some("2016-12-28T00:00:00.0+00:00"), "nightly")).toDS()

    dataset.createOrReplaceTempView("main_summary")
    HBaseMainSummaryView.etl(DateTime.now(), DateTime.now(), date => spark.sql("select * from main_summary"))

    val rdd = spark.sparkContext.hbase[String](tableName, Map(columnFamily -> Set(column)))
    val table = rdd.collect().toList

    assert(table.size == 1)
    assert(table(0)._1 == "foo:20161228:bar")
    assert(table(0)._2("cf")("payload") == """{"subsession_start_date":"2016-12-28T00:00:00.0+00:00","channel":"nightly"}""")
  }

  override def afterAll(): Unit = {
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
    spark.stop()
  }
}
