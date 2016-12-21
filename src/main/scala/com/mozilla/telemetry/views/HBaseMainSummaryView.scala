package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.util.RegionSplitter
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days, format}
import org.rogach.scallop._
import unicredit.spark.hbase._

object HBaseMainSummaryView {
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val hbaseMaster = opt[String]("hbase-master", descr = "IP address of the HBase master", required = true)
    verify()
  }

  private[views] val tableName = "main_summary"
  private[views] val columnFamily = "cf"
  private[views] val column = "payload"

  private[views] def createHBaseTable(useCompression: Boolean = true)(implicit config: HBaseConfig): Unit = {
    val admin = new HBaseAdmin(config.get)
    if (!admin.tableExists(tableName)) {
      /*
         About 200 GB of compressed data is generated per day. As we want to store data for 90 days we expect to
         use 18 TB in total. If we have 1000 regions then each one will serve about 18 GB, which is in the
         recommended range for the maximum region size ([1], 2.2.2.2).

         As our HBase cluster is based on m4.xlarge machines, a region server can reasonably handle about 50 regions
         ([1], 2.2.2.1), which means the cluster should ideally have 20 instances.

         [1] http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.5.3/bk_data-access/content/deploying_hbase.html
       */
      val splits = new RegionSplitter.HexStringSplit().split(1000)
      val td = new HTableDescriptor(tableName)
      val cd = new HColumnDescriptor(columnFamily)
      cd.setTimeToLive(60*60*24*90)  // 90 days retention
      cd.setMaxVersions(1)
      if (useCompression) {
        cd.setCompressionType(Compression.Algorithm.SNAPPY)
      }
      td.addFamily(cd)
      admin.createTable(td, splits)
    }
  }

  private[views] def etl(from: DateTime, to: DateTime,
                         datasetForDate: String => DataFrame)
                        (implicit config: HBaseConfig, spark: SparkSession): Unit = {
    val errorAcc = spark.sparkContext.longAccumulator("Validation errors")
    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      time {
        val currentDate = from.plusDays(offset)
        val currentDateString = currentDate.toString("yyyyMMdd")
        println(s"Processing $currentDateString...")

        val data = datasetForDate(currentDateString)
        val subset = data.select("client_id", "document_id", "subsession_start_date")
        val jsonData = data.drop("popup_notification_stats", "document_id", "client_id").toJSON.rdd

        val rdd = subset.rdd.zip(jsonData).flatMap{ case (row, json) =>
          val clientId = row.getString(0)
          val documentId = row.getString(1)
          val startDate = row.getString(2)

          try {
            assert(clientId != null && documentId != null && startDate != null)
            val dateFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()
            val parsedDate = dateFormatter.parseDateTime(startDate)
            val formattedDate = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC().print(parsedDate)
            val ts = parsedDate.getMillis
            Some((s"$clientId:$formattedDate:$documentId", Seq((json, ts))))
          } catch {
            case _: Throwable =>
              errorAcc.add(1)
              None
          }
        }

        rdd.toHBaseBulk(tableName, columnFamily, List(column))
        println(s"${errorAcc.value} validation errors encountered")
      }
    }
  }

  def main(args: Array[String]) = {
    val conf = new Conf(args)
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")

    val to = conf.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }

    val from = conf.from.get match {
      case Some(f) => fmt.parseDateTime(f)
      case _ => DateTime.now.minusDays(1)
    }

    implicit val hbaseConfig = HBaseConfig("hbase.zookeeper.quorum" -> conf.hbaseMaster())
    implicit val spark = SparkSession.builder().master("yarn").appName("HBaseMainSummaryView").getOrCreate()

    createHBaseTable()
    etl(from, to, date => spark.read.parquet(s"s3://telemetry-parquet/main_summary/v3/submission_date_s3=$date"))

    spark.stop()
  }
}
