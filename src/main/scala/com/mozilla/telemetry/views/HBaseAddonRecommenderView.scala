package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.util.RegionSplitter
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.{DateTime, Days, format}
import org.rogach.scallop._
import unicredit.spark.hbase._

object HBaseAddonRecommenderView {
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val hbaseMaster = opt[String]("hbase-master", descr = "IP address of the HBase master", required = true)
    verify()
  }

  private[views] val tableName = "addon_recommender_view"
  private[views] val columnFamily = "cf"
  private[views] val column = "payload"

  private[views] def createHBaseTable(useCompression: Boolean = true)(implicit config: HBaseConfig): Unit = {
    val admin = new HBaseAdmin(config.get)
    if (!admin.tableExists(tableName)) {
      val splits = new RegionSplitter.HexStringSplit().split(1000)
      val td = new HTableDescriptor(tableName)
      val cd = new HColumnDescriptor(columnFamily)
      cd.setTimeToLive(60*60*24*90)  // 90 days retention
      cd.setMaxVersions(1) // Keep a single version of each document
      if (useCompression) {
        cd.setCompressionType(Compression.Algorithm.SNAPPY)
      }
      td.addFamily(cd)
      admin.createTable(td, splits)
    }
  }

  /**
    * Read and filter the input table to fill the addon HBase view.
    *
    * @param from DateTime The initial date for which to filter the dataframe.
    * @param to DateTime The ending date for which to filter the dataframe.
    * @param datasetForDate DataFrame A "Main Summary" dataframe containing the necessary columns.
    */
  private[views] def etl(from: DateTime, to: DateTime,
                         datasetForDate: String => DataFrame)
                        (implicit config: HBaseConfig, spark: SparkSession): Unit = {
    import spark.implicits._
    val errorAcc = spark.sparkContext.longAccumulator("Validation errors")
    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      time {
        val currentDate = from.plusDays(offset)
        val currentDateString = currentDate.toString("yyyyMMdd")
        println(s"Processing $currentDateString...")

        // Get the data for the desired date.
        val data = datasetForDate(currentDateString)

        // Get the most recent (client_id, subsession_start_date) tuple for each client
        // since the main_summary might contain multiple rows per client. We will use
        // it to filter out the full table with all the columns we require.
        val clientsShortlist = data
          .select(
            $"client_id",
            $"subsession_start_date",
            row_number()
              .over(Window.partitionBy("client_id")
                .orderBy(desc("subsession_start_date")))
              .alias("clientid_rank"))
          .where($"clientid_rank" === 1)
          .drop("clientid_rank")

        // Restrict the data to the columns we're interested in.
        val dataSubset = data
          .select(
            $"client_id",
            $"subsession_start_date",
            $"city",
            $"locale",
            $"os",
            $"places_bookmarks_count",
            $"scalar_parent_browser_engagement_tab_open_event_count",
            $"scalar_parent_browser_engagement_total_uri_count",
            $"scalar_parent_browser_engagement_unique_domains_count",
            $"active_addons")

        // Join the two tables: only the elements in both dataframes will make it
        // through.
        val clientsData = dataSubset
          .join(clientsShortlist, Seq("client_id", "subsession_start_date"))

        // Convert the DataFrame to JSON and get an RDD out of it.
        val subset = clientsData.select("client_id", "subsession_start_date")
        val jsonData = clientsData.select("city",
                                          "subsession_start_date",
                                          "locale",
                                          "os",
                                          "places_bookmarks_count",
                                          "scalar_parent_browser_engagement_tab_open_event_count",
                                          "scalar_parent_browser_engagement_total_uri_count",
                                          "scalar_parent_browser_engagement_unique_domains_count",
                                          "active_addons").toJSON.rdd

        // Build an RDD containing (key, document) tuples: one per client.
        val rdd = subset.rdd.zip(jsonData).flatMap{ case (row, json) =>
          val clientId = row.getString(0)
          val startDate = row.getString(1)

          try {
            assert(clientId != null && startDate != null)
            // Validate the subsession start date: parsing will throw if
            // the date format is not valid.
            val dateFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()
            dateFormatter.parseDateTime(startDate)
            Some((clientId, Seq(json)))
          } catch {
            case _: Throwable =>
              errorAcc.add(1)
              None
          }
        }

        // Bulk-send the RDD to HBase.
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
    implicit val spark = SparkSession.builder().master("yarn").appName("HBaseAddonRecommenderView").getOrCreate()

    createHBaseTable()
    etl(from, to, date => spark.read.parquet(s"s3://telemetry-parquet/main_summary/${MainSummaryView.schemaVersion}/submission_date_s3=$date"))

    spark.stop()
  }
}
