/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.pioneer

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => f}
import org.rogach.scallop.ScallopConf

import scala.annotation.tailrec
import scala.util.Try


/**
  * This job was created due to https://github.com/mozilla/pioneer-study-online-news-2/issues/32
  * As it's a one-off job, I'm opting not to write tests in lieu of manual verification (which I've done,
  * and the output looks good to me.)
  */
object PioneerOnlineNewsDedupeView {
  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    // TODO: change to s3 bucket/keys
    val inputLocation = opt[String]("input", descr = "Source for parquet data", required = true)
    val outputLocation = opt[String]("output", descr = "Destination for parquet data", required = true)
    val s3Location = opt[String]("s3location", descr = "Location of any previously deduped data on s3", required = false)
    val tempLocation = opt[String]("templocation", descr = "Temp output location", required = false, default=Some("tmp/pioneer_online_news_dedupe_working/"))
    val startDate = opt[String]("startDate", descr = "Date to start deduping from (defaults to date)", required = false)
    val date = opt[String]("date", descr = "End date for this job (defaults to yesterday)", required = false)
    val cutoffTimestamp = opt[Int]("cutoff", descr = "Epoch timestamp before which we'll ignore entries", required = false)
    verify()
  }

  case class EntryKey(pioneer_id: String, entry_timestamp: Long, branch: String, details: String, url: String)
  case class ExplodedEntry(ping_timestamp: Long, document_id: String, pioneer_id: String, study_name: String,
                           geo_city: String, geo_country: String, submission_date_s3: String, entry_timestamp: Long,
                           branch: String, details: String, url: String) {
    def toKey: EntryKey = EntryKey(pioneer_id, entry_timestamp, branch, details, url)
  }

  val jobName = "PioneerOnlineNewsDedupe"
  val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val sparkConf = new SparkConf().setAppName(jobName)
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
  // Note these are tuned for c3.4xlarge and the memory characteristics of this job
  sparkConf.set("spark.executor.memory", "20G")
  sparkConf.set("spark.memory.storageFraction", "0.6")
  sparkConf.set("spark.memory.fraction", "0.6")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  def explodeDay(submissionDate: String, inputLocation: String, tempLocation: String, cutoff: Option[Int]): Unit = {
    // the original data schema is a ping with some metadata, and an array of log entries. This step explodes that out
    // to an entry-per-row dataset in the format of the `ExplodedEntry` case class above

    val formatted = spark.read.parquet(inputLocation)
      .where(s"submission_date_s3=$submissionDate")
      .selectExpr("metadata.timestamp as ping_timestamp",
        "metadata.document_id as document_id",
        "metadata.pioneer_id as pioneer_id",
        "metadata.study_name as study_name",
        "metadata.geo_city as geo_city",
        "metadata.geo_country as geo_country",
        "explode(entries) as entry",
        "submission_date_s3"
      )
      .withColumn("entry_timestamp", f.col("entry.timestamp"))
      .withColumn("branch", f.col("entry.branch"))
      .withColumn("details", f.col("entry.details"))
      .withColumn("url", f.col("entry.url"))
      // Entries with a null branch are assumed to be from the v1 study
      .where("branch is not null")
      .drop("entry")

    val filtered = cutoff match {
      case Some(ts) => formatted.where(s"entry_timestamp > $ts")
      case _ => formatted
    }

    filtered.write
      .mode("overwrite")
      .parquet(s"$tempLocation")
  }

  @tailrec final def dedupeByDay(submissionDate: String, endDate: String, conf: Conf): Unit = {
    if (submissionDate <= endDate) {
      import spark.implicits._

      // Get all the previously deduplicated entries
      val prevDeduped = Try(spark.read.parquet(conf.outputLocation()).as[ExplodedEntry])
        .getOrElse(spark.emptyDataset[ExplodedEntry])
      val withUnion = conf.s3Location.get match {
        case Some(l) => prevDeduped
          .union(spark.read.parquet(l).as[ExplodedEntry])
          .where(s"submission_date_s3 < $submissionDate")
        case _ => prevDeduped
          .where(s"submission_date_s3 < $submissionDate")
      }

      // Doing this explode & write before deduplication seems to speed up the job *significantly* and reduce
      // memory usage for reasons that are not entirely clear to me. See bug 1458256
      explodeDay(submissionDate, conf.inputLocation(), conf.tempLocation(), conf.cutoffTimestamp.get)
      val newEntries = spark.read.parquet(conf.tempLocation())

      // Exclude all previously deduplicated entries (left_anti join), then find the earliest occurrence of each
      // log entry that's left
      newEntries
        .join(withUnion,
          List("pioneer_id", "entry_timestamp", "branch", "details", "url"),
          "left_anti")
        .as[ExplodedEntry]
        .groupByKey(_.toKey)
        .reduceGroups((a, b) => if (a.ping_timestamp <= b.ping_timestamp) a else b)
        .map(_._2)
        .drop("submission_date_s3")
        .write
        .mode("overwrite")
        .parquet(s"${conf.outputLocation()}/submission_date_s3=$submissionDate")
      val next = LocalDate.parse(submissionDate, dateFormatter).plusDays(1L).format(dateFormatter)
      dedupeByDay(next, endDate, conf)
    }
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    val endDate = (conf.date.get orElse Some(LocalDateTime.now().format(dateFormatter))).get
    val startDate = (conf.startDate.get orElse Some(endDate)).get

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outputPath = new Path(conf.outputLocation())
    val tempPath = new Path(conf.tempLocation())
    if (fs.exists(tempPath)) fs.delete(tempPath, true)

    if (!fs.exists(outputPath)) fs.mkdirs(outputPath)

    dedupeByDay(startDate, endDate, conf)
    fs.delete(tempPath, true)
  }
}
