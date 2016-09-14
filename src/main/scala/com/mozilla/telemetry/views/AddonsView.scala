package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils.S3Store
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days, format}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.rogach.scallop._

import scala.util.Try

case class AddonData(blocklisted: Option[Boolean],
                     description: Option[String],
                     name: Option[String],
                     userDisabled: Option[Boolean],
                     appDisabled: Option[Boolean],
                     version: Option[String],
                     scope: Option[Integer],
                     `type`: Option[String],
                     foreignInstall: Option[Boolean],
                     hasBinaryComponents: Option[Boolean],
                     installDay: Option[Integer],
                     updateDay: Option[Integer],
                     signedState: Option[Integer],
                     isSystem: Option[Boolean])

object AddonsView {
  def streamVersion: String = "v1"
  def jobName: String = "addons"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    val channel = opt[String]("channel", descr = "Only process data from the given channel", required = false)
    val appVersion = opt[String]("version", descr = "Only process data from the given app version", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val to = conf.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }
    val from = conf.from.get match {
      case Some(f) => fmt.parseDateTime(f)
      case _ => DateTime.now.minusDays(1)
    }

    // Set up Spark
    val sparkConf = new SparkConf().setAppName(jobName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    implicit val sc = new SparkContext(sparkConf)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // We want to end up with reasonably large parquet files on S3.
    val parquetSize = 512 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    // Don't write metadata files, because they screw up partition discovery.
    // This is fixed in Spark 2.0, see:
    //   https://issues.apache.org/jira/browse/SPARK-13207
    //   https://issues.apache.org/jira/browse/SPARK-15454
    //   https://issues.apache.org/jira/browse/SPARK-15895
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    val spark = SparkSession
      .builder()
      .appName("AddonsView")
      .getOrCreate()

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")
      val filterChannel = conf.channel.get
      val filterVersion = conf.appVersion.get

      println("=======================================================================================")
      println(s"BEGINNING JOB $jobName FOR $currentDateString")
      if (filterChannel.nonEmpty)
        println(s" Filtering for channel = '${filterChannel.get}'")
      if (filterVersion.nonEmpty)
        println(s" Filtering for version = '${filterVersion.get}'")

      val schema = buildSchema
      val ignoredCount = sc.longAccumulator("Number of Records Ignored")
      val processedCount = sc.longAccumulator("Number of Records Processed")
      val badAddonCount = sc.longAccumulator("Number of Bad Addon records skipped")
      val messages = Dataset("telemetry")
        .where("sourceName") {
          case "telemetry" => true
        }.where("sourceVersion") {
          case "4" => true
        }.where("docType") {
          case "main" => true
        }.where("appName") {
          case "Firefox" => true
        }.where("submissionDate") {
          case date if date == currentDate.toString("yyyyMMdd") => true
        }.where("appUpdateChannel") {
          case channel => filterChannel.isEmpty || channel == filterChannel.get
        }.where("appVersion") {
          case v => filterVersion.isEmpty || v == filterVersion.get
        }.records(conf.limit.get)

      val rowRDD = messages.flatMap(m => {
        messageToRows(m, badAddonCount) match {
          case None =>
            ignoredCount.add(1)
            None
          case x =>
            processedCount.add(1)
            x
        }
      }).flatMap(x => x)

      val records = spark.createDataFrame(rowRDD, schema)

      // Note we cannot just use 'partitionBy' below to automatically populate
      // the submission_date partition, because none of the write modes do
      // quite what we want:
      //  - "overwrite" causes the entire vX partition to be deleted and replaced with
      //    the current day's data, so doesn't work with incremental jobs
      //  - "append" would allow us to generate duplicate data for the same day, so
      //    we would need to add some manual checks before running
      //  - "error" (the default) causes the job to fail after any data is
      //    loaded, so we can't do single day incremental updates.
      //  - "ignore" causes new data not to be saved.
      // So we manually add the "submission_date_s3" parameter to the s3path.
      val s3prefix = s"$jobName/$streamVersion/submission_date_s3=$currentDateString"
      val s3path = s"s3://${conf.outputBucket()}/$s3prefix"

      // Repartition the dataframe by sample_id before saving.
      val partitioned = records.repartition(100, records.col("sample_id"))

      // Then write to S3 using the given fields as path name partitions. If any
      // data already exists for the target day, replace it.
      partitioned.write.partitionBy("sample_id").mode("overwrite").parquet(s3path)

      // Then remove the _SUCCESS file so we don't break Spark partition discovery.
      S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")

      println(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      println(s"     RECORDS SEEN:    ${ignoredCount.value + processedCount.value}")
      println(s"     RECORDS IGNORED: ${ignoredCount.value}")
      println(s"     BAD ADDONS:      ${badAddonCount.value}")
      println("=======================================================================================")
      sc.stop()
    }
  }

  // Convert the given Heka message containing a "main" ping
  // to a map containing just the fields we're interested in.
  def messageToRows(message: Message, counter: LongAccumulator): Option[List[Row]] = {
    val fields = message.fieldsAsMap()

    // Don't compute the expensive stuff until we need it. We may skip a record
    // due to missing required fields.
    lazy val addons = parse(fields.getOrElse("environment.addons", "{}").asInstanceOf[String])

    val docId = fields.getOrElse("documentId", None) match {
      case x: String => x
      // documentId is required, and must be a string. If either
      // condition is not satisfied, we skip this record.
      case _ => return None
    }

    val clientId = fields.getOrElse("clientId", None) match {
      case x: String => x
      // clientId is required, and must be a string.
      case _ => return None
    }

    val sampleId = fields.getOrElse("sampleId", None) match {
      case x: Long => x
      case x: Double => x.toLong
      // sampleId is required, and must be a number.
      case _ => return None
    }

    val activeAddons = addons \ "activeAddons"
    val addonIds = activeAddons match {
      case JObject(addon) => addon map(x => x._1)
      case _ => List()
    }

    implicit val formats = DefaultFormats

    val addonsParsed = addonIds map(aid => Try((aid, (activeAddons \ aid).extract[AddonData])))
    counter.add(addonsParsed.count(_.isFailure))
    val successes = addonsParsed.filter(_.isSuccess).map(_.get).map { case (aid, addonData: AddonData) =>
      Row(docId, clientId, sampleId, aid,
        addonData.blocklisted.orNull,
        addonData.name.orNull,
        addonData.userDisabled.orNull,
        addonData.appDisabled.orNull,
        addonData.version.orNull,
        addonData.scope.orNull,
        addonData.`type`.orNull,
        addonData.foreignInstall.orNull,
        addonData.hasBinaryComponents.orNull,
        addonData.installDay.orNull,
        addonData.updateDay.orNull,
        addonData.signedState.orNull,
        addonData.isSystem.orNull)
    }

    if (addonIds.isEmpty)
      // Output a row with a null addon_id for this submission. This way we can identify pings without any addons.
      Some(List(Row(docId, clientId, sampleId, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null)))
    else
      Some(successes)
  }

  def buildSchema: StructType = {
    StructType(List(
      // Fields from the containing ping:
      StructField("document_id",           StringType, nullable = false), // documentId
      StructField("client_id",             StringType, nullable = false), // clientId
      StructField("sample_id",             LongType,   nullable = false), // Fields[sampleId]

      // Per-addon info:
      // Include the id of each addon in the ping. If there were no addons present, output a record
      // with a null addon_id so we can do analysis on addon-free clients / documents.
      StructField("addon_id",              StringType, nullable = true),
      StructField("blocklisted",           BooleanType, nullable = true),

      // Skip "description" field - if needed, look it up from AMO.

      StructField("name",                  StringType,  nullable = true),
      StructField("user_disabled",         BooleanType, nullable = true),
      StructField("app_disabled",          BooleanType, nullable = true),
      StructField("version",               StringType,  nullable = true),
      StructField("scope",                 IntegerType, nullable = true),
      StructField("type",                  StringType,  nullable = true),
      StructField("foreign_install",       BooleanType, nullable = true),
      StructField("has_binary_components", BooleanType, nullable = true),
      StructField("install_day",           IntegerType, nullable = true),
      StructField("update_day",            IntegerType, nullable = true),
      StructField("signed_state",          IntegerType, nullable = true),
      StructField("is_system",             BooleanType, nullable = true)
    ))
  }
}
