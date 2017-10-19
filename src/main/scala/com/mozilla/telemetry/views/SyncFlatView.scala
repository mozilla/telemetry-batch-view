package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils.{S3Store, SyncPingConversion}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days, format}
import org.json4s.string2JsonInput
import org.json4s.jackson.JsonMethods.parse
import org.rogach.scallop._ // Just for my attempted mocks below.....


object SyncFlatView {
  def schemaVersion: String = "v1"
  def jobName: String = "sync_flat_summary"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = false)
    val outputFilename = opt[String]("outputFilename", descr = "Destination local filename for parquet data", required = false)
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    if (!conf.outputBucket.supplied && !conf.outputFilename.supplied)
      conf.errorMessageHandler("One of outputBucket or outputFilename must be specified")
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
    val spark = SparkSession
      .builder()
      .appName("SyncFlatEngineView")
      .getOrCreate()
    val hadoopConf = sc.hadoopConfiguration

    // We want to end up with reasonably large parquet files on S3.
    val parquetSize = 256 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    val partitionCount = 100

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")

      println("=======================================================================================")
      println(s"BEGINNING JOB $jobName $schemaVersion FOR $currentDateString")

      val ignoredCount = sc.longAccumulator("Number of Records Ignored")
      val processedCount = sc.longAccumulator("Number of Records Processed")
      val failedCount = sc.longAccumulator("Number of Records Failed")

      val messages = Dataset("telemetry")
      .where("sourceName") {
        case "telemetry" => true
      }.where("sourceVersion") {
        case "4" | "5" => true
      }.where("docType") {
        case "sync" => true
      }.where("submissionDate") {
        case date if date == currentDate.toString("yyyyMMdd") => true
      }.records(conf.limit.get, Some(partitionCount))

      val rowRDD = messages.flatMap(m => {
        try {
          val payload = parse(string2JsonInput(m.payload.getOrElse(m.fieldsAsMap.getOrElse("submission", "{}")).asInstanceOf[String]))
          SyncPingConversion.pingToFlatRows(payload) match {
            case Nil => {
              ignoredCount.add(1)
              Nil
            }
            case x => {
              processedCount.add(1)
              x
            }
          }
        } catch {
          case _: Exception => {
            failedCount.add(1)
            Nil
          }
        }
      })

      val records = spark.createDataFrame(rowRDD, SyncPingConversion.singleEngineFlatSyncType)

      if (conf.outputBucket.supplied) {
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
        val s3prefix = s"$jobName/$schemaVersion/submission_date_s3=$currentDateString"
        val s3path = s"s3://${conf.outputBucket()}/$s3prefix"

        // We're already partitioned by partitionCount, so no need to repartition or coalesce.
        records.write.mode("overwrite").parquet(s3path)

        // Then remove the _SUCCESS file so we don't break Spark partition discovery.
        S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")
        println(s"Wrote data to s3 path $s3path")
      } else {
        // Write the data to a local file.
        records.write.parquet(conf.outputFilename())
        println(s"Wrote data to local file ${conf.outputFilename()}")
      }

      println(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      println(s"     RECORDS SEEN:    ${ignoredCount.value + processedCount.value + failedCount.value}")
      println(s"     RECORDS IGNORED: ${ignoredCount.value}")
      println(s"     RECORDS FAILED:  ${failedCount.value}")
      println("=======================================================================================")
    }

    sc.stop()
  }
}
