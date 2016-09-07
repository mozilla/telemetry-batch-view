package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils.S3Store
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days, format}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse
import org.rogach.scallop._ // Just for my attempted mocks below.....

object SyncView {
  def streamVersion: String = "v1"
  def jobName: String = "sync_summary"

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

    // XXX - the below is copied verbatim from MainSummaryView - I've no idea how much is relevant here.
    // Set up Spark
    val sparkConf = new SparkConf().setAppName(jobName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    implicit val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // We want to end up with reasonably large parquet files on S3.
    val parquetSize = 512 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    // Don't write temp files to S3 while building parquet files.
    hadoopConf.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
    // Don't write metadata files, because they screw up partition discovery.
    // This is fixed in Spark 2.0, see:
    //   https://issues.apache.org/jira/browse/SPARK-13207
    //   https://issues.apache.org/jira/browse/SPARK-15454
    //   https://issues.apache.org/jira/browse/SPARK-15895
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")

      println("=======================================================================================")
      println(s"BEGINNING JOB $jobName FOR $currentDateString")

      val ignoredCount = sc.accumulator(0, "Number of Records Ignored")
      val processedCount = sc.accumulator(0, "Number of Records Processed")

      val messages = Dataset("telemetry")
        .where("sourceName") {
          case "telemetry" => true
        }.where("sourceVersion") {
          case "4" => true
        }.where("docType") {
          case "sync" => true
        }.where("appName") {
          case "Firefox" => true
        }.where("submissionDate") {
          case date if date == currentDate.toString("yyyyMMdd") => true
        }.records(conf.limit.get)

      val rowRDD = messages.flatMap(m => {
        messageToRow(m) match {
          case Nil =>
            ignoredCount += 1
            None
          case x =>
            processedCount += 1
            x
        }
      })

      val records = sqlContext.createDataFrame(rowRDD, SyncPingConverter.syncType)

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
        val s3prefix = s"$jobName/$streamVersion/submission_date_s3=$currentDateString"
        val s3path = s"s3://${conf.outputBucket()}/$s3prefix"

        records.write.mode("error").parquet(s3path)

        // Then remove the _SUCCESS file so we don't break Spark partition discovery.
        S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")
        println(s"Wrote data to s3 path $s3path")
      } else {
        // Write the data to a local file.
        records.write.parquet(conf.outputFilename())
        println(s"Wrote data to local file ${conf.outputFilename()}")
      }

      println(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      println("     RECORDS SEEN:    %d".format(ignoredCount.value + processedCount.value))
      println("     RECORDS IGNORED: %d".format(ignoredCount.value))
      println("=======================================================================================")
    }
  }

  // Convert the given Heka message containing a "sync" ping
  // to a list of rows containing all the fields.
  def messageToRow(message: Message): List[Row] = {
    val fields = message.fieldsAsMap()

    val payload = parse(message.payload.getOrElse("{}").asInstanceOf[String])
    lazy val build = parse(fields.getOrElse("application.buildId", "{}").asInstanceOf[String])
    SyncPingConverter.pingToRows(payload)
  }
}

object SyncPingConverter {
  private val failureType = StructType(List(
    // failures are probably *too* flexible in the schema, but all current errors have a "name" and a second field
    // that is a string or an int. To keep things simple and small here, we just define a string "value" field and
    // convert ints to the string.
    StructField("name", StringType, nullable = false),
    StructField("value", StringType, nullable = true)
  ))

  // XXX - this looks dodgy - I'm sure there's a more scala-ish way to write this...
  private def failureReasonToRow(failure: JValue): Row = failure match {
    case JObject(x) =>
      implicit val formats = DefaultFormats
      Row(
        (failure \ "name").extract[String],
        (failure \ "name").extract[String] match {
          case "httperror" => (failure \ "code").extract[String]
          case "nserror" => (failure \ "code").extract[String]
          case "shutdownerror" => null
          case "autherror" => (failure \ "from").extract[String]
          case "othererror" => (failure \ "error").extract[String]
          case "unexpectederror" => (failure \ "error").extract[String]
          case _ => null
        }
      )
    case _ =>
      null
  }

  // The record of incoming sync-records.
  private val incomingType = StructType(List(
    StructField("applied", LongType, nullable = false),
    StructField("failed", LongType, nullable = false),
    StructField("newFailed", LongType, nullable = false),
    StructField("reconciled", LongType, nullable = false)
  ))
  // Create a row representing incomingType
  private def incomingToRow(incoming: JValue): Row = incoming match {
    case JObject(x) =>
      Row(
        incoming \ "applied" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "failed" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "newFailed" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "reconciled" match {
          case JInt(x) => x.toLong
          case _ => 0L
        }
      )
    case _ => null
  }

  // Outgoing records.
  private val outgoingType = StructType(List(
    StructField("sent", LongType, nullable = false),
    StructField("failed", LongType, nullable = false)
  ))
  private def outgoingToRow(outgoing: JValue): List[Row] = outgoing match {
    case JArray(x) =>
      val buf = scala.collection.mutable.ListBuffer.empty[Row]
      for (outgoing_entry <- x) {
        buf.append(Row(
          outgoing_entry \ "sent" match {
          case JInt(x) => x.toLong
          case _ => 0L
          },
          outgoing_entry \ "failed" match {
            case JInt(x) => x.toLong
            case _ => 0L
          }
        ))
      }
      if (buf.isEmpty) null
      else buf.toList
    case _ => null
  }

  // The schema for an engine.
  private val engineType = StructType(List(
    StructField("name", StringType, nullable = false),
    StructField("took", LongType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("failureReason", failureType, nullable = true),
    StructField("incoming", incomingType, nullable = true),
    StructField("outgoing", ArrayType(outgoingType, containsNull = false), nullable = true)
  ))

  // Parse an element of "engines" elt in a sync object
  private def engineToRow(engine: JValue): Row = {
    Row(
      engine \ "name" match {
        case JString(x) => x
        case _ => return null // engines must have a name!
      },
      engine \ "took" match {
        case JInt(x) => x.toLong
        case _ => null
      },
      engine \ "status" match {
        case JString(x) => x
        case _ => null
      },
      failureReasonToRow(engine \ "failureReason"),
      incomingToRow(engine \ "incoming"),
      outgoingToRow(engine \ "outgoing")
    )
  }

  private def toEnginesRows(engines: JValue): List[Row] = engines match {
    case JArray(x) =>
      val buf = scala.collection.mutable.ListBuffer.empty[Row]
      // Need simple array iteration??
      for (e <- x) {
        buf.append(engineToRow(e))
      }
      if (buf.isEmpty) null
      else buf.toList
    case _ => null
  }

  // The status for the Sync itself (ie, not the status for an engine - that's just a string)
  private val statusType = StructType(List(
    StructField("sync", StringType, nullable = true),
    StructField("service", StringType, nullable = true)
  ))
  private def statusToRow(status: JValue): Row = status match {
    case JObject(x) =>
      Row(
        status \ "sync" match {
          case JString(x) => x
          case _ => null
        },
        status \ "service" match {
          case JString(x) => x
          case _ => null
        }
      )
    case _ => null
  }

  // The record of a single sync event.
  def syncType = StructType(List(
    // These field names are the same as used by MainSummaryView
    StructField("app_build_id", StringType, nullable = true), // application/buildId
    StructField("app_display_version", StringType, nullable = true), // application/displayVersion
    StructField("app_name", StringType, nullable = true), // application/name
    StructField("app_version", StringType, nullable = true), // application/version
    // XXX - how do we record the "platform"?

    // These fields are unique to the sync pings.
    StructField("uid", StringType, nullable = false),
    StructField("deviceID", StringType, nullable = true), // should always exists, but old pings didn't record it.
    StructField("when", LongType, nullable = false),
    StructField("took", LongType, nullable = false),
    StructField("failureReason", failureType, nullable = true),
    StructField("status", statusType, nullable = true),
    // "why" is defined in the client-side schema but currently never populated.
    StructField("why", StringType, nullable = true),
    StructField("engines", ArrayType(SyncPingConverter.engineType, containsNull = false), nullable = true)
  ))

  // Take an entire ping and return a list of rows with "syncType" as a schemma.
  def pingToRows(ping: JValue): List[Row] = {
    ping \ "payload" \ "syncs" match {
      case JArray(x) => multiSyncPayloadToRow(ping, x)
      case _ =>
        val row = singleSyncPayloadToRow(ping, ping \ "payload")
        row match {
          case Some(x) => List(x)
          case None => List()
      }
    }
  }

  // Convert a "new style v1" ping that records multiple Syncs to a number of rows.
  private def multiSyncPayloadToRow(ping: JValue, syncs: List[JValue]): List[Row] = {
    syncs.flatMap(sync => singleSyncPayloadToRow(ping, sync))
  }

  // Convert an "old style" ping that records a single Sync to a row.
  private def singleSyncPayloadToRow(ping: JValue, payload: JValue): Option[Row] = {
    val application = ping \ "application"
    val row = Row(
      // The metadata...
      application \ "buildId" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      application \ "displayVersion" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      application \ "name" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      application \ "version" match {
        case JString(x) => x
        case _ => return None // a required field.
      },

      // Info about the sync.
      payload \ "uid" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      payload \ "deviceID" match {
        case JString(x) => x
        case _ => null
      },
      payload \ "when" match {
        case JInt(x) => x.toLong
        case _ => return None
      },
      payload \ "took" match {
        case JInt(x) => x.toLong
        case _ => return None
      },
      failureReasonToRow(payload \ "failureReason"),
      statusToRow(payload \ "status"),
      payload \ "why" match {
        case JString(x) => x
        case _ => null
      },
      toEnginesRows(payload \ "engines")
    )

    Some(row)
  }

}
