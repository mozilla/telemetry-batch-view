/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{JValue, string2JsonInput}
import org.rogach.scallop._

object SyncEventView extends BatchJobBase {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

  def schemaVersion: String = "v1"
  def jobName: String = "sync_events"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends BaseOpts(args) {
    val outputFilename = opt[String]("outputFilename", descr = "Destination local filename for parquet data", required = false)
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    if (!conf.outputBucket.supplied && !conf.outputFilename.supplied) {
      conf.errorMessageHandler("One of outputBucket or outputFilename must be specified")
    }

    // Set up Spark
    val spark = getOrCreateSparkSession("SyncEventView")
    implicit val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration

    // We want to end up with reasonably large parquet files on S3.
    val parquetSize = 512 * 1024 * 1024

    hadoopConf.setInt("dfs.blocksize", parquetSize)
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    for (currentDateString <- datesBetween(conf.from(), conf.to.toOption)) {
      logger.info("=======================================================================================")
      logger.info(s"BEGINNING JOB $jobName FOR $currentDateString")

      val ignoredCount = sc.longAccumulator("Number of Records Ignored")
      val processedCount = sc.longAccumulator("Number of Records Processed")

      val messages = Dataset("telemetry")
        .where("sourceName") {
          case "telemetry" => true
        }.where("sourceVersion") {
          case "4" | "5" => true
        }.where("docType") {
          case "sync" => true
        }.where("submissionDate") {
          case date if date == currentDateString => true
        }.records(conf.limit.get)

      val rowRDD = messages.flatMap(m => {
        messageToRow(m) match {
          case Nil =>
            ignoredCount.add(1)
            None
          case x =>
            processedCount.add(1)
            x
        }
      })
      val records = spark.createDataFrame(rowRDD, SyncEventConverter.syncEventSchema)

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

        records.repartition(1).write.mode("overwrite").parquet(s3path)

        // Then remove the _SUCCESS file so we don't break Spark partition discovery.
        S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")
        logger.info(s"Wrote data to s3 path $s3path")
      } else {
        // Write the data to a local file.
        records.write.parquet(conf.outputFilename())
        logger.info(s"Wrote data to local file ${conf.outputFilename()}")
      }

      logger.info(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      logger.info(s"     RECORDS SEEN:    ${ignoredCount.value + processedCount.value}")
      logger.info(s"     RECORDS IGNORED: ${ignoredCount.value}")
      logger.info("=======================================================================================")
    }

    if (shouldStopContextAtEnd(spark)) { spark.stop() }
  }

  // Convert the given Heka message containing a "sync" ping with event data
  // to a list of rows containing relevant fields
  def messageToRow(message: Message): List[Row] = {
    val payload = parse(string2JsonInput(message.payload.getOrElse(message.fieldsAsMap.getOrElse("submission", "{}")).asInstanceOf[String]))
    SyncEventConverter.pingToRows(payload)
  }
}

// Convert Sync Events, which are defined by the schema at:
// https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/telemetry/data/sync-ping.html#events-in-the-sync-ping
object SyncEventConverter {
  def eventFields: Array[StructField] = Events.buildEventSchema.fields.map(
    f => f.copy(name = "event_" + f.name) // prepends `event_` to event schema column names for clarity
  )

  def syncEventSchema: StructType = StructType(List(
    // These field names are the same as used by MainSummaryView
    StructField("document_id", StringType, nullable = false), // id
    StructField("app_build_id", StringType, nullable = true), // application/buildId
    StructField("app_display_version", StringType, nullable = true), // application/displayVersion
    StructField("app_name", StringType, nullable = true), // application/name
    StructField("app_version", StringType, nullable = true), // application/version
    StructField("app_channel", StringType, nullable = true), // application/channel

    // These fields are unique to the sync pings.
    StructField("uid", StringType, nullable = false), // payload/uid
    StructField("why", StringType, nullable = true),  // payload/why
    StructField("device_id", StringType, nullable = true), // payload/deviceID

    StructField("device_os_name", StringType, nullable = true), // payload/os/name
    StructField("device_os_version", StringType, nullable = true), // payload/os/version
    StructField("device_os_locale", StringType, nullable = true) // payload/os/locale
  ) ++ eventFields
    ++ List(
      StructField("event_device_id", StringType, nullable = true), // present in most events
      StructField("event_flow_id", StringType, nullable = true), // present in most events
      StructField("event_device_version", StringType, nullable = true), // present in most events
      StructField("event_device_os", StringType, nullable = true) // present in most events
  )
  )

  def pingToRows(ping: JValue): List[Row] = {
    Events.extractEvents(ping \ "payload" \ "events") match {
      case Nil => List()
      case events => eventsToRows(ping, events)
    }
  }

  private def eventsToRows(ping: JValue, events: List[List[Any]]): List[Row] = {
    events.flatMap(event => eventToRow(ping, event))
  }

  // scalastyle:off return
  private def eventToRow(ping: JValue, event: List[Any]): Option[Row] = {
    val application = ping \ "application"
    val payload = ping \ "payload"

    val (os_name, os_version, os_locale) = SyncPingConversion.extractOSData(ping, payload)

    val common = List(
      ping \ "id" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
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
      application \ "channel" match {
        case JString(x) => x
        case _ => return None // a required field.
      },

      // Info about the sync.
      payload \ "uid" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      payload \ "why" match {
        case JString(x) => x
        case _ => null
      },
      payload \ "deviceID" match {
        case JString(x) => x
        case _ => null
      },
      os_name,
      os_version,
      os_locale
    )
    val eventObject = Event.fromList(event) match {
      case None => return None
      case Some(x) => x
    }
    val devices: Map[String, (String, String)] = payload \ "syncs" match {
      case JArray(l) => {
        val deviceMaps = l.flatMap(v => v \ "devices" match {
          case JArray(devs) => {
            devs.flatMap(dev => {
              val devID = dev \ "id" match {
                case JString(x) => x
                case _ => null
              }
              val devVer = dev \ "version" match {
                case JString(x) => x
                case _ => null
              }
              val devOS = dev \ "os" match {
                case JString(x) => x
                case _ => null
              }
              if (devID != null && devVer != null && devOS != null) {
                Some(Map[String, (String, String)]((devID, (devVer, devOS))))
              } else {
                None
              }
            })
          }
          case _ => List()
        }) ++ List(Map.empty[String, (String, String)]) // ensure deviceMaps is not empty
        deviceMaps.reduce((a, b) => a ++ b)
      }
      case _ => Map()
    }

    val values = eventObject.mapValues match {
      case Some(x: Map[String @unchecked, String @unchecked]) => {
        val deviceID = x getOrElse ("deviceID", null)
        val (deviceVersion, deviceOS) =
          if (deviceID == null) {
            (null, null)
          } else {
            devices getOrElse(deviceID, (null, null))
          }

        List(
          deviceID,
          x getOrElse ("flowID", null),
          deviceVersion,
          deviceOS
        )
      }
      case _ => List(null, null, null, null)
    }

    Some(Row.fromSeq(common ++ eventObject.toList ++ values))
  }
  // scalastyle:on return
}
