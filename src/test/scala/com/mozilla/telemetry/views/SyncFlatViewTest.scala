/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.utils.{SyncPingConversion, getOrCreateSparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.joda.time.DateTime
import org.json4s.JsonAST.{JArray, JNothing, JObject}
import org.json4s.{DefaultFormats, JValue}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class SyncFlatViewTest extends FlatSpec with Matchers with DataFrameSuiteBase {

  "New Style SyncPing payload" can "be flattened" in {
    sc.setLogLevel("WARN")
    val row = SyncPingConversion.pingToFlatRows(SyncViewTestPayloads.multiSyncPing)
    val rdd = sc.parallelize(row.toSeq)
    val dataframe = spark.createDataFrame(rdd, SyncPingConversion.singleEngineFlatSyncType)

    // verify the contents
    validateSyncPing(dataframe.collect(), SyncViewTestPayloads.multiSyncPing)
  }

  "SyncPing records" can "be round-tripped to parquet as flat data" in {
    sc.setLogLevel("WARN")
    val row = SyncPingConversion.pingToFlatRows(SyncViewTestPayloads.multiSyncPing)
    // Write a parquet file with the rows.
    val rdd = sc.parallelize(row.toSeq)
    val dataframe = spark.createDataFrame(rdd, SyncPingConversion.singleEngineFlatSyncType)
    val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
    dataframe.write.parquet(tempFile.toString)
    // read it back in and verify it.
    val localDataset = spark.read.load(tempFile.toString)
    localDataset.createOrReplaceTempView("sync")
    val localDataframe = spark.sql("SELECT * FROM sync")
    validateSyncPing(localDataframe.collect(), SyncViewTestPayloads.multiSyncPing)
  }

  private def validateSyncPing(rows: Array[Row], ping: JValue) {
    implicit val formats = DefaultFormats
    val syncs = rows.groupBy(_.getAs[String]("sync_id"))
    syncs.size should be (2)
    val JArray(pingSyncs) = ping \ "payload" \ "syncs";
    for ((_, syncRows) <- syncs) {
      val Some(pingSync) = pingSyncs.find(p => (p \ "when").extract[Long] == syncRows(0).getAs[Long]("when"))
      val JArray(pingEngines) = pingSync \ "engines"

      val seen = mutable.Set.empty[String]

      syncRows.size should be (6)

      for (sync <- syncRows) {
        // check common data
        sync.getAs[String]("app_build_id") should be((ping \ "application" \ "buildId").extract[String])
        sync.getAs[String]("app_display_version") should be((ping \ "application" \ "displayVersion").extract[String])
        sync.getAs[String]("app_name") should be((ping \ "application" \ "name").extract[String])
        sync.getAs[String]("app_version") should be((ping \ "application" \ "version").extract[String])
        sync.getAs[String]("app_channel") should be((ping \ "application" \ "channel").extract[String])

        sync.getAs[String]("os") should be((ping \ "payload" \ "os" \ "name").extract[String])
        sync.getAs[String]("os_version") should be((ping \ "payload" \ "os" \ "version").extract[String])
        sync.getAs[String]("os_locale") should be((ping \ "payload" \ "os" \ "locale").extract[String])

        sync.getAs[String]("uid") should be((pingSync \ "uid").extract[String])
        sync.getAs[String]("device_id") should be((pingSync \ "deviceID").extract[String])
        sync.getAs[Long]("took") should be((pingSync \ "took").extract[Long])
        sync.getAs[GenericRowWithSchema]("status") should be(null)
        sync.getAs[String]("sync_day") should be (new DateTime((pingSync \ "when").extract[Long]).toDateTime.toString("yyyyMMdd"))

        // engine specific data
        val engineName = sync.getAs[String]("engine_name")
        seen.contains(engineName) should be(false)
        seen.add(engineName)
        val Some(pingEngine) = pingEngines.find(e => (e \ "name").extract[String] == engineName)

        sync.getAs[Long]("engine_took") should be ((pingEngine \ "took").extractOrElse[Long](0))
        sync.getAs[String]("engine_status") should be ((pingEngine \ "status").extractOrElse[String](null))

        // check failure
        sync.getAs[GenericRowWithSchema]("engine_failure_reason") match {
          case null =>
            (pingEngine \ "failureReason") should be (JNothing)
          case reason =>
            reason.getAs[String]("name") should be ((pingEngine \ "failureReason" \ "name").extract[String])
            reason.getAs[String]("value") should be ((pingEngine \ "failureReason" \ "code").extract[String])
        }
        // check incoming
        val (expectedApplied, expectedFailed, expectedNewFailed, expectedReconciled) = pingEngine \ "incoming" match {
          case JObject(_) => (
            (pingEngine \ "incoming" \ "applied").extractOrElse[Long](0),
            (pingEngine \ "incoming" \ "failed").extractOrElse[Long](0),
            (pingEngine \ "incoming" \ "newFailed").extractOrElse[Long](0),
            (pingEngine \ "incoming" \ "reconciled").extractOrElse[Long](0)
          )
          case _ =>
            (0L, 0L, 0L, 0L)
        }
        sync.getAs[Long]("engine_incoming_applied") should be(expectedApplied)
        sync.getAs[Long]("engine_incoming_failed") should be(expectedFailed)
        sync.getAs[Long]("engine_incoming_new_failed") should be(expectedNewFailed)
        sync.getAs[Long]("engine_incoming_reconciled") should be(expectedReconciled)

        // Check outgoing
        val (expectedOutCount, expectedOutSent, expectedOutFailed) = pingEngine \ "outgoing" match {
          case JArray(l) => (
            l.size.toLong,
            l.map(x => (x \ "sent").extractOrElse[Long](0)).sum,
            l.map(x => (x \ "failed").extractOrElse[Long](0)).sum
          )
          case JObject(_) => (
            1L,
            (pingEngine \ "outgoing" \ "sent").extractOrElse[Long](0),
            (pingEngine \ "outgoing" \ "sent").extractOrElse[Long](0)
          )
          case _ =>
            (0L, 0L, 0L)
        }
        sync.getAs[Long]("engine_outgoing_batch_count") should be(expectedOutCount)
        sync.getAs[Long]("engine_outgoing_batch_total_sent") should be(expectedOutSent)
        sync.getAs[Long]("engine_outgoing_batch_total_failed") should be(expectedOutFailed)
      }
    }
  }


}
