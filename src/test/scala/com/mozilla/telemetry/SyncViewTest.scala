package com.mozilla.telemetry

import com.mozilla.telemetry.views.SyncPingConverter
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonAST.JNothing
import org.json4s.{DefaultFormats, JValue}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class SyncViewTest extends FlatSpec with Matchers{
  "Old Style SyncPing payload" can "be serialized" in {
    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val ping = SyncViewTestPayloads.singleSyncPing
    implicit val formats = DefaultFormats
    sc.setLogLevel("WARN")
    try {
      val row = SyncPingConverter.pingToRows(SyncViewTestPayloads.singleSyncPing)
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)

      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConverter.syncType)

      // verify the contents.
      dataframe.count() should be (1)
      val checkRow = dataframe.first()

      checkRow.getAs[String]("app_build_id") should be ((ping \ "application" \ "buildId").extract[String])
      checkRow.getAs[String]("app_display_version") should be ((ping \ "application" \ "displayVersion").extract[String])
      checkRow.getAs[String]("app_name") should be ((ping \ "application" \ "name").extract[String])
      checkRow.getAs[String]("app_version") should be ((ping \ "application" \ "version").extract[String])

      val payload = ping \ "payload"
      checkRow.getAs[Long]("when") should be ((payload \ "when").extract[Long])
      checkRow.getAs[String]("uid") should be ((payload \ "uid").extract[String])
      checkRow.getAs[Long]("took") should be ((payload \ "took").extract[Long])

      val status = checkRow.getAs[GenericRowWithSchema]("status")
      status.getAs[String]("service") should be ((payload \ "status" \ "service").extract[String])
      status.getAs[String]("sync") should be ((payload \ "status" \ "sync").extract[String])

      val engines = checkRow.getAs[mutable.WrappedArray[GenericRowWithSchema]]("engines")
      validateEngines(engines, (payload \ "engines").extract[List[JValue]])
    } finally {
      sc.stop()
    }
  }

  "New Style SyncPing payload" can "be serialized" in {
    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      val row = SyncPingConverter.pingToRows(SyncViewTestPayloads.multiSyncPing)
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConverter.syncType)

      // verify the contents
      validateMultiSyncPing(dataframe.collect(), SyncViewTestPayloads.multiSyncPing)
    } finally {
      sc.stop()
    }
  }

  "SyncPing records" can "be round-tripped to parquet" in {
    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      val row = SyncPingConverter.pingToRows(SyncViewTestPayloads.multiSyncPing)
      // Write a parquet file with the rows.
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConverter.syncType)
      val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
      dataframe.write.parquet(tempFile.toString)
      // read it back in and verify it.
      val localDataset = sqlContext.read.load(tempFile.toString)
      localDataset.registerTempTable("sync")
      val localDataframe = sqlContext.sql("SELECT * FROM sync")
      validateMultiSyncPing(localDataframe.collect(), SyncViewTestPayloads.multiSyncPing)
    } finally {
      sc.stop()
    }
  }

  // A helper to validate rows created for engines against the source JSON payload
  private def validateEngines(engines: mutable.WrappedArray[GenericRowWithSchema], jsonengines: List[JValue]): Unit = {
    implicit val formats = DefaultFormats

    for ( (engine, jsonengine) <- engines zip jsonengines ) {
      val name = engine.getAs[String]("name")
      name should be ((jsonengine \ "name").extract[String])
      engine.getAs[Long]("took") should be ((jsonengine \ "took").extractOrElse[Long](0))
      engine.getAs[String]("status") should be ((jsonengine \ "status").extractOrElse[String](null))
      // failureReason can be null or a row with "name" and "value
      engine.getAs[GenericRowWithSchema]("failureReason") match {
        case null =>
          (jsonengine \ "failureReason") should be (JNothing)
        case reason =>
          reason.getAs[String]("name") should be ((jsonengine \ "failureReason" \ "name").extract[String])
          reason.getAs[String]("value") should be ((jsonengine \ "failureReason" \ "code").extract[String])
      }
      // incoming can be null or have a number of fields.
      engine.getAs[GenericRowWithSchema]("incoming") match {
        case null =>
          (jsonengine \ "incoming") should be (JNothing)
        case incoming =>
          incoming.getAs[Long]("applied") should be ((jsonengine \ "incoming" \ "applied").extractOrElse[Long](0))
          incoming.getAs[Long]("failed") should be ((jsonengine \ "incoming" \ "failed").extractOrElse[Long](0))
          incoming.getAs[Long]("reconciled") should be ((jsonengine \ "incoming" \ "reconciled").extractOrElse[Long](0))
      }
      // outgoing can be null or have a number of fields.
      engine.getAs[mutable.WrappedArray[GenericRowWithSchema]]("outgoing") match {
        case null =>
          (jsonengine \ "outgoing") should be (JNothing)
        case outgoing: mutable.WrappedArray[GenericRowWithSchema] =>
          outgoing(0).getAs[Long]("sent") should be ((jsonengine \ "outgoing" \ "sent").extract[Long])
      }
    }
  }

  // A helper to check the contents of the multi-sync ping.
  private def validateMultiSyncPing(rows: Array[Row], ping: JValue) {
    implicit val formats = DefaultFormats
    rows.length should be (2)

    val firstSync = rows(0)

    firstSync.getAs[String]("app_build_id") should be ((ping \ "application" \ "buildId").extract[String])
    firstSync.getAs[String]("app_display_version") should be ((ping \ "application" \ "displayVersion").extract[String])
    firstSync.getAs[String]("app_name") should be ((ping \ "application" \ "name").extract[String])
    firstSync.getAs[String]("app_version") should be ((ping \ "application" \ "version").extract[String])

    val firstPing = (ping \ "payload" \ "syncs")(0)
    firstSync.getAs[Long]("when") should be ((firstPing \ "when").extract[Long])
    firstSync.getAs[String]("uid") should be ((firstPing \ "uid").extract[String])
    firstSync.getAs[Long]("took") should be ((firstPing \ "took").extract[Long])

    firstSync.getAs[GenericRowWithSchema]("status") should be (null)

    val engines = firstSync.getAs[mutable.WrappedArray[GenericRowWithSchema]]("engines")
    validateEngines(engines, (firstPing \ "engines").extract[List[JValue]])

    // The second sync in this payload.
    val secondSync = rows(1)

    secondSync.getAs[String]("app_build_id") should be ((ping \ "application" \ "buildId").extract[String])
    secondSync.getAs[String]("app_display_version") should be ((ping \ "application" \ "displayVersion").extract[String])
    secondSync.getAs[String]("app_name") should be ((ping \ "application" \ "name").extract[String])
    secondSync.getAs[String]("app_version") should be ((ping \ "application" \ "version").extract[String])

    val secondPing = (ping \ "payload" \ "syncs")(1)

    secondSync.getAs[Long]("when") should be ((secondPing \ "when").extract[Long])
    secondSync.getAs[String]("uid") should be ((secondPing \ "uid").extract[String])
    secondSync.getAs[Long]("took") should be ((secondPing \ "took").extract[Long])

    secondSync.getAs[GenericRowWithSchema]("status") should be (null)

    val secondEngines = secondSync.getAs[mutable.WrappedArray[GenericRowWithSchema]]("engines")
    validateEngines(secondEngines, (secondPing \ "engines").extract[List[JValue]])
  }
}
