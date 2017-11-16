package com.mozilla.telemetry

import com.mozilla.telemetry.utils.SyncPingConversion
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonAST.JNothing
import org.json4s.{DefaultFormats, JValue, JObject}
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
      val row = SyncPingConversion.pingToNestedRows(SyncViewTestPayloads.singleSyncPing)
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)

      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConversion.nestedSyncType)

      // verify the contents.
      dataframe.count() should be (1)
      val checkRow = dataframe.first()

      checkRow.getAs[String]("app_build_id") should be ((ping \ "application" \ "buildId").extract[String])
      checkRow.getAs[String]("app_display_version") should be ((ping \ "application" \ "displayVersion").extract[String])
      checkRow.getAs[String]("app_name") should be ((ping \ "application" \ "name").extract[String])
      checkRow.getAs[String]("app_version") should be ((ping \ "application" \ "version").extract[String])
      checkRow.getAs[String]("app_channel") should be ((ping \ "application" \ "channel").extract[String])

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
      val row = SyncPingConversion.pingToNestedRows(SyncViewTestPayloads.multiSyncPing)
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConversion.nestedSyncType)

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
      val row = SyncPingConversion.pingToNestedRows(SyncViewTestPayloads.multiSyncPing)
      // Write a parquet file with the rows.
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConversion.nestedSyncType)
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

  "SyncPing records with validation and device data" can "be round-tripped to parquet" in {
    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      val row = SyncPingConversion.pingToNestedRows(SyncViewTestPayloads.complexSyncPing)
      // Write a parquet file with the rows.
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConversion.nestedSyncType)
      val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
      dataframe.write.parquet(tempFile.toString)
      // read it back in and verify it.
      val localDataset = sqlContext.read.load(tempFile.toString)
      localDataset.registerTempTable("sync")
      val localDataframe = sqlContext.sql("SELECT * FROM sync")
      validateComplexSyncPing(localDataframe.collect(), SyncViewTestPayloads.complexSyncPing)
    } finally {
      sc.stop()
    }
  }

  "SyncPing records with top level ids" can "come through as if they were not at the top level" in {
    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      val row = SyncPingConversion.pingToNestedRows(SyncViewTestPayloads.multiSyncPingWithTopLevelIds)
      // Write a parquet file with the rows.
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConversion.nestedSyncType)
      // Note: We intentionally validate with a *different* json object from the one we parsed.
      validateMultiSyncPing(dataframe.collect(), SyncViewTestPayloads.multiSyncPing)
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
      engine.getAs[GenericRowWithSchema]("failure_reason") match {
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

  private def getOS(ping: JValue): JValue =  ping \ "os" match {
    case obj @ JObject(_) => obj
    case _ => ping \ "payload" \ "os"
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
    firstSync.getAs[String]("app_channel") should be ((ping \ "application" \ "channel").extract[String])

    firstSync.getAs[String]("os") should be ((getOS(ping) \ "name").extract[String])
    firstSync.getAs[String]("os_version") should be ((getOS(ping) \ "version").extract[String])
    firstSync.getAs[String]("os_locale") should be ((getOS(ping) \ "locale").extract[String])

    val firstPing = (ping \ "payload" \ "syncs")(0)
    firstSync.getAs[Long]("when") should be ((firstPing \ "when").extract[Long])
    firstSync.getAs[String]("uid") should be ((firstPing \ "uid").extract[String])
    firstSync.getAs[String]("device_id") should be ((firstPing \ "deviceID").extract[String])
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
    secondSync.getAs[String]("app_channel") should be ((ping \ "application" \ "channel").extract[String])

    secondSync.getAs[String]("os") should be ((getOS(ping) \ "name").extract[String])
    secondSync.getAs[String]("os_version") should be ((getOS(ping) \ "version").extract[String])
    secondSync.getAs[String]("os_locale") should be ((getOS(ping) \ "locale").extract[String])

    val secondPing = (ping \ "payload" \ "syncs")(1)

    secondSync.getAs[Long]("when") should be ((secondPing \ "when").extract[Long])
    secondSync.getAs[String]("uid") should be ((secondPing \ "uid").extract[String])
    secondSync.getAs[String]("device_id") should be ((secondPing \ "deviceID").extract[String])
    secondSync.getAs[Long]("took") should be ((secondPing \ "took").extract[Long])

    secondSync.getAs[GenericRowWithSchema]("status") should be (null)
    secondSync.getAs[GenericRowWithSchema]("devices") should be (null)

    val secondEngines = secondSync.getAs[mutable.WrappedArray[GenericRowWithSchema]]("engines")
    validateEngines(secondEngines, (secondPing \ "engines").extract[List[JValue]])
  }


  // A helper to check the contents of the sync ping with devices and validation data.
  private def validateComplexSyncPing(rows: Array[Row], ping: JValue) {
    implicit val formats = DefaultFormats
    rows.length should be (1)

    val sync = rows(0)

    sync.getAs[String]("app_build_id") should be ((ping \ "application" \ "buildId").extract[String])
    sync.getAs[String]("app_display_version") should be ((ping \ "application" \ "displayVersion").extract[String])
    sync.getAs[String]("app_name") should be ((ping \ "application" \ "name").extract[String])
    sync.getAs[String]("app_version") should be ((ping \ "application" \ "version").extract[String])

    sync.getAs[String]("os") should be ((getOS(ping) \ "name").extract[String])
    sync.getAs[String]("os_version") should be ((getOS(ping) \ "version").extract[String])
    sync.getAs[String]("os_locale") should be ((getOS(ping) \ "locale").extract[String])

    val pingPayload = (ping \ "payload" \ "syncs")(0)
    sync.getAs[Long]("when") should be ((pingPayload \ "when").extract[Long])
    sync.getAs[String]("uid") should be ((pingPayload \ "uid").extract[String])
    sync.getAs[Long]("took") should be ((pingPayload \ "took").extract[Long])

    sync.getAs[GenericRowWithSchema]("status") should be (null)

    val syncDevices = sync.getAs[mutable.WrappedArray[GenericRowWithSchema]]("devices")
    val pingDevices = (pingPayload \ "devices").extract[List[JValue]]

    syncDevices.length should be (pingDevices.length)

    for (i <- 0 to 1) {
      syncDevices(i).getAs[String]("os") should be ((pingDevices(i) \ "os").extract[String])
      syncDevices(i).getAs[String]("version") should be ((pingDevices(i) \ "version").extract[String])
      syncDevices(i).getAs[String]("id") should be ((pingDevices(i) \ "id").extract[String])
    }

    val engines = sync.getAs[mutable.WrappedArray[GenericRowWithSchema]]("engines")
    val pingEngines = (pingPayload \ "engines").extract[List[JValue]]
    validateEngines(engines, pingEngines)

    validateEngines(engines , pingEngines)
    // Check the validation data on the bookmark engine in the first sync.
    val bmarkValidationRow = engines.find(x => x.getAs[String]("name") == "bookmarks").get
      .getAs[GenericRowWithSchema]("validation")
    val bmarkValidationJson = pingEngines.find(x => (x \ "name").extract[String] == "bookmarks").get \ "validation"

    bmarkValidationRow.getAs[Long]("version") should be ((bmarkValidationJson \ "version").extract[Long])
    bmarkValidationRow.getAs[Long]("took") should be ((bmarkValidationJson \ "took").extract[Long])
    bmarkValidationRow.getAs[Long]("checked") should be ((bmarkValidationJson \ "checked").extract[Long])
    val bmarkProblemsJson = (bmarkValidationJson \ "problems").extract[List[JValue]]
    if (bmarkProblemsJson.isEmpty) {
      bmarkValidationRow.getAs[GenericRowWithSchema]("problems") should be(null)
    } else {
      val bmarkProblems = bmarkValidationRow.getAs[mutable.WrappedArray[GenericRowWithSchema]]("problems")

      bmarkProblems.length should be(bmarkProblemsJson.length)

      for (i <- 0 to 1) {
        bmarkProblems(i).getAs[String]("name") should be((bmarkProblemsJson(i) \ "name").extract[String])
        bmarkProblems(i).getAs[Long]("count") should be((bmarkProblemsJson(i) \ "count").extract[Long])
      }
    }
  }
}
