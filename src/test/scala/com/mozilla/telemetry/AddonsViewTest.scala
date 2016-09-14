package com.mozilla.telemetry

import com.mozilla.telemetry.heka.HekaFrame
import com.mozilla.telemetry.views.AddonsView
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class AddonsViewTest extends FlatSpec with Matchers{
  def checkRowValues(row: Row, schema: StructType, expected: Map[String,Any]) = {
    val actual = new GenericRowWithSchema(row.toSeq.toArray, schema).getValuesMap(expected.keys.toList)
    val aid = expected("addon_id")
    for ((f, v) <- expected) {
      withClue(s"$aid[$f]:") { actual.get(f) should be (Some(v)) }
    }
    actual should be (expected)
  }

  "Addon records" can "be converted" in {
    val sparkConf = new SparkConf().setAppName("Addons")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val badAddonCount = sc.longAccumulator("Number of Bad Addon records skipped")
    sc.setLogLevel("WARN")
    try {
      val schema = AddonsView.buildSchema

      // Use an example framed-heka message. It is based on test_main.json.gz,
      // submitted with a URL of
      //    /submit/telemetry/foo/main/Firefox/48.0a1/nightly/20160315030230
      val hekaFileName = "/test_main.snappy.heka"
      val hekaURL = getClass.getResource(hekaFileName)
      val input = hekaURL.openStream()
      val rows = HekaFrame.parse(input).flatMap(AddonsView.messageToRows(_, badAddonCount)).flatten

      // Serialize this one row as Parquet
      val sqlContext = new SQLContext(sc)
      val dataframe = sqlContext.createDataFrame(sc.parallelize(rows.toSeq), schema)
      val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
      dataframe.write.parquet(tempFile.toString)

      badAddonCount.value should be (0)

      // Then read it back
      val data = sqlContext.read.parquet(tempFile.toString)

      val docId = "foo"
      val clientId = "c4582ba1-79fc-1f47-ae2a-671118dccd8b"
      val sampleId = 4l

      data.count() should be (3)
      data.filter(data("document_id") === "foo").count() should be (3)
      val e10s = data.filter(data("addon_id") === "e10srollout@mozilla.org")
      e10s.count() should be (1)
      checkRowValues(e10s.first, schema, Map(
        "document_id"           -> docId,
        "client_id"             -> clientId,
        "sample_id"             -> sampleId,
        "addon_id"              -> "e10srollout@mozilla.org",
        "blocklisted"           -> false,
        "name"                  -> "Multi-process staged rollout",
        "user_disabled"         -> false,
        "app_disabled"          -> false,
        "version"               -> "1.0",
        "scope"                 -> 1,
        "type"                  -> "extension",
        "foreign_install"       -> false,
        "has_binary_components" -> false,
        "install_day"           -> 16865,
        "update_day"            -> 16875,
        "signed_state"          -> null,
        "is_system"             -> true
      ))

      val pocket = data.filter(data("addon_id") === "firefox@getpocket.com")
      pocket.count() should be (1)
      checkRowValues(pocket.first, schema, Map(
        "document_id"           -> docId,
        "client_id"             -> clientId,
        "sample_id"             -> sampleId,
        "addon_id"              -> "firefox@getpocket.com",
        "blocklisted"           -> false,
        "name"                  -> "Pocket",
        "user_disabled"         -> false,
        "app_disabled"          -> false,
        "version"               -> "1.0",
        "scope"                 -> 1,
        "type"                  -> "extension",
        "foreign_install"       -> false,
        "has_binary_components" -> false,
        "install_day"           -> 16861,
        "update_day"            -> 16875,
        "signed_state"          -> null,
        "is_system"             -> true
      ))

      val loop = data.filter(data("addon_id") === "loop@mozilla.org")
      loop.count() should be (1)
      checkRowValues(loop.first, schema, Map(
        "document_id"           -> docId,
        "client_id"             -> clientId,
        "sample_id"             -> sampleId,
        "addon_id"              -> "loop@mozilla.org",
        "blocklisted"           -> false,
        "name"                  -> "Firefox Hello Beta",
        "user_disabled"         -> false,
        "app_disabled"          -> false,
        "version"               -> "1.1.12",
        "scope"                 -> 1,
        "type"                  -> "extension",
        "foreign_install"       -> false,
        "has_binary_components" -> false,
        "install_day"           -> 16861,
        "update_day"            -> 16875,
        "signed_state"          -> null,
        "is_system"             -> true
      ))

    } finally {
      sc.stop()
    }
  }
}
