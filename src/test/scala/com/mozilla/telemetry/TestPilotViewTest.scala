package com.mozilla.telemetry

import com.mozilla.telemetry.heka.HekaFrame
import com.mozilla.telemetry.views.TestPilotView
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class TestPilotViewTest extends FlatSpec with Matchers{

  "TestPilotView records" can "be serialized" in {
    val sparkConf = new SparkConf().setAppName("TestPilotView")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    implicit val sc = new SparkContext(sparkConf)
    sc.setLogLevel("INFO")
    try {
      val schema = TestPilotView.buildSchema

      // Use an example framed-heka message. It is based on test_testpilot.json.gz,
      // submitted with a URL of
      //    /submit/telemetry/foo/testpilot/Firefox/49.0.2/release/20161019084923/
      val hekaFileName = "/test_testpilot.heka"
      val hekaURL = getClass.getResource(hekaFileName)
      val input = hekaURL.openStream()
      val rows = HekaFrame.parse(input).flatMap(TestPilotView.messageToRows)
      println(rows)


      // Serialize this one row as Parquet
      val sqlContext = new SQLContext(sc)
      val dataframe = sqlContext.createDataFrame(sc.parallelize(rows.flatten.toSeq), schema)
      val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
      dataframe.write.parquet(tempFile.toString)

      // Then read it back
      val data = sqlContext.read.parquet(tempFile.toString)

      data.count() should be (1)
      data.filter(data("document_id") === "foo").count() should be (1)
    } finally {
      sc.stop()
    }
  }
}
