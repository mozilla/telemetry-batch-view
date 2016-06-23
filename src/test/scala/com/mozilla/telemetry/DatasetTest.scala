package com.mozilla.telemetry

import java.io.{ByteArrayInputStream, InputStream}
import com.mozilla.telemetry.heka.Dataset
import com.mozilla.telemetry.utils.{ObjectSummary, AbstractS3Store}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

object MockS3Store extends AbstractS3Store {
  private var retry = 0

  def getKey(bucket: String, key: String): InputStream = key match{
    case "sources.json" =>
      val text = """
        |{
        |  "telemetry": {
        |    "prefix": "telemetry",
        |    "bucket": "foo"
        |  }
        |}
      """.stripMargin
      new ByteArrayInputStream(text.getBytes)

    case "telemetry/schema.json" =>
      val text = """
         |{
         |  "dimensions": [
         |    { "field_name": "submissionDate" },
         |    { "field_name": "docType" },
         |    { "field_name": "appName" }
         |  ]
         |}
      """.stripMargin
      new ByteArrayInputStream(text.getBytes)

    case "x" =>
      new ByteArrayInputStream(Resources.hekaFile(42))

    case "error" =>
      retry += 1
      if (retry == 3) {
        new ByteArrayInputStream(Resources.hekaFile(84))
      } else {
        new ByteArrayInputStream(Resources.hekaFile(42) ++ Array(0.toByte))
      }

    case _ =>
      throw new Exception("File missing")
  }

  def listKeys(bucket: String, prefix: String): Stream[ObjectSummary] = {
    prefix match {
      case "Firefox/" => Stream(ObjectSummary("x", 1))
      case "Fennec/" => Stream(ObjectSummary("a", 1))
      case "Error/" => Stream(ObjectSummary("error", 1), ObjectSummary("missing", 1))
    }
  }
  def listFolders(bucket: String, prefix: String, delimiter: String): Stream[String] = prefix match {
    case "telemetry/" => Stream("20160606/", "20160607/")
    case "20160606/" => Stream("main/", "crash/", "error/")
    case "20160607/" => Stream("other/")
    case "main/" => Stream("Firefox/")
    case "crash/" => Stream("Fennec/")
    case "error/" => Stream("Error/")
  }
}

class DatasetTest extends FlatSpec with Matchers{
  "Partitions" can "be filtered" in {
    val files = Dataset("telemetry", MockS3Store)
      .where("submissionDate") {
        case date if date.endsWith("06") => true
      }.where("docType") {
        case "main" => true
      }.summaries().toList

    files should be (List(ObjectSummary("x", 1)))
  }

  "Files" can "be limited" in {
    val files = Dataset("telemetry", MockS3Store)
      .where("submissionDate") {
        case "20160606" => true
      }.where("docType") {
      case "main" => true
    }.summaries(Some(1)).toList

    files should be (List(ObjectSummary("x", 1)))
  }

  "Records" can "be fetched" in {
    val sparkConf = new SparkConf().setAppName("DatasetTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    implicit val sc = new SparkContext(sparkConf)

    try {
      val records = Dataset("telemetry", MockS3Store)
        .where("submissionDate") {
          case "20160606" => true
        }.where("docType") {
        case "main" => true
      }

      records.count() should be (42)
    } finally {
      sc.stop()
    }
  }

  "Reads from S3" should "be retried in case of failure" in {
    val sparkConf = new SparkConf().setAppName("DatasetTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    implicit val sc = new SparkContext(sparkConf)

    try {
      val records = Dataset("telemetry", MockS3Store)
        .where("submissionDate") {
          case "20160606" => true
        }.where("docType") {
        case "error" => true
      }

      records.count() should be (84)
    } finally {
      sc.stop()
    }
  }
}
