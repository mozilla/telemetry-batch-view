package telemetry.test

import java.io.{ByteArrayInputStream, InputStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
import telemetry.ObjectSummary
import telemetry.heka.Dataset
import telemetry.utils.AbstractS3Store

object MockS3Store extends AbstractS3Store {
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

    case _ =>
      new ByteArrayInputStream(Array())
  }

  def listKeys(bucket: String, prefix: String): Stream[ObjectSummary] = {
    prefix match {
      case "Firefox/" => Stream(ObjectSummary("x", 1), ObjectSummary("y", 2))
      case "Fennec/" => Stream(ObjectSummary("a", 1))
    }
  }
  def listFolders(bucket: String, prefix: String, delimiter: String): Stream[String] = prefix match {
    case "telemetry/" => Stream("20160606/", "20160607/")
    case "20160606/" => Stream("main/", "crash/")
    case "20160607/" => Stream("other/")
    case "main/" => Stream("Firefox/")
    case "crash/" => Stream("Fennec/")
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

    assert(files == List(ObjectSummary("x", 1), ObjectSummary("y", 2)))
  }

  "Files" can "be limited" in {
    val files = Dataset("telemetry", MockS3Store)
      .where("submissionDate") {
        case "20160606" => true
      }.where("docType") {
      case "main" => true
    }.summaries(Some(1)).toList

    assert(files == List(ObjectSummary("x", 1)))
  }

  "Records" can "be fetched" in {
    val sparkConf = new SparkConf().setAppName("KPI")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    implicit val sc = new SparkContext(sparkConf)

    try {
      val records = Dataset("telemetry", MockS3Store)
        .where("submissionDate") {
          case "20160606" => true
        }.where("docType") {
        case "main" => true
      }.records()

      assert(records.count() == 42)
    } finally {
      sc.stop()
    }
  }
}
