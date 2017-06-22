package com.mozilla.telemetry

import com.mozilla.telemetry.views.ExperimentSummaryView
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

case class ExperimentMainSummary(document_id: String, client_id: String,
                                 experiments: Option[scala.collection.Map[String, String]])


class ExperimentSummaryViewTest extends FlatSpec with Matchers{
  "Experiment records" can "be exploded from MainSummary" in {
    val spark = ExperimentSummaryView.getSparkSession()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    try {
      val m = ExperimentMainSummary(
        "6609b4d8-94d4-4e87-9f6f-80183079ff1b",
        "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f",
        Some(Map("experiment1" -> "branch1", "experiment2" -> "branch2"))
      )

      val pings : DataFrame = Seq(
        m,
        m,
        m.copy(
          document_id = "22539231-c1c6-4b9a-bed6-2a8d2e4e5e8c",
          experiments = Some(Map())),
        m.copy(
          document_id = "547b5406-8717-4696-b12b-b6c796bdbf8b",
          experiments = None),
        m.copy(
          client_id = "baedfe78-676e-440e-98b4-a4066657ded1",
          document_id = "72062950-3daf-450e-adfd-58eda3151a97",
          experiments = Some(Map("experiment1" -> "branch2"))),
        m.copy(
          client_id = "baedfe78-676e-440e-98b4-a4066657ded1",
          document_id = "72062950-3daf-450e-adfd-58eda3151a97",
          experiments = Some(Map("experiment1" -> "branch2")))
      ).toDS().toDF()

      val testMainLocation = com.mozilla.telemetry.utils.temporaryFileName().toString.replace("file:", "")
      pings.write.parquet(testMainLocation)

      val testExperimentsLocation = com.mozilla.telemetry.utils.temporaryFileName().toString.replace("file:", "")
      val testExperimentsList = List("experiment1")

      ExperimentSummaryView.writeExperiments(testMainLocation, testExperimentsLocation, "20170101", testExperimentsList, spark)

      val actual = spark.read.parquet(testExperimentsLocation)
        .orderBy(col("document_id"), col("experiment_id"))
        .collect()
        .toList

      val expected = List(
        Row(
          "6609b4d8-94d4-4e87-9f6f-80183079ff1b",
          "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f",
          Map("experiment1" -> "branch1", "experiment2" -> "branch2"),
          "branch1",
          "experiment1",
          20170101),
        Row(
          "72062950-3daf-450e-adfd-58eda3151a97",
          "baedfe78-676e-440e-98b4-a4066657ded1",
          Map("experiment1" -> "branch2"),
          "branch2",
          "experiment1",
          20170101)
      )

      assert(actual == expected)
    } finally {
      spark.stop()
    }
  }

  "Experiment recipe list" can "be extracted" in {
    val apiFixturePath = getClass.getResource("/normandy_api_result.json").getPath()
    val json = parse(Source.fromFile(apiFixturePath).mkString)

    val expected = List("shield-public-infobar-display-bug1368141",
                        "shield-lazy-client-classify",
                        "pref-flip-test-nightly-1")

    assert(ExperimentSummaryView.getExperimentList(json) == expected)
  }
}
