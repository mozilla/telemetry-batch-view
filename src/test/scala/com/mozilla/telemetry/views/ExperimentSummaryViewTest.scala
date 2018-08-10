/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.views.ExperimentSummaryView
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}
import org.json4s.jackson.JsonMethods.parse
import org.joda.time.DateTime

import scala.io.Source

case class ExperimentMainSummary(document_id: String, client_id: String,
                                 experiments: Option[scala.collection.Map[String, String]], sample_id: String = "42")


class ExperimentSummaryViewTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  "Experiment records" can "be exploded from MainSummary" in {
    sc.setLogLevel("WARN")

    import spark.implicits._

    val m = ExperimentMainSummary(
      "6609b4d8-94d4-4e87-9f6f-80183079ff1b",
      "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f",
      Some(Map("experiment1" -> "branch1", "experiment2" -> "branch2"))
    )

    val pings: DataFrame = Seq(
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

    ExperimentSummaryView.writeExperiments(testMainLocation, testExperimentsLocation, "20170101", testExperimentsList, spark, 500000)

    val actual = spark.read.parquet(testExperimentsLocation)
      .orderBy(col("document_id"), col("experiment_id"))
      .collect()
      .toList

    val expected = List(
      Row(
        "6609b4d8-94d4-4e87-9f6f-80183079ff1b",
        "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f",
        Map("experiment1" -> "branch1", "experiment2" -> "branch2"),
        "42",
        "branch1",
        "experiment1",
        20170101),
      Row(
        "72062950-3daf-450e-adfd-58eda3151a97",
        "baedfe78-676e-440e-98b4-a4066657ded1",
        Map("experiment1" -> "branch2"),
        "42",
        "branch2",
        "experiment1",
        20170101)
    )

    assert(actual == expected)
  }

  it can "be restricted to one sample ID" in {
    sc.setLogLevel("WARN")

    import spark.implicits._

    val m = ExperimentMainSummary(
      "6609b4d8-94d4-4e87-9f6f-80183079ff1b",
      "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f",
      Some(Map("experiment1" -> "branch1", "experiment2" -> "branch2"))
    )

    val pings: DataFrame = Seq(
      m.copy(sample_id="1"),
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
        experiments = Some(Map("experiment1" -> "branch2"))
      ),
      m.copy(
        client_id = "565a1d46-7320-43f9-93a8-1cc8586fad67",
        document_id = "72062950-3daf-450e-adfd-58eda3151a97",
        experiments = Some(Map("experiment1" -> "branch2")))
    ).toDS().toDF()

    val testMainLocation = com.mozilla.telemetry.utils.temporaryFileName().toString.replace("file:", "")
    pings.write.parquet(testMainLocation)

    val testExperimentsLocation = com.mozilla.telemetry.utils.temporaryFileName().toString.replace("file:", "")
    val testExperimentsList = List("experiment1")

    ExperimentSummaryView.writeExperiments(testMainLocation, testExperimentsLocation, "20170101", testExperimentsList, spark, 500000, Some("1"))

    val actual = spark.read.parquet(testExperimentsLocation)
      .orderBy(col("document_id"), col("experiment_id"))
      .collect()
      .toList

    val expected = List(
      Row(
        "6609b4d8-94d4-4e87-9f6f-80183079ff1b",
        "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f",
        Map("experiment1" -> "branch1", "experiment2" -> "branch2"),
        "1",
        "branch1",
        "experiment1",
        20170101)
    )

    assert(actual == expected)
  }

  it can "be restricted to a specified number of input rows" in {
    sc.setLogLevel("WARN")

    import spark.implicits._

    val m = ExperimentMainSummary(
      "6609b4d8-94d4-4e87-9f6f-80183079ff1b",
      "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f",
      Some(Map("experiment1" -> "branch1", "experiment2" -> "branch2"))
    )

    val pings: DataFrame = Seq(
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
        experiments = Some(Map("experiment1" -> "branch2"))
      ),
      m.copy(
        client_id = "565a1d46-7320-43f9-93a8-1cc8586fad67",
        document_id = "72062950-3daf-450e-adfd-58eda3151a97",
        experiments = Some(Map("experiment1" -> "branch2")))
    ).toDS().toDF()

    val testMainLocation = com.mozilla.telemetry.utils.temporaryFileName().toString.replace("file:", "")
    pings.write.parquet(testMainLocation)

    val testExperimentsLocation = com.mozilla.telemetry.utils.temporaryFileName().toString.replace("file:", "")
    val testExperimentsList = List("experiment1")

    ExperimentSummaryView.writeExperiments(testMainLocation, testExperimentsLocation, "20170101", testExperimentsList, spark, 500000, None, Some(1))

    val actual = spark.read.parquet(testExperimentsLocation)
      .orderBy(col("document_id"), col("experiment_id"))
      .collect()
      .toList

    val expected = List(
      Row(
        "6609b4d8-94d4-4e87-9f6f-80183079ff1b",
        "25a00eb7-2fd8-47fd-8d3f-223af3e5c68f",
        Map("experiment1" -> "branch1", "experiment2" -> "branch2"),
        "42",
        "branch1",
        "experiment1",
        20170101)
    )

    assert(actual == expected)
  }

  "Experiment recipe list" can "be extracted" in {
    val apiFixturePath = getClass.getResource("/normandy_api_result.json").getPath()
    val json = parse(Source.fromFile(apiFixturePath).mkString)

    val expected831 = Set("shield-public-infobar-display-bug1368141", "shield-lazy-client-classify")
    val expected830 = expected831 + "nightly-nothing-burger-1"

    assert(ExperimentSummaryView.getExperimentList(json, new DateTime(2017, 8, 30, 0, 0).toDate).toSet == expected830)
    assert(ExperimentSummaryView.getExperimentList(json, new DateTime(2017, 8, 31, 0, 0).toDate).toSet == expected831)
  }

  it can "be limited by option" in {
    val apiFixturePath = getClass.getResource("/normandy_api_result.json").getPath()
    val json = parse(Source.fromFile(apiFixturePath).mkString)

    val aug30 = new DateTime(2017, 8, 30, 0, 0).toDate
    val expected = Set("shield-public-infobar-display-bug1368141")

    assert(ExperimentSummaryView.getExperimentList(json, aug30, Some(1)).toSet == expected)
  }

}
