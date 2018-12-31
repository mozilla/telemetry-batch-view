/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.untrustedmodules

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.pings.Payload
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

class UntrustedModulesViewTest extends FlatSpec with DataFrameSuiteBase with Matchers {

  "UntrustedModules view builder" should "convert combined_stacks" in {
    import spark.implicits._

    // serialized from production Parquet file
    val sparkSerializedCombinedStacksField =
      """{
        |  "combined_stacks": {
        |    "memory_map": [
        |      {
        |        "module_name": "mozglue.pdb",
        |        "debug_id": "C8E2BEB8F43EEFD67ED6B526CB91CF2D1"
        |      },
        |      {
        |        "module_name": "kernelbase.pdb",
        |        "debug_id": "FDD773B7779EB9AA6B4C9A5B5C210B501"
        |      }
        |    ],
        |    "stacks": [
        |      [
        |        {
        |          "module_index": 0,
        |          "module_offset": 21244
        |        },
        |        {
        |          "module_index": 1,
        |          "module_offset": 170560
        |        }
        |      ]
        |    ]
        |  }
        |}""".stripMargin('|').replaceAll("\n", "").replaceAll(" ", "")

    val expectedJson = parse(
      """{
        |  "memoryMap": [
        |    [
        |      "mozglue.pdb",
        |      "C8E2BEB8F43EEFD67ED6B526CB91CF2D1"
        |    ],
        |    [
        |      "kernelbase.pdb",
        |      "FDD773B7779EB9AA6B4C9A5B5C210B501"
        |    ]
        |  ],
        |  "stacks": [
        |    [
        |      [
        |        0,
        |        21244
        |      ],
        |      [
        |        1,
        |        170560
        |      ]
        |    ]
        |  ]
        |}""".stripMargin('|'))

    val combinedStacks = spark.read.json(Seq(sparkSerializedCombinedStacksField).toDS).as[Payload].collect().head.combined_stacks

    val stacksJson = parse(UntrustedModulesView.combinedStackToJson(combinedStacks))

    stacksJson shouldEqual expectedJson
  }
}
