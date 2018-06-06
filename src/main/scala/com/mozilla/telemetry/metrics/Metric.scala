/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.metrics

import com.mozilla.telemetry.utils.MainPing

trait MetricDefinition {
  val keyed: Boolean
  val process: Option[String]
  val originalName: String
  val isCategoricalMetric: Boolean
}

class MetricsClass {
  def getProcesses(definitionProcesses: List[String]): List[String] = {
    definitionProcesses.flatMap{
      _ match {
        case "main" => "parent" :: Nil
        case "all" => MainPing.DefaultProcessTypes
        case "all_child" | "all_childs" | "all_children" =>
          MainPing.DefaultProcessTypes.filter(_ != "parent")
        case o => o :: Nil
      }
    }.distinct
  }
}
