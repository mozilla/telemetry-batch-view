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
