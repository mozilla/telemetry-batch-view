package com.mozilla.telemetry.metrics

import com.mozilla.telemetry.utils.MainPing

trait MetricDefinition {
  val keyed: Boolean
  val processes: List[String]
}

class MetricsClass {
  def getProcesses(definitionProcesses: List[String]): List[String] = {
    definitionProcesses.flatMap{
      _ match {
        case "main" => "parent" :: Nil
        case "all" => MainPing.ProcessTypes
        case "all_child" | "all_childs" | "all_children" =>
          MainPing.ProcessTypes.filter(_ != "parent")
        case o => o :: Nil
      }
    }.distinct
  }
}
