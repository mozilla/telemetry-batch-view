package com.mozilla.telemetry.utils

object Aggregation {
  def weightedMode(values: Seq[String], weights: Seq[Long]): String = {
    if (values.size != weights.size) {
      throw new IllegalArgumentException("Args to weighted mode must have the same length.")
    } else if (values.size == 0) {
      throw new IllegalArgumentException("Args to weighted mode must have length > 0.")
    } else {
      val pairs = values zip weights
      val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
      agg.maxBy(_._2)._1
    }
  }
}

