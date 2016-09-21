package com.mozilla.telemetry.utils

package object aggregation {
  def mean(values: Seq[Int]) = {
    if (values.length > 0) {
      Some(values.sum.toDouble / values.length)
    } else {
      None
    }
  }

  def weightedMode[A](values: Seq[A], weights: Seq[Long]): A = {
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

