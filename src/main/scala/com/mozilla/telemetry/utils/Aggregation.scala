package com.mozilla.telemetry.utils

package object aggregation {
  def weightedMean(values: Seq[Option[Long]], weights: Seq[Long]): Option[Double] = {
    if (values.size == weights.size) {
      val clean_pairs = (values zip weights).map(pair => pair._1.map((_, pair._2))).flatten
      val ttl_weight = clean_pairs.foldLeft(0l)((acc, pair) => acc + pair._2)
      val sum_prod = clean_pairs.foldLeft(0.0)((acc, pair) => acc + (pair._1 * pair._2))

      if (ttl_weight > 0) {
        Some(sum_prod / ttl_weight)
      } else {
        None
      }
    } else {
      None
    }
  }

  def weightedMode[A](values: Seq[A], weights: Seq[Long]): Option[A] = {
    if ((values.size == 0) || (values.size != weights.size)) {
      None
    } else {
      val pairs = values zip weights
      val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
      Some(agg.maxBy(_._2)._1)
    }
  }
}

