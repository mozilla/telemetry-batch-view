/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils

package object aggregation {
  def mean(values: Seq[Long]): Option[Double] = {
    if (values.size == 0) {
      None
    } else {
      Some(values.sum.toDouble/values.size)
    }
  }

  def weightedMean(pairs: Seq[(Long, Long)]): Option[Double] = {
    val ttl_weight = pairs.foldLeft(0L)((acc, pair) => acc + pair._2)
    val sum_prod = pairs.foldLeft(0.0)((acc, pair) => acc + (pair._1 * pair._2))

    if (ttl_weight > 0) {
      Some(sum_prod / ttl_weight)
    } else {
      None
    }
  }

  def weightedMode[A](pairs: Seq[(A, Long)]): Option[A] = {
    if (pairs.size == 0) {
      None
    } else {
      val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
      Some(agg.maxBy(_._2)._1)
    }
  }
}

