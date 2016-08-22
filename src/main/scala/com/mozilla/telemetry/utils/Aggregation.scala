package com.mozilla.telemetry.utils

import com.mozilla.telemetry.views.{Longitudinal, CrossSectional}
package object Aggregation{
  def weightedMode(values: Seq[String], weights: Seq[Long]): Option[String] = {
    if (values.size > 0 && values.size == weights.size) {
      val pairs = values zip weights
      val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
      Some(agg.maxBy(_._2)._1)
    } else {
      Option(null)
    }
  }

  def modalCountry(row: Longitudinal): Option[String] = {
    (row.geo_country, row.session_length) match {
      case (Some(gc), Some(sl)) => weightedMode(gc, sl)
      case _ => Option(null)
    }
  } 

  def generateCrossSectional(base: Longitudinal): CrossSectional = {
    val output = CrossSectional(base.client_id, modalCountry(base))
    output
  }
}
