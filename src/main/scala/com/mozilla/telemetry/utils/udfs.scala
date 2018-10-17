/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils

import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._
import org.apache.spark.sql.SparkSession


object UDFs{
  val HllCreate = "hll_create"
  val HllCardinality = "hll_cardinality"
  val HllMerge = new HyperLogLogMerge
  val FilteredHllMerge = new FilteredHyperLogLogMerge

  private def bucketed(value: Float, splits: Seq[Int]): Int = splits.collectFirst {
    case e if value <= e => e
  }.getOrElse(splits.max + 1)

  implicit class MozUDFs(spark: SparkSession) {
    def registerUDFs: Unit = {
      spark.udf.register(HllCreate, hllCreate _)
      spark.udf.register(HllCardinality, hllCardinality _)
      spark.udf.register("bucketed", bucketed _)
    }
  }

}
