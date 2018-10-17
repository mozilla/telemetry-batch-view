/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils.udfs

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AggMapSum() extends UserDefinedAggregateFunction {
  /**
    * Aggregates a Map[String,Integer] column by summing over each key
    **/

  override def inputSchema: StructType = StructType(
    Array(
      StructField(
        "value",
        // This is the actual input type
        MapType(StringType, IntegerType, true)
      )
    )
  )

  override def bufferSchema: StructType = StructType(
    List(StructField("map", MapType(StringType, IntegerType, false)))
  )

  override def dataType: DataType = MapType(StringType, IntegerType, false)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Int]()
  }

  // add only keys with non-null values from input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val bmap: scala.collection.Map[String, Int] = buffer.getMap(0)
      val imap = input.getAs[Map[String, Integer]](0)
      buffer(0) = bmap ++
        (imap
        .filter { p: (String, Integer) => p._2 != null }
        .map { case (k,v) => k -> (v.intValue + bmap.getOrElse(k, 0)) })
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: scala.collection.Map[String, Int] = buffer1.getMap(0)
    val map2: scala.collection.Map[String, Int] = buffer2.getMap(0)
    buffer1(0) = map1 ++
      map2.map { case (k,v) => k -> (v + map1.getOrElse(k, 0)) }
  }

  // the buffer is the output value
  override def evaluate(buffer: Row): Any = buffer.getMap(0)
}
