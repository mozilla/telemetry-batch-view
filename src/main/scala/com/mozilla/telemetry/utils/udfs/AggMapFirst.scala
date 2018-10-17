/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils.udfs

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AggMapFirst() extends UserDefinedAggregateFunction {
  /**
    * Aggregates a Map[String,String] column by choosing the first non-null
    * value for each key
    **/

  override def inputSchema: StructType = StructType(
    Array(
      StructField(
        "value",
        // This is the actual input type
        MapType(StringType, StringType)
      )
    )
  )

  override def bufferSchema: StructType = StructType(
    List(StructField("map", MapType(StringType, StringType)))
  )

  override def dataType: DataType = MapType(StringType, StringType)

  override def deterministic: Boolean = false

  // empty buffer when the aggregation key changes
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map()
  }

  // add only keys with non-null values from input that are missing in buffer
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = input.getMap(0).filter { p: (String, String) => p._2 != null } ++
        buffer.getMap(0)
    }
  }

  // add only keys from buffer2 that are missing in buffer1
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer2.getMap(0) ++ buffer1.getMap(0)
  }

  // the buffer is the output value
  override def evaluate(buffer: Row): Any = buffer.getMap(0)
}
