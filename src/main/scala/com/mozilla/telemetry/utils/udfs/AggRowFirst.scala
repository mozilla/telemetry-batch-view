/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils.udfs


import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AggRowFirst[IndexDataType](schema: StructType, idIndex: Int, indexSqlDataType: DataType)
  extends UserDefinedAggregateFunction {
  /**
    * Aggregates struct array column by choosing the first non-null
    * value for each unique id
    **/

  override def inputSchema: StructType = StructType(List(StructField("value", ArrayType(schema))))

  override def bufferSchema: StructType = StructType(
    List(StructField("map", MapType(indexSqlDataType, schema)))
  )

  override def dataType: DataType = ArrayType(schema)

  override def deterministic: Boolean = false

  // empty buffer when the aggregation key changes
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map()
  }

  // add only keys with non-null values from input that are missing in buffer
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      // the input column is a sequence of rows with the addon schema
      buffer(0) = input.getSeq(0).map { p: Row => p.getAs[IndexDataType](idIndex) -> p }.toMap ++
        buffer.getMap(0)
    }
  }

  // add only keys from buffer2 that are missing in buffer1
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer2.getMap(0) ++ buffer1.getMap(0)
  }

  // Turn it back into a Seq of Addons
  override def evaluate(buffer: Row): Any = buffer.getMap[IndexDataType, Row](0).map(_._2).toSeq
}

