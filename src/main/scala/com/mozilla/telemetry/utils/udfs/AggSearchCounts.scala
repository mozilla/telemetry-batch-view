/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils.udfs

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AggSearchCounts(validSources: List[String], prefix: String = "search_count_") extends UserDefinedAggregateFunction {
  /**
    * Aggregates the search_counts column in main_summary by taking the sum of
    * valid search counts by source, and creating a struct that maps source to
    * count with an additional field "all" that is the sum of the others. The
    * output column should be exploded to avoid issues with schema evolution in
    * athena. A search count is valid if it has count >= 0 and source is listed
    * in validSources. prefix is prepended to field names within the struct to
    * make it safe for star expansion. The input column must have the schema
    * List[Struct[engine: String, source: String, count: Long] ].
    **/

  private val numSources = validSources.length

  override def inputSchema: StructType = StructType(
    Array(
      StructField(
        "value",
        // This is the actual input type
        ArrayType(
          StructType(
            Array(
              StructField("engine", StringType),
              StructField("source", StringType),
              StructField("count", LongType)
            )
          )
        )
      )
    )
  )

  override def bufferSchema: StructType = StructType(
    List(StructField(prefix + "all", LongType)) ++
      validSources.map{c => StructField(prefix + c, LongType)}
  )

  override def dataType: DataType = bufferSchema

  // sum doesn't care about ordering
  override def deterministic: Boolean = true

  // zeroes out the buffer when the aggregation key changes
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    (0 to numSources).foreach { index =>
      buffer(index) = 0L
    }
  }

  // adds valid sources from Row to the buffer
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      input.getSeq(0).foreach { search_struct: Row =>
        val source: String = search_struct.getString(1)
        if (!search_struct.isNullAt(2)) {
          val count: Long = search_struct.getLong(2)
          if (count > 0 && validSources.contains(source)) {
            buffer(0) = buffer.getLong(0) + count
            // index is offset by one because the first field in the buffer is "all"
            val index = validSources.indexOf(source) + 1
            buffer(index) = buffer.getLong(index) + count
          }
        }
      }
    }
  }

  // merging two buffers results in the sum of values for each key
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    (0 to numSources).foreach { index =>
      buffer1(index) = buffer1.getLong(index) + buffer2.getLong(index)
    }
  }

  // the buffer is the output value
  override def evaluate(buffer: Row): Any = buffer
}
