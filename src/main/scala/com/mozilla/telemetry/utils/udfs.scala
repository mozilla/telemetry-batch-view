/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import com.mozilla.spark.sql.hyperloglog.functions._
import com.mozilla.spark.sql.hyperloglog.aggregates._

import scala.annotation.tailrec

class CollectList(inputStruct: StructType, orderCols: List[String], maxLength: Option[Int]) extends UserDefinedAggregateFunction {
  /**
   * This transforms the groups to arrays. It is similar to collect_list, but
   * we can't use that because it throws out null values, which we want to retain.
   * Additionally, it sorts and trims them before outputting.
   **/

  private val outputSchema = StructType(
    inputStruct.fields.map{
      sf => StructField(sf.name, ArrayType(sf.dataType), sf.nullable)
    }
  )

  private val numFields = inputStruct.fields.length

  override def inputSchema: StructType = inputStruct

  override def bufferSchema: StructType = outputSchema

  override def dataType: DataType = outputSchema

  override def deterministic: Boolean = false

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    (0 to numFields - 1).foreach{
      n => buffer(n) = Nil
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    (0 to numFields - 1).foreach{
      n => buffer(n) = buffer.getSeq(n) :+ input(n)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    (0 to numFields - 1).foreach{
      n => buffer1(n) = buffer1.getSeq(n) ++ buffer2.getSeq(n)
    }
  }

  /**
   * SORTING
   *
   * We want a descending, stable sort - this includes null values.
   * As such, our sorting is:
   *
   * case e1==null, e2==null  => given ordering
   * case e1, e2==null => e1 before e2
   * case e1==null, e2 => e2 before e1
   * case e1 > e2 => e1 before e2
   * case e1 < e2 => e2 before e1
   * case e1 == e2 => given ordering
   **/

  private def sortInt(arr: Seq[Integer])(e0: Int, e1: Int): Boolean = {
    val v1 = arr(e0)
    val v2 = arr(e1)
    (v1 != null) && ((v2 == null) || v1 > v2)
  }

  private def sortString(arr: Seq[String])(e0: Int, e1: Int): Boolean = {
    val v1 = arr(e0)
    val v2 = arr(e1)
    (v1 != null) && ((v2 == null) || v1 > v2)
  }

  private def sortLong(arr: Seq[java.lang.Long])(e0: Int, e1: Int): Boolean = {
    val v1 = arr(e0)
    val v2 = arr(e1)
    (v1 != null) && ((v2 == null) || v1 > v2)
  }

  private def sort(order: Seq[Int], orderType: DataType, seq: Seq[Any]): Seq[Int] = {
    orderType match {
      case _: IntegerType => order.sortWith(sortInt(seq.asInstanceOf[Seq[Integer]]))
      case _: StringType => order.sortWith(sortString(seq.asInstanceOf[Seq[String]]))
      case _: LongType => order.sortWith(sortLong(seq.asInstanceOf[Seq[java.lang.Long]]))
      case other =>
        throw new UnsupportedOperationException(s"Unsupported sort column type ${other.simpleString}")
    }
  }

  @tailrec
  private def rsort(input: Seq[Int], buffer: Row, orderColumns: Seq[String]): Seq[Int] = {
    orderColumns match {
      case Nil => input
      case oc => rsort(
        sort(input,
             inputSchema(orderColumns.head).dataType,
             buffer.getSeq(outputSchema.fieldIndex(orderColumns.head))),
        buffer,
        orderColumns.tail
      )
    }
  }

  private def sortTrimTypedArray[T](array: Seq[Any], sortOrder: Seq[Int]): Seq[T] = {
    val typedArray =
      array
      .asInstanceOf[Seq[T]]

    val sorted =
      sortOrder
      .map(typedArray)

    maxLength match {
      case Some(m) => sorted.take(m)
      case _ => sorted
    }
  }

  /* We need the specific type of the Seq. For a mapping of Spark Type => Scala Type,
   * see http://spark.apache.org/docs/latest/sql-programming-guide.html#data-types
   */
  private def sortTrimArrayBySeq(dataType: DataType, buffer: Row, arrayIndex: Int, sortOrder: Seq[Int]): Seq[Any] = {
    dataType match {
      case _: ByteType      => sortTrimTypedArray[Byte](buffer.getSeq(arrayIndex), sortOrder)
      case _: ShortType     => sortTrimTypedArray[Short](buffer.getSeq(arrayIndex), sortOrder)
      case _: IntegerType   => sortTrimTypedArray[Int](buffer.getSeq(arrayIndex), sortOrder)
      case _: LongType      => sortTrimTypedArray[Long](buffer.getSeq(arrayIndex), sortOrder)
      case _: FloatType     => sortTrimTypedArray[Float](buffer.getSeq(arrayIndex), sortOrder)
      case _: DoubleType    => sortTrimTypedArray[Double](buffer.getSeq(arrayIndex), sortOrder)
      case _: DecimalType   => sortTrimTypedArray[BigDecimal](buffer.getSeq(arrayIndex), sortOrder)
      case _: StringType    => sortTrimTypedArray[String](buffer.getSeq(arrayIndex), sortOrder)
      case _: BinaryType    => sortTrimTypedArray[Array[Byte]](buffer.getSeq(arrayIndex), sortOrder)
      case _: BooleanType   => sortTrimTypedArray[Boolean](buffer.getSeq(arrayIndex), sortOrder)
      case _: TimestampType => sortTrimTypedArray[Timestamp](buffer.getSeq(arrayIndex), sortOrder)
      case _: DateType      => sortTrimTypedArray[Date](buffer.getSeq(arrayIndex), sortOrder)
      case _: ArrayType     => sortTrimTypedArray[Seq[Any]](buffer.getSeq(arrayIndex), sortOrder)
      case _: MapType       => sortTrimTypedArray[Map[Any, Any]](buffer.getSeq(arrayIndex), sortOrder)
      case _: StructType    => sortTrimTypedArray[Row](buffer.getSeq(arrayIndex), sortOrder)
      case other            =>
        throw new UnsupportedOperationException(s"${other.simpleString} column types are not supported")
    }
  }

  private def getOrderedTrimmedArrays(buffer: Row, sortOrder: Seq[Int]): Seq[Seq[Any]] = {
    (0 to (inputSchema.length - 1)).toList.map{
        n => sortTrimArrayBySeq(inputSchema(n).dataType, buffer, n, sortOrder)
    }
  }

  private def trimAndSort(buffer: Row): Row = {
    val sortOrder = rsort((0 to (buffer.getSeq(0).length - 1)).toList, buffer, orderCols.reverse)
    Row.fromSeq(getOrderedTrimmedArrays(buffer, sortOrder))
  }

  override def evaluate(buffer: Row): Any = {
    trimAndSort(buffer)
  }
}

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
