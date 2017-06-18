package com.mozilla.telemetry.utils

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.twitter.algebird.{Bytes, DenseHLL, HyperLogLog}
import com.mozilla.spark.sql.hyperloglog.functions._

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

/**
 * This class fixes HyperLogLogMerge when there are no
 * rows, or when input rows are NULL.
 */
class HyperLogLogMerge extends UserDefinedAggregateFunction {

  /**
   * This HLL instance is has zero counts.
   *
   * scala> (new DenseHLL(12, new Bytes(Array.fill[Byte](1 << 12)(0)))).approximateSize.estimate
   * res0: Long = 0
   */
  val emptyHll = new DenseHLL(12, new Bytes(Array.fill[Byte](1 << 12)(0)))

  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", BinaryType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("count", BinaryType) ::
    StructField("bits", IntegerType) :: Nil)

  def dataType: DataType = BinaryType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
    buffer(1) = 0
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input(0) != null) {
      val hll = HyperLogLog.fromBytes(input.getAs[Array[Byte]](0)).toDenseHLL

      if (buffer(0) != null) {
        hll.updateInto(buffer.getAs[Array[Byte]](0))
      } else {
        buffer(0) = hll.v.array
        buffer(1) = hll.bits
      }
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1(0) == null) {
      buffer1(0) = buffer2(0)
      buffer1(1) = buffer2(1)
    } else if (buffer1(0) != null && buffer2(0) != null) {
      val state2 = new DenseHLL(buffer2.getAs[Int](1), new Bytes(buffer2.getAs[Array[Byte]](0)))
      state2.updateInto(buffer1.getAs[Array[Byte]](0))
    }
  }

  def evaluate(buffer: Row): Any = {
    val state = buffer(0) match {
      case null => emptyHll
      case o => new DenseHLL(buffer.getAs[Int](1), new Bytes(buffer.getAs[Array[Byte]](0)))
    }
    com.twitter.algebird.HyperLogLog.toBytes(state)
  }
}

/**
 * This class adds the capability to take in
 * another Boolean column and only adds the
 * associated hll if the Boolean column is `True`.
 */
class FilteredHyperLogLogMerge extends HyperLogLogMerge {

  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", BinaryType) :: StructField("filtered", BooleanType) :: Nil)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input(0) != null && input(1) != null && input.getAs[Boolean](1)) {
      val hll = HyperLogLog.fromBytes(input.getAs[Array[Byte]](0)).toDenseHLL

      if (buffer(0) != null) {
        hll.updateInto(buffer.getAs[Array[Byte]](0))
      } else {
        buffer(0) = hll.v.array
        buffer(1) = hll.bits
      }
    }
  }
}

object UDFs{
  val HllCreate = "hll_create"
  val HllCardinality = "hll_cardinality"
  val HllMerge = new HyperLogLogMerge
  val FilteredHllMerge = new FilteredHyperLogLogMerge

  implicit class MozUDFs(spark: SparkSession) {
    def registerUDFs: Unit = {
      spark.udf.register(HllCreate, hllCreate _)
      spark.udf.register(HllCardinality, hllCardinality _)
    }
  }
}
