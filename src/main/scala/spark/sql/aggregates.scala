package telemetry.spark.sql.aggregates

import com.twitter.algebird.{Bytes, DenseHLL, HyperLogLogMonoid}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, MutableAggregationBuffer}
import org.apache.spark.sql.types._

class HyperLogLog(bitSize: Int) extends UserDefinedAggregateFunction {
  val monoid = new HyperLogLogMonoid(bits = bitSize)

  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", StringType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("count", BinaryType) :: Nil)

  def dataType: DataType = BinaryType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = monoid.zero.toDenseHLL.v.array
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val state = new DenseHLL(bitSize, new Bytes(buffer.getAs[Array[Byte]](0)))
    val x = monoid.toHLL(input.getAs[String](0))
    buffer(0) = monoid.plus(state, x).toDenseHLL.v.array
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val state1 = new DenseHLL(bitSize, new Bytes(buffer1.getAs[Array[Byte]](0)))
    val state2 = new DenseHLL(bitSize, new Bytes(buffer2.getAs[Array[Byte]](0)))
    buffer1(0) = monoid.plus(state1, state2).toDenseHLL.v.array
  }

  def evaluate(buffer: Row): Any = {
    val state = new DenseHLL(bitSize, new Bytes(buffer.getAs[Array[Byte]](0)))
    com.twitter.algebird.HyperLogLog.toBytes(state)
  }
}
