package telemetry.spark.sql.aggregates

import com.twitter.algebird.{Bytes, DenseHLL, HyperLogLog}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, MutableAggregationBuffer}
import org.apache.spark.sql.types._

class HyperLogLogMerge extends UserDefinedAggregateFunction {
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
    val hll = HyperLogLog.fromBytes(input.getAs[Array[Byte]](0)).toDenseHLL

    if (buffer(0) != null) {
      hll.updateInto(buffer.getAs[Array[Byte]](0))
    } else {
      buffer(0) = hll.v.array
      buffer(1) = hll.bits
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
    val state = new DenseHLL(buffer.getAs[Int](1), new Bytes(buffer.getAs[Array[Byte]](0)))
    com.twitter.algebird.HyperLogLog.toBytes(state)
  }
}
