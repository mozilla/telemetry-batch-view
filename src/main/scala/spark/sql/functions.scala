package telemetry.spark.sql

import org.apache.spark.sql.functions.udf
import com.twitter.algebird.{HyperLogLog, HyperLogLogMonoid}

object functions {
  def hllCardinality(hll: Array[Byte]): Long = {
    HyperLogLog.fromBytes(hll).approximateSize.estimate 
  }

  // 12 bits corresponds to an error of 0.0163
  // See https://github.com/twitter/algebird/blob/0.10.0/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala#L194-L210
  def hllCreate(x: String): Array[Byte] = {
    val monoid = new HyperLogLogMonoid(bits = 12)
    HyperLogLog.toBytes(monoid.toHLL(x))
  }

  val hllCardinalityUDF = udf(hllCardinality _)
  val hllCreateUDF = udf(hllCreate _)
}
