package telemetry.spark.sql

import org.apache.spark.sql.functions.udf
import com.twitter.algebird.HyperLogLog

object functions {
  val hll_cardinality = udf((hll: Array[Byte]) => { HyperLogLog.fromBytes(hll).approximateSize.estimate })
}
