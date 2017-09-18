package com.mozilla.telemetry.utils

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.math.{floor, log, pow}

/**
 * A small utility class to enable Consistent Hashing across
 * a set of partitions.
 *
 * Best used in multiples of two, else the partitions will
 * not be equally sized. If the hash does not result in
 * well distributed data, the partitions will also not
 * be equally sized.
 */
object ConsistentPartitioner {

  /*
   * Get the point in [0, 1] that represents this partition.
   *
   * To give an idea of the sequence that results, below are
   * the first 10 inputs and outputs. Note that at all powers
   * of 2, the range [0, 1] is evenly divided between all partitions.
   *
   * For example: With N=2 (so n={0, 1}), you have two partitions.
   * One in the range [0.0, 0.5] and one in the range [0.5, 1.0].
   *
   * Say we then want to add some more partitions, because these
   * are becoming too large. If we now set N=4, n={0, 1, 2, 3}.
   * Using the chart below, we can see that n=0 and n=1 have the
   * same values - 1.0 and 0.5, respectively. n=2 then adds
   * 0.25, and n=3 adds 0.75. Our partition values are now
   * {0.25, 0.5, 0.75, 1.0}.
   *
   * n=2 then splits the region [0.0, 0.5] into two
   * where values in [0.0, 0.25] are now shuffled into
   * partition 2, and values [0.25, 0.5] are shuffled
   * partition 1. n=3 splits [0.5, 1.0] into two as well
   * and any values in [0.5, 0.75] will be shuffled into
   * partition 3.
   *
   * Note that: before adding more partitions, values in [0.0, 0.5]
   * were all shuffled into partition 1. After adding more partitions,
   * partition 1 has values in [0.25, 0.5]; this means that to join
   * with that data, partition 1 still only needs to check partition 1.
   *
   * Indeed, for any consistently partitioned dataset P1 with M partitions
   * joining a consistenly partitioned dataset P2 with N partitions:
   * if M >= N, each partition in P1 only needs to check one partition in P2
   * if M < N, each partition in P1 will need to check at most N/2 in P2
   *  (if M%2 == 0 and N%2 == 0)
   *
   *
   * | n  | output |
   * --------------
   * | 0  | 1.0    |
   * | 1  | 0.5    |
   * | 2  | 0.25   |
   * | 3  | 0.75   |
   * | 4  | 0.125  |
   * | 5  | 0.375  |
   * | 6  | 0.625  |
   * | 7  | 0.875  |
   * | 8  | 0.0625 |
   * | 9  | 0.1875 |
   */
  def getValue(n: Int): Double = {
    if(n == 0){
      return 1.0
    }

    val base = floor(log(n) / log(2))
    val pow2Base = pow(2, base)
    val offset = 0.5 / pow2Base
    val add = (n - pow2Base) / pow2Base

    offset + add
  }

  /*
   * Given N partitions, get their values in [0, 1] space
   * and output a sorted list of those values.
   */
  def getPartitionRing(numPartitions: Int): Seq[Double] = {
    (0 to (numPartitions - 1)).map(getValue _).sorted
  }

  /*
   * Given a sorted list of partition values in [0, 1], return a function
   * that takes a sample value and outputs the partition that sample belongs in.
   */
  def getPartitioningFunc(numPartitions: Int, hashFunc: String => Double, defaultPartition: Int = 0): UserDefinedFunction = {
    val ring = getPartitionRing(numPartitions).zipWithIndex

    udf(
      (input: String) =>
        input match {
          case null =>
            defaultPartition
          case o =>
            val hash = hashFunc(o)
            ring.collectFirst{ case (e, i) if 0 <= hash && hash <= e => i }
              .getOrElse(defaultPartition)
        }
    )
  }
}
