package com.mozilla.telemetry.experiments.analyzers

import org.apache.spark.sql.Row
import scala.annotation.tailrec


case class MetricAggregate(values: Seq[Long], keys: List[Int]) {
  val n: Long = values.sum
  val zipped: List[(Int, Long)] = keys zip values

  val getSummaryStats: List[(String, Double)] = {
    List(
      ("mean", mean),
      ("median", median),
      ("standard deviation", standardDeviation)
    )
  }

  def getSchematizedSummaryStats: List[Row] = {
    getSummaryStats.map {
      case (name: String, value: Double) => Row(null, name, value, null, null, null, null)
    }
  }

  def mean: Double = {
    // TODO: fix this for linear + exponential
    zipped.map {case (k, v) => k * v}.sum.toDouble / n
  }

  def median: Double = {
    // TODO: fix this for linear + exponential
    (getNthValue(n / 2).toDouble + getNthValue((n / 2) + (n % 2)).toDouble) / 2
  }

  def standardDeviation: Double = {
    // TODO: fix this for linear + exponential
    val m = mean
    scala.math.sqrt(zipped.map {case (k, v) => scala.math.pow(k.toDouble - m, 2) * v}.sum / n)
  }

  def getNthValue(i: Long): Int = {
    @tailrec def _getNthValue(i: Long, l: List[(Int, Long)], cumulative: Long): Int = {
      val (k, v) = l.head
      if (cumulative + v >= i) k else _getNthValue(i, l.tail, cumulative + v)
    }

    _getNthValue(i, zipped, 0L)
  }
}