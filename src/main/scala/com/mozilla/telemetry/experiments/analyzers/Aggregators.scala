package com.mozilla.telemetry.experiments.analyzers

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.Map


abstract class MetricAggregator[T]
  extends Aggregator[PreAggregateRow[T], Map[T, Long], Map[Long, HistogramPoint]] {
  def zero: Map[T, Long] = Map[T, Long]()

  def reduce(b: Map[T, Long], s: PreAggregateRow[T]): Map[T, Long] = {
    s.metric match {
      case Some(m: Map[T, Long]) => addHistograms[T](b, m)
      case _ => b
    }
  }

  def merge(l: Map[T, Long], r: Map[T, Long]): Map[T, Long] = addHistograms[T](l, r)

  def outputEncoder: Encoder[Map[Long, HistogramPoint]] = ExpressionEncoder()
}

object BooleanAggregator extends MetricAggregator[Boolean] {
  def finish(b: Map[Boolean, Long]): Map[Long, HistogramPoint] = {
    val sum = b.values.sum.toDouble
    if (sum == 0) return Map.empty[Long, HistogramPoint]
    val f = b.getOrElse(false, 0L).toDouble
    val t = b.getOrElse(true, 0L).toDouble

    Map(0L -> HistogramPoint(f/sum, f, Some("False")), 1L -> HistogramPoint(t/sum, t, Some("True")))
  }

  def bufferEncoder: Encoder[Map[Boolean, Long]] = ExpressionEncoder()
}

object UintAggregator extends MetricAggregator[Int] {
  def finish(b: Map[Int, Long]): Map[Long, HistogramPoint] = {
    val sum = b.values.sum.toDouble
    if (sum == 0) return Map.empty[Long, HistogramPoint]
    b.map { case (k: Int, v) => k.toLong -> HistogramPoint(v.toDouble / sum, v.toDouble, None) }
  }

  def bufferEncoder: Encoder[Map[Int, Long]] = ExpressionEncoder()
}

object StringAggregator extends MetricAggregator[String] {
  def finish(b: Map[String, Long]): Map[Long, HistogramPoint] = {
    val sum = b.values.sum.toDouble
    if (sum == 0) return Map.empty[Long, HistogramPoint]
    // We can't assign real numeric indexes until we combine all the histograms across all branches
    // so just assign any number for now
    b.zipWithIndex.map {
      case ((l: String, v), i) => i.toLong -> HistogramPoint(v.toDouble / sum, v.toDouble, Some(l))
    }
  }

  def bufferEncoder: Encoder[Map[String, Long]] = ExpressionEncoder()
}
