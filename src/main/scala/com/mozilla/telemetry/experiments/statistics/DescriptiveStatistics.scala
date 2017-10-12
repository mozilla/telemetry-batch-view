package com.mozilla.telemetry.experiments.statistics

import com.mozilla.telemetry.experiments.analyzers.{HistogramPoint, MetricAnalysis, Statistic}

import scala.annotation.tailrec
import scala.collection.Map


sealed abstract class DescriptiveStatistic(h: Map[Long, HistogramPoint]) {
  val name: String
  lazy val total = h.values.map(_.count).sum

  def run: Double

  def asStatistic: Statistic = {
    Statistic(None, name, run)
  }
}

case class Mean(h: Map[Long, HistogramPoint]) extends DescriptiveStatistic(h) {
  val name = "Mean"
  def run: Double = {
    h.map { case (bucket, point) => bucket * point.count }.sum / total
  }
}

case class Percentile(h: Map[Long, HistogramPoint], percentile: Double, name: String)
  extends DescriptiveStatistic(h) {
  protected def percentileIndex: Double = {
    total * percentile
  }

  def run: Double =  {
    val i = percentileIndex
    val rem = i % 1

    if (rem == 0) {
      getNthValue(i.toLong).toDouble
    } else {
      // This scales the percentile steps linearly, which is not quite accurate for exponential histograms, esp
      // for, say, 95th percentile calculations. Fwiw, this behavior is the default in Pandas
      getNthValue(i.floor.toLong) * (1 - rem) + getNthValue(i.ceil.toLong) * rem
    }
  }

  protected def getNthValue(n: Long): Long = {
    @tailrec def _getNthValue(n: Long, l: List[(Long, Long)], cumulative: Long): Long = {
      val (bucket, count) = l.head
      if (cumulative + count >= n) bucket else _getNthValue(n, l.tail, cumulative + count)
    }

    _getNthValue(n, h.toList.sortBy(_._1).map { case (bucket, point) => (bucket, point.count.toLong)}, 0L)
  }
}

case class DescriptiveStatistics(m: MetricAnalysis) {
  lazy val statisticsList = List(
    Mean(m.histogram).asStatistic,
    Percentile(m.histogram, 0.5, "Median").asStatistic,
    Percentile(m.histogram, 0.25, "25th Percentile").asStatistic,
    Percentile(m.histogram, 0.75, "75th Percentile").asStatistic
  )

  def getStatistics: List[Statistic] = {
    statisticsList
  }
}
