package com.mozilla.telemetry.experiments.statistics

import com.mozilla.telemetry.experiments.analyzers.{HistogramPoint, MetricAnalysis, Statistic}

import scala.annotation.tailrec
import scala.collection.Map


sealed abstract class DescriptiveStatistic(actual: Map[Long, HistogramPoint],
                                           resampled: Option[List[Map[Long, HistogramPoint]]] = None) {
  val Alpha = 0.05
  val name: String
  lazy val total = actual.values.map(_.count).sum

  def run(h: Map[Long, HistogramPoint] = actual): Double

  def confidenceIntervals: Option[(Double, Double)] = {
    resampled.map {
      l => val stats = l.map(run(_)).sorted
        // currently using the super simple percentile version instead of the BCA algorithm
        val halfAlpha = Alpha / 2
        val lowIdx = scala.math.floor(stats.length * halfAlpha).toInt
        val highIdx = scala.math.ceil(stats.length * (1 - halfAlpha)).toInt
        (stats(lowIdx), stats(highIdx))
    }
  }

  def asStatistic: Statistic = {
    val c = confidenceIntervals
    Statistic(None, name, run(), c.map(_._1), c.map(_._2), c.map(_ => Alpha))
  }
}

case class Mean(actual: Map[Long, HistogramPoint], resampled: Option[List[Map[Long, HistogramPoint]]] = None)
  extends DescriptiveStatistic(actual, resampled) {
  val name = "Mean"
  def run(h: Map[Long, HistogramPoint] = actual): Double = {
    h.map { case (bucket, point) => bucket * point.count }.sum / total
  }
}

case class Percentile(actual: Map[Long, HistogramPoint],
                      percentile: Double,
                      name: String,
                      resampled: Option[List[Map[Long, HistogramPoint]]] = None)
  extends DescriptiveStatistic(actual, resampled) {
  protected def percentileIndex: Double = {
    total * percentile
  }

  def run(h: Map[Long, HistogramPoint] = actual): Double =  {
    val i = percentileIndex
    val rem = i % 1

    if (rem == 0) {
      getNthValue(i.toLong).toDouble
    } else {
      // This scales the percentile steps linearly, which is not quite accurate for exponential histograms, esp
      // for, say, 95th percentile calculations. Fwiw, this behavior is the default in Pandas
      getNthValue(i.floor.toLong, h) * (1 - rem) + getNthValue(i.ceil.toLong, h) * rem
    }
  }

  protected def getNthValue(n: Long, h: Map[Long, HistogramPoint] = actual): Long = {
    @tailrec def _getNthValue(n: Long, l: List[(Long, Long)], cumulative: Long): Long = {
      val (bucket, count) = l.head
      if (cumulative + count >= n) bucket else _getNthValue(n, l.tail, cumulative + count)
    }

    _getNthValue(n, h.toList.sortBy(_._1).map { case (bucket, point) => (bucket, point.count.toLong)}, 0L)
  }
}

case class DescriptiveStatistics(m: MetricAnalysis, resampled: Option[List[MetricAnalysis]] = None) {
  lazy val resampledHistograms = resampled.map(_.map(_.histogram))
  lazy val statisticsList = List(
    Mean(m.histogram, resampledHistograms).asStatistic,
    Percentile(m.histogram, 0.5, "Median", resampledHistograms).asStatistic,
    Percentile(m.histogram, 0.25, "25th Percentile", resampledHistograms).asStatistic,
    Percentile(m.histogram, 0.75, "75th Percentile", resampledHistograms).asStatistic
  )

  def getStatistics: List[Statistic] = {
    statisticsList
  }
}
