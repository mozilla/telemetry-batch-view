/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.statistics

import com.mozilla.telemetry.experiments.analyzers.{HistogramPoint, MetricAnalysis, Statistic}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{mean, stddev_pop, count}

import scala.annotation.tailrec
import scala.collection.Map


sealed abstract class DescriptiveStatistic(actual: Map[Long, HistogramPoint],
                                           jackknifed: Option[Dataset[MetricAnalysis]] = None)
  extends java.io.Serializable {
  val Alpha = 0.01
  val name: String
  lazy val total = actual.values.map(_.count).sum

  def run(histogram: Map[Long, HistogramPoint] = actual): Double

  def confidenceIntervals: Option[(Double, Double)] = {
    // I'd prefer not to use spark for this, but unfortunately for some of our high-variance
    // scalars, collecting all the aggregates to one machine actually causes OOM issues
    jackknifed.map { ds =>
      import ds.sparkSession.implicits._
      val (jkMean, jkStdDev, n) = ds
        .map(m => run(m.histogram))
        .agg(mean("value"), stddev_pop("value"), count("*"))
        .first() match {
        case Row(m: Double, s: Double, n: Long) => (m, s, n)
        case _ => throw new Exception("Unexpected jackknife statistics results")
      }

      // 99% confidence interval is the standard z-score formula assuming approx normal distribution for pseudostats
      val z99 = ZScore99 * (jkStdDev * scala.math.sqrt(n.toDouble - 1.0))
      val confIntervals = (jkMean - z99, jkMean + z99)
      confIntervals
    }
  }

  def asStatistic: Statistic = {
    val c = confidenceIntervals
    Statistic(None, name, run(), c.map(_._1), c.map(_._2), Some(Alpha))
  }
}

case class Mean(actual: Map[Long, HistogramPoint], jackknifed: Option[Dataset[MetricAnalysis]] = None)
  extends DescriptiveStatistic(actual, jackknifed) {
  val name = "Mean"
  def run(histogram: Map[Long, HistogramPoint] = actual): Double = {
    Mean.run(histogram)
  }
}

object Mean {
  def run(h: Map[Long, HistogramPoint]): Double = {
    val total = h.values.map(_.count).sum
    h.map { case (bucket, point) => bucket * point.count }.sum / total
  }
}

case class Percentile(actual: Map[Long, HistogramPoint],
                      percentile: Double,
                      name: String,
                      jackknifed: Option[Dataset[MetricAnalysis]] = None)
  extends DescriptiveStatistic(actual, jackknifed) {
  def run(histogram: Map[Long, HistogramPoint] = actual): Double = Percentile.run(percentile)(histogram)
}

object Percentile {
  def run(percentile: Double)(h: Map[Long, HistogramPoint]): Double = {
    val total = h.values.map(_.count).sum
    val i = total * percentile
    val rem = i % 1

    if (rem == 0) {
      getNthValue(i.toLong, h).toDouble
    } else {
      // This scales the percentile steps linearly, which is not quite accurate for exponential histograms, esp
      // for, say, 95th percentile calculations. Fwiw, this behavior is the default in Pandas
      getNthValue(i.floor.toLong, h) * (1 - rem) + getNthValue(i.ceil.toLong, h) * rem
    }
  }

  protected def getNthValue(n: Long, h: Map[Long, HistogramPoint]): Long = {
    @tailrec def getNthValue(n: Long, l: List[(Long, Long)], cumulative: Long): Long = {
      val (bucket, count) = l.head
      if (cumulative + count >= n) bucket else getNthValue(n, l.tail, cumulative + count)
    }

    getNthValue(n, h.toList.sortBy(_._1).map { case (bucket, point) => (bucket, point.count.toLong) }, 0L)
  }
}

case class DescriptiveStatistics(m: MetricAnalysis, jackknifed: Option[Dataset[MetricAnalysis]] = None) {
  lazy val statisticsList = List(
    Mean(m.histogram, jackknifed).asStatistic,
    Percentile(m.histogram, 0.5, "Median", jackknifed).asStatistic,
    Percentile(m.histogram, 0.1, "10th Percentile", jackknifed).asStatistic,
    Percentile(m.histogram, 0.2, "20th Percentile", jackknifed).asStatistic,
    Percentile(m.histogram, 0.3, "30th Percentile", jackknifed).asStatistic,
    Percentile(m.histogram, 0.4, "40th Percentile", jackknifed).asStatistic,
    Percentile(m.histogram, 0.6, "60th Percentile", jackknifed).asStatistic,
    Percentile(m.histogram, 0.7, "70th Percentile", jackknifed).asStatistic,
    Percentile(m.histogram, 0.8, "80th Percentile", jackknifed).asStatistic,
    Percentile(m.histogram, 0.9, "90th Percentile", jackknifed).asStatistic
  )

  def getStatistics: List[Statistic] = {
    statisticsList
  }
}
