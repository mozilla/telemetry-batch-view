/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.statistics

object StatisticalComputation {
  val percentileInts: Seq[Int] = 5 +: (10 to 90 by 10) :+ 95
  val percentiles: Seq[PercentileComputation] = percentileInts.map(PercentileComputation)
  val values: Seq[StatisticalComputation] = percentiles :+ MeanComputation
  val names: Seq[String] = values.map(_.name)
}

trait StatisticalComputation {
  def name: String
  def calculate(arr: Array[Double]): Double
}

object MeanComputation extends StatisticalComputation {
  override val name: String = "Mean"
  override def calculate(arr: Array[Double]): Double = {
    breeze.stats.mean(arr)
  }
}

/**
  * Object capturing the logic for naming and computing a particular percentile.
  *
  * @param pInt The desired percentile as an integer between 0 and 100
  */
case class PercentileComputation(pInt: Int) extends StatisticalComputation {
  val p: Double = pInt * 0.01

  override def name: String = pInt match {
    case 50 => "Median"
    case 11 | 12 | 13 => s"${pInt}th Percentile"
    case _ if pInt % 10 == 1 => s"${pInt}st Percentile"
    case _ if pInt % 10 == 2 => s"${pInt}nd Percentile"
    case _ if pInt % 10 == 3 => s"${pInt}rd Percentile"
    case _ => s"${pInt}th Percentile"
  }

  /**
    * Based on https://github.com/scalanlp/breeze/blob/releases/v1.0-RC2/math/src/main/scala/breeze/stats/DescriptiveStats.scala#L523
    *
    * We copy this code out of breeze because their implementation always sorts the array.
    * We call this function many times on a single array, so want to be able to sort once.
    *
    * The array must already be sorted.
    */
  override def calculate(arr: Array[Double]): Double = {
    if (p > 1 || p < 0) throw new IllegalArgumentException("p must be in [0,1]")
    // +1 so that the .5 == mean for even number of elements.
    val f = (arr.length + 1) * p
    val i = f.toInt
    if (i == 0) {
      arr.head
    }
    else if (i >= arr.length) {
      arr.last
    }
    else {
      arr(i - 1) + (f - i) * (arr(i) - arr(i - 1))
    }
  }
}

