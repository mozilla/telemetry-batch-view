/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.analyzers

import breeze.stats.MeanAndVariance
import breeze.stats.distributions.Poisson
import com.mozilla.telemetry.experiments.statistics.ZScore99
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * See https://metrics.mozilla.com/protected/sguha/shield_bootstrap.html#definitions
  */
object EngagementMeasures {
  val mth  = "engagement_daily_hours"
  val mah  = "engagement_daily_active_hours"
  val muri = "engagement_hourly_uris"
  val mins = "engagement_intensity"
  val base: List[String] = List(mth, mah, muri)
  val derived: List[String] = List(mins)
  val all: List[String] = base ++ derived
}

object Percentiles {
  val ints: Seq[Int] = (1 to 9).map(_ * 10)
  val decimals: Seq[Double] = ints.map(_ * 0.01)
  val names: Seq[String] = ints.map {
    case 50 => "Median"
    case p => s"${p}th Percentile"
  }
}

/**
  * Introduced for bug 1460090, based on a proof-of-concept analysis in
  * https://metrics.mozilla.com/protected/sguha/shield_bootstrap.html
  */
object ExperimentEngagementAnalyzer {

  val logger: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(getClass)

  // We pass around this one instance to different threads, which should be fine because the implementation
  // looks to use ThreadLocals such that each thread is getting a unique generator.
  val poisson = Poisson(mean = 1.0)


  /**
    * Takes in a dataframe of observations from the `experiments` dataset, calculates aggregate engagement
    * metrics per client per experiment, and returns percentile statistics of those engagement metrics
    * with confidence intervals given by a Poisson bootstrap technique.
    * @param experimentsDF dataframe from experiments dataset
    * @param iterations how many bootstrap iterations to perform for calculating confidence intervals
    */
  def getMetrics(experimentsDF: DataFrame, iterations: Int): Seq[MetricAnalysis] = {

    val clients = filterOutliersAndAggregatePerClientDaily(experimentsDF)
    clients.persist()

    logger.info(s"Calculating engagement metrics for ${clients.count()} client/experiment/branch rows")

    val branches = clients
      .select("experiment_id", "experiment_branch")
      .distinct()
      .collect()
      .map { row => (row.getString(0), row.getString(1)) }

    val metricAnalyses =
      for {
        (experimentId, branch) <- branches
        measure <- EngagementMeasures.all
      } yield calculateMetricAnalysis(clients, experimentId, branch, measure, iterations)

    clients.unpersist()

    metricAnalyses
  }


  private def filterOutliersAndAggregatePerClientDaily(
      input: DataFrame, outlierPercentile: Double = 0.9999, relativeError: Double = 0.0001): DataFrame = {

    val dailyWithOutliers = input
      .groupBy("experiment_id", "experiment_branch", "client_id", "submission_date_s3")
      .agg(
        sum(expr("total_time/3600.0")).cast("double").alias(EngagementMeasures.mth),
        sum(expr("active_ticks/(3600.0/5)")).cast("double").alias(EngagementMeasures.mah),
        sum("scalar_parent_browser_engagement_total_uri_count").alias(EngagementMeasures.muri)
      )

    dailyWithOutliers.persist()

    // Per sguha, we expect every hypothesis test to benefit from removing outliers above the 99.99th percentile;
    // see https://metrics.mozilla.com/protected/sguha/shield_bootstrap.html#remove-outliers
    val outlierCutoffs = dailyWithOutliers.stat
      .approxQuantile(EngagementMeasures.base.toArray, Array(outlierPercentile), relativeError)
      .map(_.head)
    val daily =
      (EngagementMeasures.base, outlierCutoffs)
        .zipped
        .foldLeft(dailyWithOutliers) { (df, cutInfo) =>
          val (measure, cut) = cutInfo
          df.where(col(measure) < cut)
        }

    val clients = daily
      .groupBy("experiment_id", "experiment_branch", "client_id")
      .agg(
        avg(EngagementMeasures.mth).alias(EngagementMeasures.mth),
        avg(EngagementMeasures.mah).alias(EngagementMeasures.mah),
        expr(s"sum(${EngagementMeasures.muri})/(1.0 / 3600.0 + sum(${EngagementMeasures.mah}))")
          .cast("double").alias(EngagementMeasures.muri),
        expr(s"sum(${EngagementMeasures.mah})/(1.0 / 3600.0 + sum(${EngagementMeasures.mth}))")
          .cast("double").alias(EngagementMeasures.mins)
      )
      .na.fill(0)

    dailyWithOutliers.unpersist()

    // root
    // |-- experiment_id: string (nullable = true)
    // |-- experiment_branch: string (nullable = true)
    // |-- client_id: string (nullable = true)
    // |-- engagement_daily_hours: double (nullable = false)
    // |-- engagement_daily_active_hours: double (nullable = false)
    // |-- engagement_hourly_uris: double (nullable = false)
    // |-- engagement_intensity: double (nullable = false)
    clients
  }


  /**
    * This is the inner logic for the analyzer. We filter and reduce the input dataframe down to a single column
    * representing just a single measure of interest for a single experiment branch, then we broadcast that
    * array of values to the whole cluster and distribute computations of various random samples,
    * finally collecting the results of the samples to a single MetricAnalysis with confidence intervals for
    * each calculated percentile.
    */
  private def calculateMetricAnalysis(
      clients: DataFrame,
      experimentId: String,
      branch: String,
      measure: String,
      iterations: Int,
      limit: Int = 200000000): MetricAnalysis = {

    val sc = clients.sparkSession.sparkContext

    val measureDF = clients
      .filter(s"experiment_id = '$experimentId'")
      .filter(s"experiment_branch = '$branch'")
      .select(measure)
      .limit(limit)

    // As of July 2018, the largest enrollment for a single experiment branch was ~2 billion, which is a bit ridiculous;
    // we choose to limit these percentile calculations to max 200 million values, which at 8 bytes per double
    // should be less than 2 GB, small enough to comfortably fit in memory on a single machine.
    // We convert the observations to a Scala array, sort them, and broadcast to all nodes.
    val observations = measureDF
      .collect()
      .map(_.getDouble(0))
    util.Sorting.quickSort(observations)
    val count = observations.length
    val observationsBroadcast = sc.broadcast(observations)

    // The "Poisson bootstrap" is a statistical technique where we resample the dataset many times,
    // calculating percentiles on each sample, then use the distribution of each percentile across
    // all samples in order to estimate confidence in each percentile value.
    // We drop down to the more raw RDD API in order to express this loop of resampling;
    // we create an RDD of integers via sc.parallelize simply to split out parallel iterations;
    // each iteration generates a different random sample following the Poisson bootstrap methodology
    // and returns an array giving one value per calculated percentile.
    //
    // See: https://metrics.mozilla.com/protected/sguha/shield_bootstrap.html#bootstrapping
    val percentileArrays: Array[Array[Double]] = sc.parallelize(1 to iterations).flatMap { _ =>

      // For each observation, we draw a random poisson value (usually 0 or 1 with a tail of larger integers)
      // and duplicate the observation that many times.
      //
      // Note that the random generator is not repeatable across different measures,
      // which may affect joint distributions.
      val resampled = observationsBroadcast.value
        .flatMap(Array.fill(poisson.draw())(_))

      if (resampled.isEmpty) {
        // If an experiment branch has only a few entries, we might get all zeroes from the Poisson distribution,
        // thus we have to guard against the possibility of an empty sample.
        None
      } else {
        Some(Percentiles.decimals.map(percentileFromSortedArray(resampled, _)).toArray)
      }
    }.collect()

    observationsBroadcast.unpersist()

    val statsArray: Array[Statistic] = (percentileArrays.transpose, Percentiles.names)
      .zipped
      .map { case (valuesForPercentile, nameForPercentile) =>
        val stats: MeanAndVariance = breeze.stats.meanAndVariance(valuesForPercentile)
        val z99 = ZScore99 * (stats.stdDev * scala.math.sqrt(stats.count.toDouble - 1.0))
        Statistic(None, nameForPercentile, stats.mean,
          Some(stats.mean - z99), Some(stats.mean + z99),
          None, None)
      }

    MetricAnalysis(experimentId, branch, "", count, measure, "DoubleScalar",
      Map.empty[Long, HistogramPoint],
      Some(statsArray))
  }


  /**
    * Based on https://github.com/scalanlp/breeze/blob/releases/v1.0-RC2/math/src/main/scala/breeze/stats/DescriptiveStats.scala#L523
    */
  private def percentileFromSortedArray(arr: Array[Double], p: Double): Double = {
    // scalastyle:off
    if (p > 1 || p < 0) throw new IllegalArgumentException("p must be in [0,1]")
    // +1 so that the .5 == mean for even number of elements.
    val f = (arr.length + 1) * p
    val i = f.toInt
    if (i == 0) arr.head
    else if (i >= arr.length) arr.last
    else {
      arr(i - 1) + (f - i) * (arr(i) - arr(i - 1))
    }
  }


}
