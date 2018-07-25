/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.analyzers

import breeze.stats.distributions.Poisson
import com.mozilla.telemetry.utils.ColumnEnumeration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame}

/**
  * See discussion of confidence levels in
  * https://metrics.mozilla.com/protected/sguha/shield_bootstrap.html#the-shift-plot
  */
object ConfidenceInterval {
  val confidenceLevel:  Double = 0.975
  val confidenceMargin: Double = 0.5 * (1.0 - confidenceLevel)
  val percentileLow:    Double = 0.0 + confidenceMargin
  val percentileHigh:   Double = 1.0 - confidenceMargin
}

case class ConfidenceInterval(arr: Array[Double]) {
  def low:  Double = breeze.stats.DescriptiveStats.percentile(arr, ConfidenceInterval.percentileLow)
  def high: Double = breeze.stats.DescriptiveStats.percentile(arr, ConfidenceInterval.percentileHigh)
}

/**
  * Columns we need to pull from the input DataFrame.
  */
object InputCols extends ColumnEnumeration {
  val experiment_id, experiment_branch, client_id, submission_date_s3 = Val()
  val total_time, active_ticks, scalar_parent_browser_engagement_total_uri_count = Val()
}

/**
  * Derived columns calculated via window functions.
  */
object InputWindowCols extends ColumnEnumeration {
  private val branchSet: Column =
    collect_set(InputCols.experiment_branch.col).over(
      Window
        .partitionBy(InputCols.experiment_id.col, InputCols.client_id.col)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

  val branch_count = Val(size(branchSet))
}

/**
  * Aggregations we apply per experiment/branch/client/day.
  */
object DailyAggCols extends ColumnEnumeration {
  val partitionCols: Array[Column] = Array(
    InputCols.experiment_id.col,
    InputCols.experiment_branch.col,
    InputCols.client_id.col,
    InputCols.submission_date_s3.col
  )

  val ticksPerSecond: Double = 5.0
  val secondsPerHour: Double = 3600.0

  val sum_total_hours = Val(
    sum(InputCols.total_time.col).cast(DoubleType) / secondsPerHour
  )
  val sum_active_hours = Val(
    sum(InputCols.active_ticks.col.cast(DoubleType) * ticksPerSecond / secondsPerHour)
  )
  val sum_total_uris = Val(
    sum(InputCols.scalar_parent_browser_engagement_total_uri_count.col)
  )
}

  * Aggregations per experiment/branch/client, giving a summary per client
  * of engagement over all completed days of the experiment.
  */
object EngagementAggCols extends ColumnEnumeration {
  val partitionCols: Array[Column] = Array(
    InputCols.experiment_id.col,
    InputCols.experiment_branch.col,
    InputCols.client_id.col
  )

  val engagement_daily_active_hours = Val(avg(DailyAggCols.sum_active_hours.col))
  val engagement_hourly_uris = Val(
    sum(DailyAggCols.sum_total_uris.col) /
      (sum(DailyAggCols.sum_active_hours.col) + 1.0 / DailyAggCols.secondsPerHour)
  )
  val engagement_intensity = Val(
    sum(DailyAggCols.sum_active_hours.col) /
      (sum(DailyAggCols.sum_total_hours.col) + 1.0 / DailyAggCols.secondsPerHour)
  )
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
    case _ => s"${pInt}th Percentile"
  }

  /**
    * Based on https://github.com/scalanlp/breeze/blob/releases/v1.0-RC2/math/src/main/scala/breeze/stats/DescriptiveStats.scala#L523
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

object MeanComputation extends StatisticalComputation {
  override val name: String = "Mean"
  override def calculate(arr: Array[Double]): Double = {
    breeze.stats.mean(arr)
  }
}

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


/**
  * Introduced for bug 1460090, based on a proof-of-concept analysis in
  * https://metrics.mozilla.com/protected/sguha/shield_bootstrap.html
  */
object ExperimentEngagementAnalyzer {

  val logger: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(getClass)

  // Each executor will get a new copy of this variable as it will be included in the percentileArrays closure;
  // it looks to use ThreadLocals such that each thread is getting a unique random generator.
  val poisson = Poisson(mean = 1.0)


  /**
    * Takes in a dataframe of observations from the `experiments` dataset, calculates aggregate engagement
    * metrics per client per experiment, and returns percentile statistics of those engagement metrics
    * with confidence intervals given by a Poisson bootstrap technique.
    * @param experimentsDF dataframe from experiments dataset
    * @param iterations how many bootstrap iterations to perform for calculating confidence intervals
    */
  def getMetrics(experimentsDF: DataFrame, iterations: Int): Seq[MetricAnalysis] = {

    val clients = filterOutliersAndAggregatePerClientDaily(experimentsDF).persist()

    logger.info(s"Calculating engagement metrics for ${clients.count()} client/experiment/branch rows")

    val branches = clients
      .select(InputCols.experiment_id.col, InputCols.experiment_branch.col)
      .distinct()
      .collect()
      .map { row => (row.getString(0), row.getString(1)) }

    val metricAnalyses =
      for {
        (experimentId, branch) <- branches
        measure <- EngagementAggCols.names
      } yield calculateMetricAnalysis(clients, experimentId, branch, measure, iterations)

    clients.unpersist()

    metricAnalyses
  }


  private def filterOutliersAndAggregatePerClientDaily(
      input: DataFrame, outlierPercentile: Double = 0.9999, relativeError: Double = 0.0001): DataFrame = {

    val inputWithBranchCount = input
      .select(col("*"), InputWindowCols.branch_count.expr)
      .persist()

    val numClientsSwitchingBranches = inputWithBranchCount
      .filter(InputWindowCols.branch_count.col > 1)
      .select(InputCols.experiment_id.col, InputCols.client_id.col)
      .distinct()
      .count()

    logger.info(s"Pruning $numClientsSwitchingBranches " +
      "clients that appear in more than one branch per experiment")

    val dailyWithOutliers = inputWithBranchCount
      .filter(InputWindowCols.branch_count.col === 1)
      .groupBy(DailyAggCols.partitionCols: _*)
      .agg(DailyAggCols.exprs.head, DailyAggCols.exprs.tail: _*)
      .select(col("*"), EnrollmentWindowCols.week_number.expr)
      .persist()

    inputWithBranchCount.unpersist()

    // Per sguha, we expect every hypothesis test to benefit from removing outliers above the 99.99th percentile;
    // see https://metrics.mozilla.com/protected/sguha/shield_bootstrap.html#remove-outliers
    val outlierCutoffs = dailyWithOutliers.stat
      .approxQuantile(DailyAggCols.names.toArray, Array(outlierPercentile), relativeError)
      .map(_.head)

    val daily = (DailyAggCols.cols, outlierCutoffs)
      .zipped
      .foldLeft(dailyWithOutliers) { (df, cutInfo) =>
        val (measure, cut) = cutInfo
        df.where(measure < cut)
      }

    val clients = daily
      .groupBy(EngagementAggCols.partitionCols: _*)
      .agg(EngagementAggCols.exprs.head, EngagementAggCols.exprs.tail: _*)
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
      maxClientsPerBranch: Int = 200000000): MetricAnalysis = {

    val sc = clients.sparkSession.sparkContext

    val measureDF = clients
      .filter(s"experiment_id = '$experimentId'")
      .filter(s"experiment_branch = '$branch'")
      .select(measure)
      .limit(maxClientsPerBranch)

    // Maximum expected enrollment for a single experiment would be a few million clients;
    // in order to efficiently parallelize these percentile calculations, we impose a generous limit
    // of 200 million values, which at 8 bytes per double should be less than 2 GB,
    // small enough to comfortably fit in memory on a single machine.
    // We convert the observations to a Scala array, sort them, and broadcast to all nodes, so that we
    // can have each node running iterations in parallel on local data.
    val observations = measureDF
      .collect()
      .map(_.getDouble(0))

    util.Sorting.quickSort(observations)
    val count = observations.length
    val observationsBroadcast = sc.broadcast(observations)

    val fullSampleComputations = StatisticalComputation.values.map(_.calculate(observations)).toArray

    // The "Poisson bootstrap" is a statistical technique where we resample the dataset many times,
    // calculating percentiles on each sample, then use the distribution of each percentile across
    // all samples in order to estimate confidence in each percentile value.
    // We drop down to the more raw RDD API in order to express this loop of resampling;
    // we create an RDD of integers via sc.parallelize simply to split out parallel iterations;
    // each iteration generates a different random sample following the Poisson bootstrap methodology
    // and returns an array giving one value per calculated percentile.
    //
    // See: https://metrics.mozilla.com/protected/sguha/shield_bootstrap.html#bootstrapping
    val bootstrapComputations: Array[Array[Double]] = sc.parallelize(1 to iterations).flatMap { _ =>

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
        Some(StatisticalComputation.values.map(_.calculate(resampled)).toArray)
      }
    }.collect()

    observationsBroadcast.unpersist()

    val statsArray: Array[Statistic] =
      (fullSampleComputations, bootstrapComputations.transpose, StatisticalComputation.names)
        .zipped
        .map { case (fullSampleValue, bootstrapValues, nameForComputation) =>
          val confidence = ConfidenceInterval(bootstrapValues)
          Statistic(None, nameForComputation, fullSampleValue, Some(confidence.low), Some(confidence.high), None, None)
        }

    MetricAnalysis(experimentId, branch, "", count, measure, "DoubleScalar",
      Map.empty[Long, HistogramPoint],
      Some(statsArray))
  }

}
