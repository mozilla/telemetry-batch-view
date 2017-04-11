package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.utils.MetricDefinition
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.functions.{col, count, explode, lit}
import org.apache.spark.sql.{Column, DataFrame, Row}
import com.mozilla.telemetry.scalars._
import com.mozilla.telemetry.histograms._

import scala.util.{Failure, Success, Try}


abstract class MetricAnalyzer(name: String, md: MetricDefinition, df: DataFrame) extends java.io.Serializable {
  val reducer: UserDefinedAggregateFunction
  def handleKeys: Column
  val keyedUDF: UserDefinedFunction

  def analyze(): List[Row] = {
    println(name)
    val filtered = formatAndFilter match {
      case Some(x: DataFrame) => x.persist()
      case _ => return List()
    }
    val rows = aggregate(explodeMetric(filtered)).rdd.collect.toList
    val summary_stats = runSummaryStatistics(rows)
    val test_stats = runTestStatistics(filtered, rows)
    filtered.unpersist()
    toFinalSchema(rows, summary_stats, test_stats)
  }

  // We want to keep this output since we'll need this to do the experimental distance metrics
  // Question: for permutation tests, are the null values significant? e.g. should we be doing the tests on the DF before
  // filtering out nulls?
  def formatAndFilter: Option[DataFrame] = {
    Try(df.select(
      col("experiment").as("experiment_name"),
      col("branch").as("experiment_branch"),
      lit("All").as("subgroup"),
      col(name).as("metric"))
      .filter(col("metric").isNotNull)
    ) match {
      case Success(x) => Some(x)
      // expected failure, if the dataset doesn't include this metric (e.g. it's newly added)
      case Failure(_: org.apache.spark.sql.AnalysisException) => None
      // Let other exceptions bubble up
    }
  }
  def explodeMetric(filtered: DataFrame): DataFrame = {
      filtered.select(
        col("experiment_name"),
        col("experiment_branch"),
        col("subgroup"),
        explode(handleKeys).as("metric"))
        .filter(col("metric").isNotNull)
  }

  def aggregate(exploded: DataFrame): DataFrame = {
    val aggregate = exploded
      .groupBy(col("experiment_name"), col("experiment_branch"), col("subgroup"))
      .agg(count("metric").as("n"), reducer(col("metric")).as("histogram_agg"))
      .withColumn("metric_name", lit(name))
      .withColumn("metric_type", lit(md.getClass.getSimpleName))
      .coalesce(1)
    aggregate.show()
    aggregate
  }

  def runSummaryStatistics(rows: List[Row]): List[List[Row]] = {
    // TODO: fill me in!
    List()
  }

  def runTestStatistics(filtered: DataFrame, rows: List[Row]): List[List[Row]] = {
    // TODO: fill me in!
    List()
  }

  def toFinalSchema(rows: List[Row], summary_stats: List[List[Row]], test_stats: List[List[Row]]): List[Row]
}

object MetricAnalyzer {
  def getAnalyzer(name: String, md: MetricDefinition, df: DataFrame): MetricAnalyzer = {
    md match {
      case t: FlagHistogram => new FlagHistogramAnalyzer(name, t, df)
      case t: BooleanHistogram => new BooleanHistogramAnalyzer(name, t, df)
      case t: CountHistogram => new CountHistogramAnalyzer(name, t, df)
      case t: EnumeratedHistogram => new EnumeratedHistogramAnalyzer(name, t, df)
      case t: LinearHistogram => new LinearHistogramAnalyzer(name, t, df)
      case t: ExponentialHistogram => new ExponentialHistogramAnalyzer(name, t, df)
      case t: UintScalar => new UintScalarAnalyzer(name, t, df)
      case t: StringScalar => new StringScalarAnalyzer(name, t, df)
      case t => throw new Exception("Unknown metric definition type " + t.getClass.getSimpleName)
    }
  }
}