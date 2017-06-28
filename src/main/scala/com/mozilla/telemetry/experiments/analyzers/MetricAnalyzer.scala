package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.metrics.MetricDefinition
import com.mozilla.telemetry.experiments.statistics.PairwiseAggregateTests
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, count, lit}

import scala.collection.Map
import scala.util.{Failure, Success, Try}


trait PreAggregateRow[T] {
  val experiment_id: String
  val branch: String
  val subgroup: String
  val metric: Option[Map[T, Long]]
}

case class MetricKey(experiment_id: String, branch: String, subgroup: String)

// pdf is the count normalized by counts for all buckets
case class HistogramPoint(pdf: Double, count: Double, label: Option[String])

case class Statistic(comparison_branch: Option[String],
                     name: String,
                     value: Double,
                     confidence_low: Option[Double],
                     confidence_high: Option[Double],
                     confidence_level: Option[Double],
                     p_value: Option[Double])

case class MetricAnalysis(experiment_id: String,
                          experiment_branch: String,
                          subgroup: String,
                          n: Long,
                          metric_name: String,
                          metric_type: String,
                          histogram: Map[Long, HistogramPoint],
                          statistics: Seq[Statistic] = Seq.empty[Statistic])

abstract class MetricAnalyzer[T](name: String, md: MetricDefinition, df: DataFrame) extends java.io.Serializable {
  type PreAggregateRowType <: PreAggregateRow[T]
  val aggregator: MetricAggregator[T]

  import df.sparkSession.implicits._

  def analyze(): Dataset[MetricAnalysis] = {
    format match {
      case Some(d: DataFrame) => {
        val agg_column = aggregator.toColumn.name("metric_aggregate")
        val preagg = collapseKeys(d)
        val output = preagg.filter(r => r.metric.isDefined)
          .groupByKey(x => MetricKey(x.experiment_id, x.branch, x.subgroup))
          .agg(agg_column, count("*"))
          .map(toOutputSchema)
        val aggregates = reindex(output)
        addStatistics(aggregates, preagg)
      }
      case _ => df.sparkSession.emptyDataset[MetricAnalysis]
    }
  }

  private def format: Option[DataFrame] = {
    Try(df.select(
      col("experiment_id"),
      col("experiment_branch").as("branch"),
      lit("All").as("subgroup"),
      col(name).as("metric"))
    ) match {
      case Success(x) => Some(x)
      // expected failure, if the dataset doesn't include this metric (e.g. it's newly added)
      case Failure(_: org.apache.spark.sql.AnalysisException) => None
      case Failure(x: Throwable) => throw x
    }
  }

  def collapseKeys(formatted: DataFrame): Dataset[PreAggregateRowType]

  protected def reindex(aggregates: Dataset[MetricAnalysis]): Dataset[MetricAnalysis] = {
    // This is meant as a finishing step for string scalars only
    aggregates
  }

  private def toOutputSchema(r: (MetricKey, Map[Long, HistogramPoint], Long)): MetricAnalysis = r match {
    case (k: MetricKey, h: Map[Long, HistogramPoint], n: Long) =>
      MetricAnalysis(k.experiment_id, k.branch, k.subgroup, n, name, md.getClass.getSimpleName, h, List())
  }

  def addStatistics(aggregates: Dataset[MetricAnalysis], preagg: Dataset[PreAggregateRowType]): Dataset[MetricAnalysis] = {
    val collected = aggregates.collect().toList
    collected match {
      case Nil => aggregates
      case _ =>
        // TODO: add summary statistics for each aggregate
        // TODO: add permutation tests
        addPairwiseAggregateStatistics(collected).toSeq.toDS()
    }
  }

  def addPairwiseAggregateStatistics(aggregates: List[MetricAnalysis]): List[MetricAnalysis] = {
    val pairs = aggregates.combinations(2).toList
    pairs match {
      case Nil => aggregates
      case x =>
        val s =
          x.map {
            case l :: r :: Nil => new PairwiseAggregateTests(l, r).runAllTests
            case _ => Map.empty[String, Array[Statistic]]
          }.reduce(combineMaps)
          aggregates.map(attachStats(_, s))
    }
  }

  def combineMaps(l: Map[String, Array[Statistic]], r: Map[String, Array[Statistic]]): Map[String, Array[Statistic]] = {
    l ++ r.map { case (k, v) => k -> (v ++ l.getOrElse(k, Array())) }
  }

  def attachStats(m: MetricAnalysis, statsMap: Map[String, Array[Statistic]]): MetricAnalysis = {
    statsMap.getOrElse(m.experiment_branch, None) match {
      case stats: Array[Statistic] => m.copy(statistics = m.statistics ++ stats)
      case _ => m
    }
  }
}
