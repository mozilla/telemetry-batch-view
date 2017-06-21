package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.metrics.MetricDefinition
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
                     confidence_low: Double,
                     confidence_high: Double,
                     confidence_level: Double,
                     p_value: Double)

case class MetricAnalysis(experiment_id: String,
                          experiment_branch: String,
                          subgroup: String,
                          n: Long,
                          metric_name: String,
                          metric_type: String,
                          histogram: Map[Long, HistogramPoint],
                          statistics: Option[Seq[Statistic]])

abstract class MetricAnalyzer[T](name: String, md: MetricDefinition, df: DataFrame) extends java.io.Serializable {
  type PreAggregateRowType <: PreAggregateRow[T]
  val aggregator: MetricAggregator[T]

  import df.sparkSession.implicits._

  def analyze(): Dataset[MetricAnalysis] = {
    format match {
      case Some(d: DataFrame) => {
        val agg_column = aggregator.toColumn.name("metric_aggregate")
        val output = collapseKeys(d)
          .filter(r => r.metric.isDefined)
          .groupByKey(x => MetricKey(x.experiment_id, x.branch, x.subgroup))
          .agg(agg_column, count("*"))
          .map(toOutputSchema)
        reindex(output)
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
      MetricAnalysis(k.experiment_id, k.branch, k.subgroup, n, name, md.getClass.getSimpleName, h, None)
  }
}
