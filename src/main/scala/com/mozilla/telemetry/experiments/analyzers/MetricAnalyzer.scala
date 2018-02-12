package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.experiments.statistics.{ComparativeStatistics, DescriptiveStatistics}
import com.mozilla.telemetry.metrics.MetricDefinition
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, count, lit, sum}

import scala.collection.Map
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}


trait PreAggregateRow[T] {
  val experiment_id: String
  val branch: String
  val subgroup: String
  val metric: Option[Map[T, Long]]
  val block_id: Int
}

case class BlockAggregate[T](key: (MetricKey, Int), metric_aggregate: Map[T, Long], val count: Long)

case class MetricKey(experiment_id: String, branch: String, subgroup: String)

// pdf is the count normalized by counts for all buckets
case class HistogramPoint(pdf: Double, count: Double, label: Option[String])

case class Statistic(comparison_branch: Option[String],
                     name: String,
                     value: Double,
                     confidence_low: Option[Double] = None,
                     confidence_high: Option[Double] = None,
                     confidence_level: Option[Double] = None,
                     p_value: Option[Double] = None)

case class MetricAnalysis(experiment_id: String,
                          experiment_branch: String,
                          subgroup: String,
                          n: Long,
                          metric_name: String,
                          metric_type: String,
                          histogram: Map[Long, HistogramPoint],
                          statistics: Option[Seq[Statistic]]) {
  def metricKey: MetricKey = MetricKey(experiment_id, experiment_branch, subgroup)
}

abstract class MetricAnalyzer[T](name: String, md: MetricDefinition, df: DataFrame, numJackknifeBlocks: Int)(implicit tag: TypeTag[T])
  extends java.io.Serializable {
  type PreAggregateRowType <: PreAggregateRow[T]
  val groupAggregator: GroupAggregator[T]
  val finalAggregator: MapAggregator[T]
  def validateRow(row: PreAggregateRowType): Boolean


  import df.sparkSession.implicits._

  def analyze(): List[MetricAnalysis] = {
    format match {
      case Some(d: DataFrame) => {
        val cleanData = collapseKeys(d)
          .filter(validateRow _)

        val groupedDF = aggregateBlocks(cleanData).persist
        val trueAggregates = aggregateAll(groupedDF)
        val jackknifeAggregates = if (!md.isCategoricalMetric) Some(aggregatePseudosamples(groupedDF)) else None
        val output = addStatistics(reindex(trueAggregates), jackknifeAggregates)
        groupedDF.unpersist()
        output
      }
      case _ => List.empty[MetricAnalysis]
    }
  }

  private def format: Option[DataFrame] = {
    Try(df.select(
      col("experiment_id"),
      col("experiment_branch").as("branch"),
      lit(MetricAnalyzer.topLevelLabel).as("subgroup"),
      col(name).as("metric"),
      col("block_id"))
    ) match {
      case Success(x) => Some(x)
      // expected failure, if the dataset doesn't include this metric (e.g. it's newly added)
      case Failure(_: org.apache.spark.sql.AnalysisException) => None
      case Failure(x: Throwable) => throw x
    }
  }

  def collapseKeys(formatted: DataFrame): Dataset[PreAggregateRowType]

  def aggregateBlocks(ds: Dataset[PreAggregateRowType]): Dataset[BlockAggregate[T]] = {
    val agg_column = groupAggregator.toColumn.name("metric_aggregate")
    ds.groupByKey(x => (MetricKey(x.experiment_id, x.branch, x.subgroup), x.block_id))
      .agg(agg_column, count("*").name("count"))
      .as[BlockAggregate[T]]
  }

  def aggregateAll(ds: Dataset[BlockAggregate[T]]): List[MetricAnalysis] = {
    val agg_column = finalAggregator.toColumn.name("metric_aggregate")

    ds.groupByKey(x => x.key._1)
      .agg(agg_column, sum("count").as[Long])
      .map(toOutputSchema)
      .collect
      .toList
  }

  def aggregatePseudosamples(ds: Dataset[BlockAggregate[T]]): Dataset[MetricAnalysis] = {
    // We're using the block or delete-d jackknife variant
    val agg_column = finalAggregator.toColumn.name("metric_aggregate")
    // This is a bit of a hack so we can reuse aggregators -- the input dataset is the original dataset aggregated over
    // block_id -- that is, there's one row per block_id and the metric_aggregate is the summed up map for each block.
    // The flatmap below duplicates each row (total_blocks - 1) times, omitting the row for the dropped block it
    // belongs to and re-keying the other copies with the block_id. The grouping + aggregation step then groups by the
    // new key and aggregates the results.
    val flatMapped = ds
      .flatMap[BlockAggregate[T]] {
        row: BlockAggregate[T] => (0 until numJackknifeBlocks).filter(_ != row.key._2).map(s => row.copy(key = (row.key._1, s)))
      }
    flatMapped
      .groupByKey(_.key)
      .agg(agg_column)
      .map(r => toOutputSchema(r._1._1, r._2, 0L))
  }

  protected def reindex(aggregates: List[MetricAnalysis]): List[MetricAnalysis] = {
    // This is meant as a finishing step for string scalars only
    aggregates
  }

  private def addStatistics(aggregates: List[MetricAnalysis],
                            jackknifed: Option[Dataset[MetricAnalysis]]): List[MetricAnalysis] = {
    val descriptiveStatsMap = if (!md.isCategoricalMetric) {
      aggregates.map {
        m =>
          val filtered = jackknifed.map(o => o.filter(x => x.metricKey == m.metricKey))
          m -> DescriptiveStatistics(m, filtered).getStatistics
      }.toMap
    } else {
      Map.empty[MetricAnalysis, List[Statistic]]
    }

    val grouped = aggregates.groupBy(_.subgroup)

    val comparativeStatsMap = grouped.getOrElse(MetricAnalyzer.topLevelLabel, List.empty[MetricAnalysis]) match {
      // two branches are easy to compare
      case m if m.length == 2 => ComparativeStatistics(m.head, m.last).getStatistics

      // for > 2 branches, we find the control branch and compare the rest of the branches against that
      case m if m.length > 2 =>
        val controlOrNot = m.groupBy(_.experiment_branch.toLowerCase == "control")
        (controlOrNot.get(true), controlOrNot.get(false)) match {
          case (Some(control), Some(notControl)) =>
            if (control.length != 1) throw new Exception("Something wonky is going on with the control branch")

            // iterate through each of the experimental branches and compare against control
            notControl
              .map(ComparativeStatistics(control.head, _).getStatistics)
              .reduce(addListMaps[MetricAnalysis, Statistic])

          // if no branches are named "control", we don't know what to compare against so we don't run these stats
          case _ => Map.empty[MetricAnalysis, List[Statistic]]
        }
      // We have seen a couple "single-branch" sanity test experiments
      case _ => Map.empty[MetricAnalysis, List[Statistic]]
    }

    addListMaps[MetricAnalysis, Statistic](descriptiveStatsMap, comparativeStatsMap)
      .map { case(m, s) => m.copy(statistics = Some(s)) }
      .toList
  }

  private def toOutputSchema(r: (MetricKey, Map[Long, HistogramPoint], Long)): MetricAnalysis = r match {
    case (k: MetricKey, h: Map[Long, HistogramPoint], n: Long) =>
      MetricAnalysis(k.experiment_id, k.branch, k.subgroup, n, name, md.getClass.getSimpleName, h, None)
  }
}

object MetricAnalyzer {
  val topLevelLabel = "All"
}
