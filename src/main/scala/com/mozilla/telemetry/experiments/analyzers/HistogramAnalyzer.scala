package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.metrics.HistogramDefinition
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.{Map => CMap}
import scala.util.{Failure, Success, Try}


case class HistogramRow(experiment_id: String, branch: String, subgroup: String, metric: Option[CMap[Int, Int]])
case class KeyedHistogramRow(experiment_id: String, branch: String, subgroup: String,
                             metric: Option[CMap[String, CMap[Int, Int]]]) {
  def collapsedMetric: Option[CMap[Int, Int]] = {
    // The OKRs state that we collapse keyed metrics (for now)
    metric match {
      case Some(m) => Some(m.values.reduce(sumCMaps[Int](_: CMap[Int, Int], _: CMap[Int, Int])))
      case _ => None
    }
  }

  def toHistogramRow: HistogramRow = {
    HistogramRow(experiment_id, branch, subgroup, collapsedMetric)
  }
}

object HistogramAggregator
  extends Aggregator[HistogramRow, Map[Int, Long], Map[Long, HistogramPoint]] {
  def zero: Map[Int, Long] = Map[Int, Long]()

  def reduce(b: Map[Int, Long], h: HistogramRow): Map[Int, Long] = {
    h.metric match {
      case Some(m: CMap[Int, Int]) => b ++ m.map { case (k: Int, v: Int) => k -> (v.toLong + b.getOrElse(k, 0L)) }
      case _ => b
    }
  }

  def merge(l: Map[Int, Long], r: Map[Int, Long]): Map[Int, Long] = {
    l ++ r.map { case (k, v) => k -> (v + l.getOrElse(k, 0L)) }
  }

  def finish(b: Map[Int, Long]): Map[Long, HistogramPoint] = {
    val sum = b.values.sum
    b.map { case (k, v) => k.toLong -> HistogramPoint(v.toDouble / sum.toDouble, v.toDouble, None) }
  }

  def bufferEncoder: Encoder[Map[Int, Long]] = ExpressionEncoder()
  def outputEncoder: Encoder[Map[Long, HistogramPoint]] = ExpressionEncoder()
}

class HistogramAnalyzer(name: String, hd: HistogramDefinition, df: DataFrame) extends java.io.Serializable {
  import df.sparkSession.implicits._

  def analyze(): Option[Dataset[HistogramAnalysis]] = {
    val ds = format match {
      case Some(d: DataFrame) => collapseKeys(d)
      case _ => return None
    }

    val histogramAggregate = HistogramAggregator.toColumn.name("histogram_aggregate")
    val output = ds.filter(r => r.metric.isDefined)
      .groupByKey(x => MetricKey(x.experiment_id, x.branch, x.subgroup))
      .agg(histogramAggregate, count("*"))
      .map { toOutputSchema }

    Some(output)
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

  private def collapseKeys(formatted: DataFrame): Dataset[HistogramRow] = {
    if (hd.keyed) formatted.as[KeyedHistogramRow].map(_.toHistogramRow) else formatted.as[HistogramRow]
  }

  private def toOutputSchema(r: (MetricKey, Map[Long, HistogramPoint], Long)): HistogramAnalysis = r match {
    case (k: MetricKey, h: Map[Long, HistogramPoint], n: Long) =>
      HistogramAnalysis(k.experiment_id, k.branch, k.subgroup, n, name, hd.getClass.getSimpleName, h, None)
  }
}
