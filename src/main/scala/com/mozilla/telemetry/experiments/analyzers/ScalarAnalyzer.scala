package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.metrics._
import org.apache.spark.sql._

import scala.collection.Map


// Spark doesn't support Datasets of case classes with type parameters (nor of abstract type members) -- otherwise we'd
// be able to avoid creating the concrete type versions of all of these case classes

trait definesToScalarMapRow[R] {
  def toScalarMapRow: R
}

trait ScalarRow[T] {
  val experiment_id: String
  val branch: String
  val subgroup: String
  val metric: Option[T]

  def scalarMapRowMetric: Option[Map[T, Long]] = {
    metric match {
      case Some(m) => Some(Map(m -> 1L))
      case _ => None
    }
  }
}

case class BooleanScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Boolean])
  extends ScalarRow[Boolean] with definesToScalarMapRow[BooleanScalarMapRow] {
  def toScalarMapRow: BooleanScalarMapRow = BooleanScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric)
}
case class UintScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Int])
  extends ScalarRow[Int] with definesToScalarMapRow[UintScalarMapRow] {
  def toScalarMapRow: UintScalarMapRow = UintScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric)
}
case class LongScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Long])
  extends ScalarRow[Long] with definesToScalarMapRow[LongScalarMapRow] {
  def toScalarMapRow: LongScalarMapRow = LongScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric)
}
case class StringScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[String])
  extends ScalarRow[String] with definesToScalarMapRow[StringScalarMapRow] {
  def toScalarMapRow: StringScalarMapRow = StringScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric)
}

trait KeyedScalarRow[T] {
  val experiment_id: String
  val branch: String
  val subgroup: String
  val metric: Option[Map[String, T]]

  def collapsedMetric: Option[Map[T, Long]] = {
    metric match {
      case Some(m: Map[String, T]) => Some(m.values.foldLeft(Map.empty[T, Long]) {
        case(m: Map[T, Long], e: T @unchecked) => m + (e -> (m.getOrElse(e, 0L) + 1L))
      })
      case _ => None
    }
  }
}

case class KeyedBooleanScalarRow(experiment_id: String, branch: String, subgroup: String,
                                 metric: Option[Map[String, Boolean]])
  extends KeyedScalarRow[Boolean] with definesToScalarMapRow[BooleanScalarMapRow] {
  def toScalarMapRow: BooleanScalarMapRow = BooleanScalarMapRow(experiment_id, branch, subgroup, collapsedMetric)
}
case class KeyedUintScalarRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Int]])
  extends KeyedScalarRow[Int] with definesToScalarMapRow[UintScalarMapRow] {
  def toScalarMapRow: UintScalarMapRow = UintScalarMapRow(experiment_id, branch, subgroup, collapsedMetric)
}
case class KeyedLongScalarRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Long]])
  extends KeyedScalarRow[Long] with definesToScalarMapRow[LongScalarMapRow] {
  def toScalarMapRow: LongScalarMapRow = LongScalarMapRow(experiment_id, branch, subgroup, collapsedMetric)
}
case class KeyedStringScalarRow(experiment_id: String, branch: String, subgroup: String,
                                metric: Option[Map[String, String]])
  extends KeyedScalarRow[String] with definesToScalarMapRow[StringScalarMapRow] {
  def toScalarMapRow: StringScalarMapRow = StringScalarMapRow(experiment_id, branch, subgroup, collapsedMetric)
}

case class BooleanScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                               metric: Option[Map[Boolean, Long]]) extends PreAggregateRow[Boolean]
case class UintScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                            metric: Option[Map[Int, Long]]) extends PreAggregateRow[Int]
case class LongScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                            metric: Option[Map[Long, Long]]) extends PreAggregateRow[Long]
case class StringScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Long]]) extends PreAggregateRow[String]

trait AggregateResampler {
  type PreAggregateRowType

  // This implementation reaggregates directly from the aggregated histogram, which works when we expect one sample
  // per ping, such as when we're resampling scalar aggregates
  def resample(ds: Dataset[PreAggregateRowType],
               aggregations: List[MetricAnalysis],
               iterations: Int = 1000): Map[MetricKey, List[MetricAnalysis]] = {
    aggregations.map { a =>
      val total = a.histogram.values.map(_.count).sum
      // We could use the pdf field directly to cumulatively sum but we'd likely run into weird floating point addition
      // issues
      val cdf =  a.histogram
        .map { case (k, v) => k -> v.count }
        .toList
        .scanLeft(0L, 0.0) { case((_: Long, c: Double), (k: Long, v: Double)) => (k, c + v)}
        .tail
        .map { case (k, v) => (k, v / total) }
      (a, total, cdf)
    }.map { case (m, total, cdf) =>
      val aggs = ds.sparkSession.sparkContext.parallelize(1 to iterations).map { x: Int =>
        val agg = resampleCdf(cdf, x, total.toInt)
        m.copy(histogram = agg.map {case (k, v) => k -> HistogramPoint(v / total, v.toDouble, None)})
      }.collect.toList
      m.metricKey -> aggs
    }.toMap
  }

  private def resampleCdf(cumulativeWeights: List[(Long, Double)], seed: Int, n: Int): Map[Long, Long] = {
    val rng = new scala.util.Random(seed)
    (0 to n).foldLeft(Map.empty[Long, Long]) { (agg, _) =>
      val rand = rng.nextDouble()
      val sample = cumulativeWeights.collectFirst { case(k, c) if rand <= c => k } match {
        case Some(k) => k
        case _ => throw new Exception("Something very strange happened in constructing the cdf")
      }
      agg + (sample -> (agg.getOrElse(sample, 0L) + 1L))
    }
  }
}


class BooleanScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame, bootstrap: Boolean = false)
  extends MetricAnalyzer[Boolean](name, md, df, bootstrap) with AggregateResampler {
  override type PreAggregateRowType = BooleanScalarMapRow
  val aggregator = BooleanAggregator

  override def validateRow(row: BooleanScalarMapRow): Boolean = row.metric.isDefined

  def collapseKeys(formatted: DataFrame): Dataset[BooleanScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedBooleanScalarRow] else formatted.as[BooleanScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class UintScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame, bootstrap: Boolean = false)
  extends MetricAnalyzer[Int](name, md, df, bootstrap) with AggregateResampler {
  override type PreAggregateRowType = UintScalarMapRow
  val aggregator = UintAggregator
  override def validateRow(row: UintScalarMapRow): Boolean = row.metric match {
    case Some(m) => m.keys.forall(_ >= 0)
    case None => false
  }

  def collapseKeys(formatted: DataFrame): Dataset[UintScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedUintScalarRow] else formatted.as[UintScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class LongScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame, bootstrap: Boolean = false)
  extends MetricAnalyzer[Long](name, md, df, bootstrap) with AggregateResampler {
  override type PreAggregateRowType = LongScalarMapRow
  val aggregator = LongAggregator

  override def validateRow(row: LongScalarMapRow): Boolean = row.metric match {
    case Some(m) => m.keys.forall(_ >= 0L)
    case None => false
  }

  def collapseKeys(formatted: DataFrame): Dataset[LongScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedLongScalarRow] else formatted.as[LongScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class StringScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame, bootstrap: Boolean = false)
  extends MetricAnalyzer[String](name, md, df, bootstrap) with AggregateResampler {
  override type PreAggregateRowType = StringScalarMapRow
  val aggregator = StringAggregator

  override def validateRow(row: StringScalarMapRow): Boolean = row.metric.isDefined

  def collapseKeys(formatted: DataFrame): Dataset[StringScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedStringScalarRow] else formatted.as[StringScalarRow]
    s.map(_.toScalarMapRow)
  }

  override protected def reindex(aggregates: List[MetricAnalysis]): List[MetricAnalysis] = {
    // this is really annoying, but we need to give string scalar values indexes and they have to be
    // consistent among all the histograms across all branches, so we: aggregate the histograms
    // across all the branches, sort by count descending, and use that order for our index
    val counts = aggregates.map { a: MetricAnalysis =>
      a.histogram.values.map {p: HistogramPoint => p.label.get -> p.count.toLong}.toMap[String, Long]
    }

    if (counts.isEmpty)
      aggregates
    else {
      val indexes = counts.reduce(addHistograms[String]).toSeq.sortWith(_._2 > _._2).zipWithIndex.map {
        case ((key, _), index) => key -> index.toLong
      }.toMap

      aggregates.map(r => r.copy(
        histogram = r.histogram.map { case (_, p) => indexes(p.label.get) -> p }
      ))
    }
  }
}

object ScalarAnalyzer {
  def getAnalyzer(name: String, sd: ScalarDefinition, df: DataFrame, bootstrap: Boolean = false): MetricAnalyzer[_] = {
    sd match {
      case s: BooleanScalar => new BooleanScalarAnalyzer(name, s, df, bootstrap)
      case s: UintScalar => new UintScalarAnalyzer(name, s, df, bootstrap)
      case s: StringScalar => new StringScalarAnalyzer(name, s, df, bootstrap)
      case _ => throw new UnsupportedOperationException("Unsupported scalar type")
    }
  }
}
