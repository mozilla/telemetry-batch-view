package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.metrics._
import com.mozilla.telemetry.metrics.UintDerivedScalar
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


class BooleanScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricAnalyzer[Boolean](name, md, df) {
  override type PreAggregateRowType = BooleanScalarMapRow
  val aggregator = BooleanAggregator

  def collapseKeys(formatted: DataFrame): Dataset[BooleanScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedBooleanScalarRow] else formatted.as[BooleanScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class UintScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricAnalyzer[Int](name, md, df) {
  override type PreAggregateRowType = UintScalarMapRow
  val aggregator = UintAggregator

  def collapseKeys(formatted: DataFrame): Dataset[UintScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedUintScalarRow] else formatted.as[UintScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class LongScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricAnalyzer[Long](name, md, df) {
  override type PreAggregateRowType = LongScalarMapRow
  val aggregator = LongAggregator

  def collapseKeys(formatted: DataFrame): Dataset[LongScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedLongScalarRow] else formatted.as[LongScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class StringScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame)
  extends MetricAnalyzer[String](name, md, df) {
  override type PreAggregateRowType = StringScalarMapRow
  val aggregator = StringAggregator

  def collapseKeys(formatted: DataFrame): Dataset[StringScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedStringScalarRow] else formatted.as[StringScalarRow]
    s.map(_.toScalarMapRow)
  }

  override protected def reindex(aggregates: Dataset[MetricAnalysis]): Dataset[MetricAnalysis] = {
    import aggregates.sparkSession.implicits._
    // this is really annoying, but we need to give string scalar values indexes and they have to be
    // consistent among all the histograms across all branches, so we: aggregate the histograms
    // across all the branches, sort by count descending, and use that order for our index
    val counts = aggregates.collect().map { a: MetricAnalysis =>
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
  def getAnalyzer(name: String, sd: ScalarDefinition, df: DataFrame): MetricAnalyzer[_] = {
    sd match {
      case s: BooleanScalar => new BooleanScalarAnalyzer(name, s, df)
      case s: UintScalar => new UintScalarAnalyzer(name, s, df)
      case s: StringScalar => new StringScalarAnalyzer(name, s, df)
      case s: UintDerivedScalar => new UintScalarAnalyzer(name, s, df)
      case s: LongDerivedScalar => new LongScalarAnalyzer(name, s, df)
      case s: StringDerivedScalar => new StringScalarAnalyzer(name, s, df)
      case s: BooleanDerivedScalar => new BooleanScalarAnalyzer(name, s, df)
      case _ => throw new UnsupportedOperationException("Unsupported scalar type")
    }
  }
}
