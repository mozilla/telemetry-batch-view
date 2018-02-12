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
  val block_id: Int

  def scalarMapRowMetric: Option[Map[T, Long]] = {
    metric match {
      case Some(m) => Some(Map(m -> 1L))
      case _ => None
    }
  }
}

case class BooleanScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Boolean], block_id: Int)
  extends ScalarRow[Boolean] with definesToScalarMapRow[BooleanScalarMapRow] {
  def toScalarMapRow: BooleanScalarMapRow = BooleanScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric, block_id)
}
case class UintScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Int], block_id: Int)
  extends ScalarRow[Int] with definesToScalarMapRow[UintScalarMapRow] {
  def toScalarMapRow: UintScalarMapRow = UintScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric, block_id)
}
case class LongScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Long], block_id: Int)
  extends ScalarRow[Long] with definesToScalarMapRow[LongScalarMapRow] {
  def toScalarMapRow: LongScalarMapRow = LongScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric, block_id)
}
case class StringScalarRow(experiment_id: String, branch: String, subgroup: String, metric: Option[String], block_id: Int)
  extends ScalarRow[String] with definesToScalarMapRow[StringScalarMapRow] {
  def toScalarMapRow: StringScalarMapRow = StringScalarMapRow(experiment_id, branch, subgroup, scalarMapRowMetric, block_id)
}

trait KeyedScalarRow[T] {
  val experiment_id: String
  val branch: String
  val subgroup: String
  val metric: Option[Map[String, T]]
  val block_id: Int

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
                                 metric: Option[Map[String, Boolean]], block_id: Int)
  extends KeyedScalarRow[Boolean] with definesToScalarMapRow[BooleanScalarMapRow] {
  def toScalarMapRow: BooleanScalarMapRow = BooleanScalarMapRow(experiment_id, branch, subgroup, collapsedMetric,
    block_id)
}
case class KeyedUintScalarRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Int]], block_id: Int)
  extends KeyedScalarRow[Int] with definesToScalarMapRow[UintScalarMapRow] {
  def toScalarMapRow: UintScalarMapRow = UintScalarMapRow(experiment_id, branch, subgroup, collapsedMetric, block_id)
}
case class KeyedLongScalarRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Long]], block_id: Int)
  extends KeyedScalarRow[Long] with definesToScalarMapRow[LongScalarMapRow] {
  def toScalarMapRow: LongScalarMapRow = LongScalarMapRow(experiment_id, branch, subgroup, collapsedMetric, block_id)
}
case class KeyedStringScalarRow(experiment_id: String, branch: String, subgroup: String,
                                metric: Option[Map[String, String]], block_id: Int)
  extends KeyedScalarRow[String] with definesToScalarMapRow[StringScalarMapRow] {
  def toScalarMapRow: StringScalarMapRow = StringScalarMapRow(experiment_id, branch, subgroup, collapsedMetric, block_id)
}

case class BooleanScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                               metric: Option[Map[Boolean, Long]], block_id: Int) extends PreAggregateRow[Boolean]
case class UintScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                            metric: Option[Map[Int, Long]], block_id: Int) extends PreAggregateRow[Int]
case class LongScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                            metric: Option[Map[Long, Long]], block_id: Int) extends PreAggregateRow[Long]
case class StringScalarMapRow(experiment_id: String, branch: String, subgroup: String,
                              metric: Option[Map[String, Long]], block_id: Int) extends PreAggregateRow[String]


class BooleanScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame, numJackknifeBlocks: Int)
  extends MetricAnalyzer[Boolean](name, md, df, numJackknifeBlocks) {
  override type PreAggregateRowType = BooleanScalarMapRow
  val groupAggregator = GroupBooleanAggregator
  val finalAggregator = BooleanAggregator

  override def validateRow(row: BooleanScalarMapRow): Boolean = row.metric.isDefined

  def collapseKeys(formatted: DataFrame): Dataset[BooleanScalarMapRow] = {
    import df.sparkSession.implicits._
    val s = if (md.keyed) formatted.as[KeyedBooleanScalarRow] else formatted.as[BooleanScalarRow]
    s.map(_.toScalarMapRow)
  }
}

class UintScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame, numJackknifeBlocks: Int)
  extends MetricAnalyzer[Int](name, md, df, numJackknifeBlocks) {
  override type PreAggregateRowType = UintScalarMapRow
  val groupAggregator = GroupUintAggregator
  val finalAggregator = UintAggregator

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

class LongScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame, numJackknifeBlocks: Int)
  extends MetricAnalyzer[Long](name, md, df, numJackknifeBlocks) {
  override type PreAggregateRowType = LongScalarMapRow
  val groupAggregator = GroupLongAggregator
  val finalAggregator = LongAggregator

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

class StringScalarAnalyzer(name: String, md: ScalarDefinition, df: DataFrame, numJackknifeBlocks: Int)
  extends MetricAnalyzer[String](name, md, df, numJackknifeBlocks) {
  override type PreAggregateRowType = StringScalarMapRow
  val groupAggregator = GroupStringAggregator
  val finalAggregator = StringAggregator

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
  def getAnalyzer(name: String, sd: ScalarDefinition, df: DataFrame, numJackknifeBlocks: Int): MetricAnalyzer[_] = {
    sd match {
      case s: BooleanScalar => new BooleanScalarAnalyzer(name, s, df, numJackknifeBlocks)
      case s: UintScalar => new UintScalarAnalyzer(name, s, df, numJackknifeBlocks)
      case s: StringScalar => new StringScalarAnalyzer(name, s, df, numJackknifeBlocks)
      case _ => throw new UnsupportedOperationException("Unsupported scalar type")
    }
  }
}
