package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.scalars.{ScalarDefinition, StringScalar, UintScalar}
import com.mozilla.telemetry.utils.userdefinedfunctions.AggregateScalars
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Row}


sealed abstract class ScalarAnalyzer(name: String, sd: ScalarDefinition, df: DataFrame) extends MetricAnalyzer(name, sd, df) {
  def handleKeys: Column = if (sd.keyed) keyedUDF(col("metric")) else col(s"metric.value")
}

class UintScalarAnalyzer(name: String, sd: UintScalar, df: DataFrame) extends ScalarAnalyzer(name, sd, df) {
  override type AggregateType = Map[Long, Long]
  val reducer = new AggregateScalars[Long](LongType)
  lazy val keyedUDF: UserDefinedFunction = udf(collapseKeys _)

  def collapseKeys(m: Map[String, Seq[Row]]): Seq[Long] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(_.map {
        case Row(v: Long) => v
        case _ => 0L
      }).map(_.sum).toSeq
    }
  }

  def aggToMap(values: AggregateType): Map[Long, Row] = {
    val sum = values.toSeq.map(_._2).sum.toDouble
    // Histograms are output as Key -> Row(prob dist (pdf), count, label)
    values.map {
      case (k: Long, v: Long) => k -> Row(v.toDouble / sum, v, null)
    }
  }
}

class StringScalarAnalyzer(name: String, sd: StringScalar, df: DataFrame) extends ScalarAnalyzer(name, sd, df) {
  override type AggregateType = Map[String, Long]
  val reducer = new AggregateScalars[String](StringType)

  def collapseKeys(m: Map[String, Seq[Row]]): Seq[String] = {
    m match {
      case null => List()
      case _ => m.values.transpose.flatMap(_.map {
        case Row(v: String) => v
        case _ => null
      }).toSeq
    }
  }

  lazy val keyedUDF: UserDefinedFunction = udf(collapseKeys _)

  def aggToMap(values: AggregateType): Map[Long, Row] = {
    val sum = values.toSeq.map(_._2).sum.toDouble
    // Histograms are output as Key -> Row(prob dist (pdf), count, label)
    values.toSeq.sortWith(_._2 > _._2).zipWithIndex.map {
      // Histograms are output as Key -> Row(prob dist (pdf), count, label)
      case ((k: String, v: Long), i: Int) => i.toLong -> Row(v.toDouble / sum, v, k)
    }.toMap
  }
}

// TODO: Boolean Scalars (we don't have any yet)
