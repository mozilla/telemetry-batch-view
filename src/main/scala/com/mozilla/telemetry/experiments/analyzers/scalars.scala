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

  def toFinalSchema(rows: List[Row], summary_stats: List[List[Row]], test_stats: List[List[Row]]): List[Row] = {
    def aggToMap(values: Map[Long, Long]): Map[Long, Row] = {
      val sum = values.toSeq.map(_._2).sum.toDouble
      // Histograms are output as Key -> Row(prob dist (pdf), count, label)
      values.map {case (k: Long, v: Long) => k -> Row(v.toDouble / sum, v, null)}
    }

    rows.map {
      case Row(experiment:    String,
               branch:        String,
               subgroup:      String,
               n:             Long,
               histogram_seq: Map[Long, Long],
               metric_name:   String,
               metric_type:   String) => Row(experiment, branch, subgroup, n, metric_name, metric_type,
                                             aggToMap(histogram_seq), summary_stats ++ test_stats)
    }
  }
}

class StringScalarAnalyzer(name: String, sd: StringScalar, df: DataFrame) extends ScalarAnalyzer(name, sd, df) {
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

  def toFinalSchema(rows: List[Row], summary_stats: List[List[Row]], test_stats: List[List[Row]]): List[Row] = {
    def aggToMap(values: Map[String, Long]): Map[Long, Row] = {
      val sum = values.toSeq.map(_._2).sum.toDouble
      // Sort the string keys by number of occurrences and assign each an index to fit the schema
      values.toSeq.sortWith(_._2 > _._2).zipWithIndex.map {
        // Histograms are output as Key -> Row(prob dist (pdf), count, label)
        case ((k: String, v: Long), i: Int) => i.toLong -> Row(v.toDouble / sum, v, k)
      }.toMap
    }

    rows.map {
      case Row(experiment:    String,
               branch:        String,
               subgroup:      String,
               n:             Long,
               histogram_seq: Map[String, Long],
               metric_name:   String,
               metric_type:   String) => Row(experiment, branch, subgroup, n, metric_name, metric_type,
                                             aggToMap(histogram_seq), summary_stats ++ test_stats)
    }
  }
}

// TODO: Boolean Scalars (we don't have any yet)
