package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.histograms._
import com.mozilla.telemetry.utils.userdefinedfunctions.AggregateHistograms
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


sealed abstract class HistogramAnalyzer(name: String, hd: HistogramDefinition, df: DataFrame) extends MetricAnalyzer(name, hd, df) {
  val reducer: AggregateHistograms
  val keys: List[Int]

  val keyedUDF: UserDefinedFunction
  def nonKeyedUDF: UserDefinedFunction
  def handleKeys: Column = if (hd.keyed) keyedUDF(col("metric")) else nonKeyedUDF(col("metric"))

  def filterExp(df: DataFrame): DataFrame

  override def runSummaryStatistics(rows: List[Row]): List[List[Row]] = {
    rows.map { case Row(_, _, _, _, histogram_agg: Seq[Long], _, _) => _runSummaryStatistics(histogram_agg) }
  }

  def _runSummaryStatistics(histogram_agg: Seq[Long]): List[Row] = {
    MetricAggregate(histogram_agg, keys).getSchematizedSummaryStats
  }

  def toFinalSchema(rows: List[Row], summary_stats: List[List[Row]], test_stats: List[List[Row]]): List[Row] = {
    def aggToMap(values: Seq[Long]): Map[Long, Row] = {
      val n = values.sum.toDouble
      // Histograms are output as Key -> Row(prob dist (pdf), count, label)
      // Labels are none for histograms for now (meant for string scalars) -- may be useful for enum histograms
      // in the future
      (keys zip values).map { case (k, v) => k.toLong -> Row(v.toDouble / n, v, null) }.toMap
    }

    rows.map {
      case Row(experiment:    String,
               branch:        String,
               subgroup:      String,
               n:             Long,
               histogram_seq: Seq[Long],
               metric_name:   String,
               metric_type:   String) => Row(experiment, branch, subgroup, n, metric_name, metric_type,
                                             aggToMap(histogram_seq), summary_stats ++ test_stats)
    }
  }
}

object HistogramAnalyzer {
  def sumFilterExp(df: DataFrame): DataFrame = {
    val sumArray: Seq[Int] => Int = _.sum
    val sumArrayUDF = udf(sumArray)
    df.filter(sumArrayUDF(col("metric")) > 0)
  }

  def collapseEnumKeys(m: Map[String, Seq[Seq[Int]]]): Seq[Seq[Int]] = {
    m match {
      case null => null
      // h: array of each key's histogram array
      // _: array of each key's values for a bucket
      case _ => m.values.transpose.map(h => h.transpose.map(_.sum).toSeq).toSeq

    }
  }

  def collapseRealKeys(m: Map[String, Seq[Row]]): Seq[Seq[Int]] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(
        h => h.map(r => r.getAs[Seq[Int]]("values")).transpose.map(_.sum).toSeq
      ).toSeq
    }
  }

  def prepEnumColumn(m: Seq[Seq[Int]]): Seq[Seq[Int]] = m
  def prepRealColumn(m: Seq[Row]): Seq[Seq[Int]] = m.map(_.getSeq[Int](0))
}

class FlagHistogramAnalyzer(name: String, hd: FlagHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df) {
  val reducer = new AggregateHistograms(2)
  val keys = List(0, 1)

  def filterExp(df: DataFrame): DataFrame = df

  def collapseKeys(m: Map[String, Seq[Boolean]]): Seq[Seq[Int]] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(
        h => h.map(x => if (x) Array(0, 1) else Array(1, 0)).transpose.map(_.sum).toSeq
      ).toSeq
    }
  }

  def prepColumn(m: Seq[Boolean]): Seq[Seq[Int]] = m.map(if (_) Seq(0, 1) else Seq(1, 0))

  lazy val keyedUDF: UserDefinedFunction = udf(collapseKeys _)
  lazy val nonKeyedUDF: UserDefinedFunction = udf(prepColumn _)
}

class CountHistogramAnalyzer(name: String, hd: CountHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df) {
  val reducer = new AggregateHistograms(1)
  val keys = List(0)

  def filterExp(df: DataFrame): DataFrame = HistogramAnalyzer.sumFilterExp(df)

  def collapseKeys(m: Map[String, Seq[Int]]): Seq[Seq[Int]] = {
    m match {
      case null => List()
      case _ => m.values.transpose.map(x => Seq(x.sum)).toSeq
    }
  }

  def prepColumn(m: Seq[Int]): Seq[Seq[Int]] = m.map(Seq(_))

  lazy val keyedUDF: UserDefinedFunction = udf(collapseKeys _)
  lazy val nonKeyedUDF: UserDefinedFunction = udf(prepColumn _)
}

class BooleanHistogramAnalyzer(name: String, hd: BooleanHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df) {
  val reducer = new AggregateHistograms(2)
  val keys = List(0, 1)

  def filterExp(df: DataFrame): DataFrame = HistogramAnalyzer.sumFilterExp(df)

  lazy val keyedUDF = udf(HistogramAnalyzer.collapseEnumKeys _)
  lazy val nonKeyedUDF = udf(HistogramAnalyzer.prepEnumColumn _)
}

class EnumeratedHistogramAnalyzer(name: String, hd: EnumeratedHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df) {
  val reducer = new AggregateHistograms(hd.nValues)
  val keys: List[Int] = (0 until hd.nValues).toList

  def filterExp(df: DataFrame): DataFrame = HistogramAnalyzer.sumFilterExp(df)

  lazy val keyedUDF = udf(HistogramAnalyzer.collapseEnumKeys _)
  lazy val nonKeyedUDF = udf(HistogramAnalyzer.prepEnumColumn _)
}

class LinearHistogramAnalyzer(name: String, hd: LinearHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df) {
  val reducer = new AggregateHistograms(hd.nBuckets)
  val keys: List[Int] = Histograms.linearBuckets(hd.low, hd.high, hd.nBuckets).toList

  def filterExp(df: DataFrame): DataFrame = HistogramAnalyzer.sumFilterExp(df)

  lazy val keyedUDF = udf(HistogramAnalyzer.collapseRealKeys _)
  lazy val nonKeyedUDF = udf(HistogramAnalyzer.prepRealColumn _)
}

class ExponentialHistogramAnalyzer(name: String, hd: ExponentialHistogram, df: DataFrame)
  extends HistogramAnalyzer(name, hd, df) {
  val reducer = new AggregateHistograms(hd.nBuckets)
  val keys: List[Int] = Histograms.exponentialBuckets(hd.low, hd.high, hd.nBuckets).toList

  def filterExp(df: DataFrame): DataFrame = HistogramAnalyzer.sumFilterExp(df)

  lazy val keyedUDF = udf(HistogramAnalyzer.collapseRealKeys _)
  lazy val nonKeyedUDF = udf(HistogramAnalyzer.prepRealColumn _)
}