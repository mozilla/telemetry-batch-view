package org.apache.spark.ml.evaluation

import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType}

final class NaNRegressionEvaluator(override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("regEval"))

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("mse", "rmse", "r2", "mae"))
    new Param(this, "metricName", "metric name in evaluation (mse|rmse|r2|mae)", allowedParams)
  }

  def getMetricName: String = $(metricName)

  def setMetricName(value: String): this.type = set(metricName, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  val dropNaN: BooleanParam = new BooleanParam(this, "dropNaN",
    "whether to drop rows where 'predictionCol' is NaN. NOTE - only set this to true if you are " +
      "certain that NaN predictions should be ignored! (default: false)")

  def getDropNaN: Boolean = $(dropNaN)

  def setDropNaN(value: Boolean): this.type = set(dropNaN, value)

  setDefault(metricName -> "rmse", dropNaN -> false)

  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnTypes(schema, $(predictionCol), Seq(DoubleType, FloatType))
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val predictionAndLabels = dataset
      .select(col($(predictionCol)).cast(DoubleType), col($(labelCol)).cast(DoubleType))
      .na.drop("any", if ($(dropNaN)) Seq($(predictionCol)) else Seq())
      .rdd
      .map { case Row(prediction: Double, label: Double) => (prediction, label) }
    val metrics = new RegressionMetrics(predictionAndLabels)
    val metric = $(metricName) match {
      case "rmse" => metrics.rootMeanSquaredError
      case "mse" => metrics.meanSquaredError
      case "r2" => metrics.r2
      case "mae" => metrics.meanAbsoluteError
    }
    metric
  }

  override def isLargerBetter: Boolean = $(metricName) match {
    case "rmse" => false
    case "mse" => false
    case "r2" => true
    case "mae" => false
  }

  override def copy(extra: ParamMap): NaNRegressionEvaluator = defaultCopy(extra)
}

object NaNRegressionEvaluator extends DefaultParamsReadable[NaNRegressionEvaluator] {
  override def load(path: String): NaNRegressionEvaluator = super.load(path)
}
