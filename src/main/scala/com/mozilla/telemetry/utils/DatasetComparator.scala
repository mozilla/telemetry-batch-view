package com.mozilla.telemetry.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop._

object DatasetComparator {
  def jobName: String = "dataset_comparator"

  case class Columns(originalOnly: Seq[String], testOnly: Seq[String], both: Seq[String])
  case class Count(original: Long, test: Long, change: Long)
  case class Values(originalOnly: Long, testOnly: Long)
  case class Result(columns: Columns, nulls: Map[String, Count], rows: Count, values: Values, same: Boolean = false)

  // Configuration for command line arguments
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val date = opt[String]("date", descr = "Date to compare", required = true)
    val dateField = opt[String]("date-field", descr = "Name of date partition field", required = false, default = Some("submission_date_s3"))
    val dataPath = opt[String]("data-path", descr = "Path in buckets to data", required = false, default = Some("main_summary/v4"))
    val failWithoutMatch = opt[Boolean]("fail-without-match", descr = "If true, exit 1 if data does not match", required = false, default = Some(true))
    val originalBucket = opt[String]("original-bucket", descr = "Source bucket for original data", required = false, default = Some("telemetry-parquet"))
    val originalViewname = opt[String]("original-viewname", descr = "View to pull original data from. Cannot be used with --original-bucket", required = false)
    val select = opt[String]("select", descr = "Comma separated list of columns that will be directly compared with DataFrame.except", required = false, default = Some("document_id"))
    val testBucket = opt[String]("test-bucket", descr = "Source bucket for test data", required = false, default = Some("telemetry-test-bucket"))
    val testViewname = opt[String]("test-viewname", descr = "View to pull test data from. Cannot be used with --test-bucket", required = false)
    val where = opt[String]("where", descr = "Filter statement for the data to compare; e.g. \"sample_id = '0'\"", required = false)
    val printResult = opt[Boolean]("print-result", descr = "Whether or not to print job results", required = false, default = Some(true))
    val resultPath = opt[String]("result-path", descr = "S3 location to write results to in json format", required = false)

    conflicts(originalViewname, List(originalBucket))
    conflicts(testViewname, List(testBucket))
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    // Set up Spark
    val spark = getOrCreateSparkSession(jobName)
    val result = compare(spark, conf)
    output(spark, conf, result)
    spark.stop()
    if (!result.same && conf.failWithoutMatch()) {
      System.exit(1)
    }
  }

  def output(spark: SparkSession, conf: Conf, result: Result): Unit = {
    if (conf.resultPath.isDefined) {
      import spark.sqlContext.implicits._
      List(result).toDF.write.mode("overwrite").json(conf.resultPath())
    }

    if (conf.printResult()) {
      println(s"Common columns: ${result.columns.both.mkString(",")}")
      println(s"Added columns: ${result.columns.testOnly.mkString(",")}")
      println(s"Dropped columns: ${result.columns.originalOnly.mkString(",")}")
      println(s"Row count went from ${result.rows.original} to ${result.rows.test}")
      println(s"Added ${result.values.testOnly} row values")
      println(s"Dropped ${result.values.originalOnly} row values")
      result.nulls.foreach { pair =>
        val (col, count) = pair
        println(s"Null values in '$col' went from ${count.original} to ${count.test}")
      }
      println(s"Datasets are${if (result.same) "" else " not"} the same")
    }
  }

  def compare(spark: SparkSession, conf: Conf): Result = {
    val original = getDataFrame(spark, conf.originalViewname, conf.originalBucket, conf.dataPath(), conf.dateField(), conf.date(), conf.where)
    val test = getDataFrame(spark, conf.testViewname, conf.testBucket, conf.dataPath(), conf.dateField(), conf.date(), conf.where)

    val result: Result = Result(
      columns = checkColumns(original, test),
      nulls = checkNulls(original, test),
      rows = checkRows(original, test),
      values = checkValues(original, test, conf.select())
    )
    val same = checkSame(result)

    result.copy(same = same)
  }

  def getDataFrame(spark: SparkSession, view: ScallopOption[String], bucket: ScallopOption[String], dataPath: String, dateField: String, date: String, where: ScallopOption[String]): DataFrame = {
    val df = view.get match {
      case Some(t) => spark.sql(s"SELECT * FROM $t").where(s"$dateField = '$date'")
      case _ => spark.read.option("mergeSchema", "true").parquet(s"s3://${bucket()}/$dataPath/$dateField=$date")
    }
    where.get match {
      case Some(clause) => df.where(clause)
      case None => df
    }
  }

  // intersection and difference of columns
  def checkColumns(original: DataFrame, test: DataFrame): Columns = {
    val originalColumns: Set[String] = original.columns.map{c=>s"$c"}.toSet
    val testColumns: Set[String] = test.columns.map{c=>s"$c"}.toSet

    Columns(
      originalColumns.diff(testColumns).toSeq,
      testColumns.diff(originalColumns).toSeq,
      (originalColumns & testColumns).toSeq
    )
  }

  // null count by column
  def checkNulls(original: DataFrame, test: DataFrame): Map[String, Count] = {
    val intersectCols = (original.columns.toSet & test.columns.toSet).toSeq
    val query = intersectCols.map(col => s"sum(cast(($col is null) as int)) as $col")
    val originalNulls = original.selectExpr(query:_*).first().toSeq
    val testNulls = test.selectExpr(query:_*).first().toSeq

    (intersectCols zip (originalNulls zip testNulls)).map{ m =>
      val (col: String, (original: Long, test: Long)) = m
      col -> Count(original, test, test - original)
    }.toMap
  }

  // row count
  def checkRows(original: DataFrame, test: DataFrame): Count = {
    val originalCount = original.count
    val testCount = test.count

    Count(
      original = originalCount,
      test = testCount,
      change = testCount - originalCount
    )
  }

  // distinct value count for `select` columns that DataFrame.except works on
  def checkValues(original: DataFrame, test: DataFrame, select: String): Values = {
    val originalColumns = selectColumns(select, original)
    val testColumns = selectColumns(select, test)

    Values(
      originalOnly = originalColumns.except(testColumns).count,
      testOnly = testColumns.except(originalColumns).count
    )
  }

  // select columns from `df` where `select` is a comma separated list of columns
  def selectColumns(select: String, df: DataFrame): DataFrame = df.select(
    select.split(",").map{c=>df.col(c)}:_*
  )

  def checkSame(result: Result): Boolean = {
    result.columns.originalOnly.isEmpty &&
    result.columns.testOnly.isEmpty &&
    result.nulls.forall(pair => pair._2.change == 0) &&
    result.rows.change == 0 &&
    result.values.originalOnly == 0 &&
    result.values.testOnly == 0
  }
}
