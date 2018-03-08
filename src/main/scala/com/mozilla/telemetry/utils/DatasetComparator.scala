package com.mozilla.telemetry.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop._

object DatasetComparator {
  def jobName: String = "dataset_comparator"

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

    conflicts(originalViewname, List(originalBucket))
    conflicts(testViewname, List(testBucket))
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    // Set up Spark
    val spark = getOrCreateSparkSession(jobName)
    val valid = validate(spark, conf)
    spark.stop()
    if (!valid && conf.failWithoutMatch()) {
      System.exit(1)
    }
  }

  def getDataFrame(spark: SparkSession, view: ScallopOption[String], bucket: ScallopOption[String], dataPath: String, dateField: String, date: String, where: ScallopOption[String]): DataFrame = {
    val df = view.get match {
      case Some(t) => spark.sql(s"SELECT * FROM $t").where(s"$dateField = '$date'")
      case _ => spark.read.parquet(s"s3://${bucket()}/$dataPath/$dateField=$date")
    }
    where.get match {
      case Some(clause) => df.where(clause)
      case None => df
    }
  }

  def validate(spark: SparkSession, conf: Conf): Boolean = {
    val dataPath = conf.dataPath()
    val date = conf.date()
    val dateField = conf.dateField()
    val select = conf.select()

    println("=======================================================================================")
    println(s"BEGINNING JOB $jobName FOR $date")

    val original = getDataFrame(spark, conf.originalViewname, conf.originalBucket, dataPath, dateField, date, conf.where)
    val test = getDataFrame(spark, conf.testViewname, conf.testBucket, dataPath, dateField, date, conf.where)

    var same = true

    // compare row counts
    val originalCount = original.count
    val testCount = test.count
    if (originalCount != testCount) {
      println(s"Wrong number of rows. Expected $originalCount. Got $testCount")
      same = false
    }

    // compare null count in each column
    val intersectCols = (original.columns.toSet & test.columns.toSet).toSeq

    val query = intersectCols.map(col => s"sum(cast(($col is null) as int)) as $col")
    val originalNulls = original.selectExpr(query:_*).first()
    val testNulls = test.selectExpr(query:_*).first()

    for ((col, (original, test)) <- intersectCols zip (originalNulls.toSeq zip testNulls.toSeq)) {
      if (original != test)  {
        println(s"Wrong number of null values in '$col'. Expected $original. Got $test")
        same = false
      }
    }

    original.columns.foreach{ col =>
      if (!(test.columns contains col)) {
        println(s"Missing column '$col'.")
        same = false
      }
    }

    // check for extra columns
    test.columns.foreach { col =>
      if (!(original.columns contains col)) {
        println(s"Extra column '$col'.")
        same = false
      }
    }

    // compare comparable columns
    val originalColumns = original.select(select)
    val testColumns = test.select(select)

    val missing = originalColumns.except(testColumns).count
    if (missing > 0) {
      println(s"$missing missing value(s)")
      same = false
    }

    val extra = testColumns.except(originalColumns).count
    if (extra > 0) {
      println(s"$extra extra value(s)")
      same = false
    }

    val status = if (same) "SUCCESS" else "FAILURE"

    println(s"$status ON JOB $jobName FOR $date")
    println("=======================================================================================")

    same
  }
}
