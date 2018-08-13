/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.dau

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

trait GenericDauTraitTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  val hllCols: List[String] = "dau" :: "mau" :: "smoothed_dau" :: "er" :: Nil

  def nonHllCols(df: DataFrame): List[String] = df.columns.filter(!hllCols.contains(_)).toList

  def average(input: Seq[Int]): Double = input.sum / input.length

  def writeInput(conf: GenericDauConf, input: DataFrame): Unit = input
    .write
    .mode("overwrite")
    .partitionBy(conf.inputDateColumn)
    .parquet(conf.inputBasePath)

  def readOutput(conf: GenericDauConf): DataFrame = spark.read.parquet(conf.outputBasePath)

  def sortOutput(df: DataFrame): DataFrame = {
    val orderBy = nonHllCols(df).sorted
    df
      // sort columns
      .select(hllCols.head, hllCols.tail ::: orderBy:_*)
      // sort rows
      .orderBy(orderBy.head, orderBy.tail:_*)
  }

  def assertDauDataFrameEquals(expect: DataFrame, result: DataFrame): Unit = {
    // assert that non-hll columns are all exactly equal
    assertDataFrameEquals(expect.drop(hllCols:_*), result.drop(hllCols:_*))

    // assert that hll columns are less than 5% different
    expect.createOrReplaceTempView("expect")
    result.createOrReplaceTempView("result")
    assertEmpty(
      // find first 10 rows differing by > 5%
      spark.sql(
        s"""SELECT
           |  ${hllCols.map{c=>s"e.$c AS expect_$c,"}.mkString("\n  ")}
           |  r.*
           |FROM
           |  expect AS e,
           |  result AS r
           |WHERE
           |  ${nonHllCols(expect).map{c=>s"e.$c = r.$c"}.mkString("\n  AND ")}
           |  AND (
           |    ${hllCols.map{c=>s"abs(1-(r.$c/e.$c)) > 0.05"}.mkString("\n    OR ")}
           |  )
           |LIMIT 10
        """.stripMargin
      )
        // format as json to preserve column names in error message
        .toJSON
        .collect
    )
  }

  def testGenericDauTraitMain(view: GenericDauTrait, args: Array[String], input: DataFrame, expect: DataFrame): Unit = {
    val conf = view.getGenericDauConf(args)
    // put input on disk for main
    writeInput(
      conf,
      input
    )
    // execute aggregation via main
    view.main(args)
    // check result
    assertDauDataFrameEquals(
      // sort expect and result the same way
      sortOutput(expect),
      result = sortOutput(
        // read output from disk
        readOutput(conf)
          // don't validate the values in hllColumn
          .drop(conf.hllColumn)
      )
    )
  }

  def testGenericDauTraitAggregate(view: GenericDauTrait, args: Array[String], input: DataFrame, expect: DataFrame): Unit = {
    val conf = view.getGenericDauConf(args)
    // check result
    assertDauDataFrameEquals(
      // sort expect and result the same way
      sortOutput(expect),
      result = sortOutput(
        // execute aggregation directly
        view
          .aggregate(input, conf)
          // don't validate the values in hllColumn
          .drop(conf.hllColumn)
      )
    )
  }
}
