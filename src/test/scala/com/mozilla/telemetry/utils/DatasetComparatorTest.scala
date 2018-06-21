/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

case class MSRow(
  document_id:Int,
  submission_date_s3:String,
  client_id:Option[String] = None
)

class DatasetComparatorTest extends FlatSpec with DataFrameSuiteBase with BeforeAndAfterAll {

  private val date = "test"

  def getConf(original: String, test: String, where: String = "", select: String = "document_id",
              dateField: String = "submission_date_s3"): DatasetComparator.Conf = {
    val args =
      "--date" :: date ::
      "--date-field" :: dateField ::
      "--original-viewname" :: original ::
      "--select" :: select ::
      "--test-viewname" :: test ::
      {if (where == "") Nil else "--where" :: where :: Nil}
    new DatasetComparator.Conf(args.toArray)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    import spark.implicits._

    (0 to 10).map{e => MSRow(e,date)}.toDF.createOrReplaceTempView("zeroToTen")
    (0 to 9).map{e => MSRow(e,date)}.toDF.createOrReplaceTempView("zeroToNine")
    (0 to 11).map{e => MSRow(e,date)}.toDF.createOrReplaceTempView("zeroToEleven")
    (1 to 11).map{e => MSRow(e,date)}.toDF.createOrReplaceTempView("oneToEleven")
    (0 to 10).map{e => (0,date)}.toDF("document_id", "submission_date_s3").createOrReplaceTempView("ZeroNulls")
    (0 to 10).map{e => if (e % 2 == 0) MSRow(0,date,Some("c")) else MSRow(0,date)}.toDF.createOrReplaceTempView("FiveNulls")
    (1 to 11).map{e => if (e % 2 == 0) MSRow(0,date,Some("c")) else MSRow(0,date)}.toDF.createOrReplaceTempView("SixNulls")
  }

  "Simple frame" must "equal itself" in {
    assert(DatasetComparator.compare(spark, getConf("zeroToTen", "zeroToTen")).same)
  }

  it must "not equal one less row" in {
    assert(!DatasetComparator.compare(spark, getConf("zeroToTen", "zeroToNine")).same)
  }

  it must "equal one less row with where clause" in {
    assert(DatasetComparator.compare(spark, getConf("zeroToTen", "zeroToNine", "document_id < 10")).same)
  }

  it must "not equal one more row" in {
    assert(!DatasetComparator.compare(spark, getConf("zeroToTen", "zeroToEleven")).same)
  }

  it must "not equal one different row" in {
    assert(!DatasetComparator.compare(spark, getConf("zeroToTen", "oneToEleven")).same)
  }

  "Frame with nulls" must "equal itself" in {
    assert(DatasetComparator.compare(spark, getConf("FiveNulls", "FiveNulls")).same)
  }

  it must "not equal extra null row" in {
    assert(!DatasetComparator.compare(spark, getConf("FiveNulls", "SixNulls")).same)
  }

  it must "not equal missing column" in {
    assert(!DatasetComparator.compare(spark, getConf("FiveNulls", "ZeroNulls")).same)
  }

  it must "not equal extra column" in {
    assert(!DatasetComparator.compare(spark, getConf("ZeroNulls", "FiveNulls")).same)
  }
}
