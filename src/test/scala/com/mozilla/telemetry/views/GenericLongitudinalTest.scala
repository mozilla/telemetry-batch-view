package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

class GenericLongitudinalTest extends FlatSpec {
  val tablename = "test_table"
  val colNames = List("random_data", "client_id", "submission_date_s3", "os", "ordering")
  val data = List(
    (201742, "1", "20170101", "Android", 1),
    (199,"1", "20170103", "Android", 3),
    (42, "1", "20170102", "iOS", 2),
    (44, "1", "20171212", "nothing", 4),
    (4242, "2", "20160102", "Android", 1),
    (2424, "2", "20160101", null, 2)
  )

  val baseArgs =
    "--tablename" :: tablename ::
    "--from" :: "20160101" ::
    "--to" :: "20171212" ::
    "--output-path" :: "missing-bucket" :: Nil

  val args =
    "--max-array-length" :: "3" :: baseArgs

  val intOrderingArgs =
    "--ordering-columns" :: "ordering" :: baseArgs

  val secondaryOrderingArgs =
    "--ordering-columns" :: "os,ordering" :: baseArgs

  val alternateGroupingArgs =
    "--grouping-column" :: "os" :: baseArgs

  val nestedDataTableName = "nested_table"
  val nestedDataColNames = List("group", "order", "nest1", "nest2", "map", "submission_date_s3")
  val nestedData = List(
    (1, 2, ("hello", 42), ("hello", 42), Map("hello" -> "world"), "20170101"),
    (1, 1, ("world", 84), ("world", 84), Map("answer" -> "42"), "20170101")
  )

  val nestedArgs =
    "--tablename" :: nestedDataTableName ::
    "--to" :: "20170201" ::
    "--output-path" :: "missing-bucket" ::
    "--grouping-column" :: "group" ::
    "--ordering-columns" :: "order" :: Nil

  val longTempTableName = "longTempTable"
  val longData = List(Row("a", null, 2, "1"), Row("a", 1: Long, 1, "2"))
  val longSchema =
    StructType(List(
      StructField("client_id", StringType, nullable = false),
      StructField("long_val", LongType, nullable = true),
      StructField("ordering", IntegerType, nullable = false),
      StructField("submission", StringType, nullable = false)))

  val longArgs =
    "--tablename" :: longTempTableName ::
    "--from" :: "0" ::
    "--to" :: "4" ::
    "--submission-date-col" :: "submission" ::
    "--output-path" :: "missing-bucket" ::
    "--ordering-columns" :: "long_val" :: Nil

  val fixture = {
    new {
      private val spark = getOrCreateSparkSession("Generic Longitudinal Test")

      private val sc = spark.sparkContext
      private val hiveContext = new HiveContext(sc)

      import spark.implicits._
      data.toDF(colNames: _*).registerTempTable(tablename)

      private val opts = new GenericLongitudinalView.Opts(args.toArray)
      private val groupedDf = GenericLongitudinalView.run(hiveContext, opts)

      val longitudinal = groupedDf.collect()
      val fieldIndex: String => Integer = longitudinal.head.fieldIndex

      val intOrdering = GenericLongitudinalView.run(
        hiveContext,
        new GenericLongitudinalView.Opts(intOrderingArgs.toArray)
      ).collect()

      val secondaryOrdering = GenericLongitudinalView.run(
        hiveContext,
        new GenericLongitudinalView.Opts(secondaryOrderingArgs.toArray)
      ).collect()

      val alternateGrouping = GenericLongitudinalView.run(
        hiveContext,
        new GenericLongitudinalView.Opts(alternateGroupingArgs.toArray)
      ).collect()

      nestedData.toDF(nestedDataColNames: _*).registerTempTable(nestedDataTableName)
      val nestedGroup = GenericLongitudinalView.run(
        hiveContext,
        new GenericLongitudinalView.Opts(nestedArgs.toArray)
      ).collect()

      // Test Long sorting - can't use List.toDF for nulled Long
      spark.createDataFrame(sc.parallelize(longData), longSchema).registerTempTable(longTempTableName)
      val groupedLongDF = GenericLongitudinalView.run(
        hiveContext,
        new GenericLongitudinalView.Opts(longArgs.toArray)
      ).collect()

      spark.stop()
    }
  }

  "longitudinal" must "have grouping column first" in {
    assert(fixture.longitudinal.head.fieldIndex("client_id") == 0)
  }

  "longitudinal" must "contain all groups" in {
    val clientIds = fixture.longitudinal.map(_.get(fixture.fieldIndex("client_id"))).toList
    assert(clientIds == List("1", "2"))
  }

  "longitudinal" must "correctly aggregate strings" in {
    val oss = fixture.longitudinal.map(_.getSeq(fixture.fieldIndex("os")).toList).toList
    assert(oss == List(List("nothing", "Android", "iOS"), List("Android", null)))
  }

  "longitudinal" must "order output correctly" in {
    val ordering = fixture.longitudinal
                   .filter(_.get(fixture.fieldIndex("client_id")) == "1")
                   .map(_.getSeq(fixture.fieldIndex("ordering")))
                   .head
                   .toList
    assert(ordering == List(4, 3, 2))
  }

  "longitudinal" must "trim long histories" in {
    val row = fixture.longitudinal.filter(_.get(fixture.fieldIndex("client_id")) == "1").head
    for(i <- 1 to (row.size - 1)){
      assert(row.getSeq(i).length == 3)
    }
  }

  "longitudinal" must "allow for null elements" in {
    val row = fixture.longitudinal
              .filter(_.get(fixture.fieldIndex("client_id")) == "2")
              .head

    val numNulls =
      (1 to (row.size - 1))
      .map(row.getSeq(_).contains(null))
      .map(if(_) 1 else 0)
      .reduce(_ + _)

    assert(numNulls == 1)
  }

  "longitudinal" must "sort null last" in {
    val row = fixture.secondaryOrdering
              .filter(_.get(fixture.fieldIndex("client_id")) == "2")
              .map(_.getSeq[String](fixture.fieldIndex("os")))
              .head

    assert(row.indexOf(null) == row.length - 1)
  }

  "ordering" must "work for integer type" in {
    val ordering = fixture.intOrdering
                   .filter(_.get(fixture.fieldIndex("client_id")) == "1")
                   .map(_.getSeq[Int](fixture.fieldIndex("ordering")))
                   .head
                   .toList
    assert(ordering == List(4, 3, 2, 1))
  }

  "secondary ordering" must "work for string-integer combination" in {
    val ordering = fixture.secondaryOrdering
                   .filter(_.get(fixture.fieldIndex("client_id")) == "1")
                   .head
                   .getSeq[Int](fixture.fieldIndex("ordering"))
                   .toList
    assert(ordering == List(4, 2, 3, 1))
  }

  "grouping" must "allow null values" in {
    assert(fixture.alternateGrouping.length == 4)
    assert(fixture.alternateGrouping.toList.map(_.get(0)).toSet == Set("Android", "iOS", "nothing", null))
  }

  "struct type" must "be converted correctly" in {
    val nestedCol = fixture.nestedGroup
                    .head.getSeq[Row](2)

    assert(nestedCol(0) == Row("hello", 42))
    assert(nestedCol(1) == Row("world", 84))
  }

  "map type" must "be converted correctly" in {
    val mapCol = fixture.nestedGroup
                 .head.getSeq[Map[String, String]](4)

    assert(mapCol(0) == nestedData(0)._5)
    assert(mapCol(1) == nestedData(1)._5)
  }

  "Long type" must "handle nulls correctly" in {
    val orderCol = fixture.groupedLongDF
                   .head
                   .getSeq[Int](2)
    assert(orderCol == List(1, 2))
  }
}
