package com.mozilla.telemetry.views

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

class GenericLongitudinalTest extends FlatSpec {
  val fixture = {
    new {
      private val spark = SparkSession.builder()
        .appName("Generic Longitudinal Test")
        .master("local[1]")
        .getOrCreate()

      import spark.implicits._
      private val colNames = List("random_data", "client_id", "submission_date_s3", "os", "ordering")

      val data = List(
        (201742, "1", "20170101", "Android", 1),
        (199,"1", "20170103", "Android", 3),
        (42, "1", "20170102", "iOS", 2),
        (44, "1", "20171212", "nothing", 4),
        (4242, "2", "20160101", "Android", 1),
        (2424, "2", null, null, 2)
      )

      private val df = data.toDF(colNames: _*)
      private val groupedDf = GenericLongitudinalView.group(df, maxLength=Some(3))

      val longitudinal = groupedDf.collect()
      val fieldIndex: String => Integer = longitudinal.head.fieldIndex

      val intOrdering = GenericLongitudinalView.group(df, orderColumns=List("ordering")).collect()
      val secondaryOrdering = GenericLongitudinalView.group(df, orderColumns=List("os", "ordering")).collect()
      val alternateGrouping = GenericLongitudinalView.group(df, groupColumn="os").collect()

      private val nestedDataColNames = List("group", "order", "nest1", "nest2", "map")
      val nestedData = List(
        (1, 2, ("hello", 42), ("hello", 42), Map("hello" -> "world")),
        (1, 1, ("world", 84), ("world", 84), Map("answer" -> "42"))
      )

      val nestedGroup = GenericLongitudinalView.group(
        nestedData.toDF(nestedDataColNames: _*),
        groupColumn="group",
        orderColumns=List("order")
      ).collect()

      // Test Long sorting - can't use List.toDF for nulled Long
      private val longVal: Long = 1
      private val longRDD = spark.sparkContext.parallelize(List(Row("a", null, 2), Row("a", longVal, 1)))
      private val longSchema =
        StructType(List(
          StructField("client_id", StringType, nullable = false),
          StructField("long_val", LongType, nullable = true),
          StructField("ordering", IntegerType, nullable = false)))

      private val longDF = spark.createDataFrame(longRDD, longSchema)
      val groupedLongDF = GenericLongitudinalView.group(
        longDF,
        orderColumns=List("long_val")
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

    assert(numNulls == 2)
  }

  "longitudinal" must "sort null last" in {
    val row = fixture.longitudinal
              .filter(_.get(fixture.fieldIndex("client_id")) == "2")
              .map(_.getSeq[String](fixture.fieldIndex("submission_date_s3")))
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

    assert(mapCol(0) == fixture.nestedData(0)._5)
    assert(mapCol(1) == fixture.nestedData(1)._5)
  }

  "Long type" must "handle nulls correctly" in {
    val orderCol = fixture.groupedLongDF
                   .head
                   .getSeq[Int](2)
    assert(orderCol == List(1, 2))
  }
}
