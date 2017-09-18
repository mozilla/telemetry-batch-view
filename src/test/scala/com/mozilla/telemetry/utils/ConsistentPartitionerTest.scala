package com.mozilla.telemetry

import com.github.nscala_time.time.Imports._
import com.mozilla.telemetry.utils.CustomPartitioners._
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll}

import scala.io.Source

case class row(id: String, sample_id: String)

class ConsistentPartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val spark = getOrCreateSparkSession("Partitioner Test")
  import spark.implicits._

  val df = List(row("a", "24"), row("b", "49"), row("c", "74"), row("d", "99")).toDS.toDF
  val toDouble = (v: String) => v.toInt / 100.0

  def getPartitionIds(df: DataFrame): Set[Set[String]] = {
    df.rdd.mapPartitions(p => Iterator(p.map(r => r.getAs[String]("id")).toSet)).collect().toSet
  }

  "Consistent Partitioner" can "properly partition a dataframe" in {
    val res = df.consistentRepartition(2, "sample_id", toDouble)
    getPartitionIds(res) should be (Set(Set("a", "b"), Set("c", "d")))
  }

  it can "properly repartition a dataframe with more partitions" in {
    val res = df.consistentRepartition(4, "sample_id", toDouble)
    getPartitionIds(res) should be (Set(Set("a"), Set("b"), Set("c"), Set("d")))
  }

  it can "properly repartition a dataframe with empty partitions" in {
    val res = df.consistentRepartition(8, "sample_id", toDouble)
    getPartitionIds(res) should be (Set(Set(), Set("a"), Set(), Set("b"), Set(), Set("c"), Set(), Set("d")))
  }

  it can "properly handle values outside the range [0, 1]" in {
    val res = List(row("e", "110")).toDS.toDF.consistentRepartition(4, "sample_id", toDouble)
    getPartitionIds(res) should be (Set(Set("e"), Set(), Set(), Set()))
  }

  it can "properly handle negative values outside the range [0, 1]" in {
    val res = List(row("e", "-110")).toDS.toDF.consistentRepartition(4, "sample_id", toDouble)
    getPartitionIds(res) should be (Set(Set("e"), Set(), Set(), Set()))
  }

  it can "properly handle null values" in {
    val res = List(row("f", null)).toDS.toDF.consistentRepartition(2, "sample_id", toDouble)
    getPartitionIds(res) should be (Set(Set("f"), Set()))
  }

  override def afterAll() = {
    spark.stop()
  }
}

