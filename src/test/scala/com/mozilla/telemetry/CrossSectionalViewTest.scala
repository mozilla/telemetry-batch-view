package com.mozilla.telemetry

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.views._
import CrossSectionalView._
import Aggregation._
import org.scalatest.FlatSpec
import org.apache.spark.sql.Dataset

class CrossSectionalViewTest extends FlatSpec {
  def compareDS(actual: Dataset[CrossSectional], expected: Dataset[CrossSectional]) = {
    actual.collect.zip(expected.collect)
      .map(xx => xx._1 == xx._2)
      .reduce(_ && _)
  }

  "CrossSectional" must "be calculated correctly" in {
    val sparkConf = new SparkConf().setAppName("CrossSectionalTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val longitudinalDataset = Seq(
      new Longitudinal("a", Option(Seq("DE", "DE", "IT")), Option(Seq(2, 3, 4))),
      new Longitudinal("b", Option(Seq("EG", "EG", "DE")), Option(Seq(1, 1, 2)))
    ).toDS

    val actual = longitudinalDataset.map(new CrossSectional(_))
    val expected = Seq(
      new CrossSectional("a", Option("DE")),
      new CrossSectional("b", Option("EG"))).toDS

    assert(compareDS(actual, expected))
    sc.stop()
  }

  "Modes" must "combine repeated keys" in {
    val ll = new Longitudinal(
      "id",
      Option(Seq("DE", "IT", "DE")),
      Option(Seq(3, 6, 4)))
    val country = modalCountry(ll)
    assert(country == Some("DE"))
  }

  it must "respect session weight" in {
    val ll = new Longitudinal(
      "id",
      Option(Seq("DE", "IT", "IT")),
      Option(Seq(3, 1, 1)))
    val country = modalCountry(ll)
    assert(country == Some("DE"))
  }

  "DataSetRows" must "distinguish between unequal rows" in {
    val l1 = new Longitudinal("id", Some(Seq("DE")), Some(Seq(1)))
    val l2 = new Longitudinal("other_id", Some(Seq("DE")), Some(Seq(1)))

    assert(l1 != l2)
  }

  it must "acknowledge equal rows" in {
    val l1 = new Longitudinal("id", Some(Seq("DE")), Some(Seq(1)))
    val l2 = new Longitudinal("id", Some(Seq("DE")), Some(Seq(1)))

    println(l1.valSeq.hashCode)
    println(l2.valSeq.hashCode)
    assert(l1 == l2)
  }
}
