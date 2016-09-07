package com.mozilla.telemetry

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.views._
import CrossSectionalView._
import org.scalatest.FlatSpec
import org.apache.spark.sql.Dataset

class CrossSectionalViewTest extends FlatSpec {
  def compareDS(actual: Dataset[CrossSectional], expected: Dataset[CrossSectional]) = {
    actual.collect.zip(expected.collect)
      .map(x=> x._1.compare(x._2))
      .reduce(_ && _)
  }

  def getExampleLongitudinal(client_id: String) = {
    new Longitudinal(
      client_id = client_id,
      normalized_channel = "release",
      submission_date = Some(Seq("2016-01-01T00:00:00.0+00:00",
        "2016-01-02T00:00:00.0+00:00", "2016-01-03T00:00:00.0+00:00")),
      geo_country = Some(Seq("DE", "DE", "IT")),
      session_length = Some(Seq(3600, 7200, 14400)),
      is_default_browser = Some(Seq(Some(true), Some(true), Some(true))),
      default_search_engine = Some(Seq(Some("grep"), Some("grep"), Some("grep"))),
      locale = Some(Seq(Some("de_DE"), Some("de_DE"), None)),
      architecture = Some(Seq(None, Some("arch"), Some("arch")))
    )
  }

  def getExampleCrossSectional(client_id: String) = {
    new CrossSectional(
      client_id = client_id,
      normalized_channel = "release",
      active_hours_total = 25200,
      active_hours_sun = 14400 / 3600.0,
      active_hours_mon = 0.0,
      active_hours_tue = 0.0,
      active_hours_wed = 0.0,
      active_hours_thu = 0.0,
      active_hours_fri = 3600/3600.0,
      active_hours_sat = 7200/3600.0,
      geo_Mode = Some("IT"),
      geo_Cfgs = 2,
      architecture_Mode = Some("arch"),
      ffLocale_Mode = None
    )
  }

  "CrossSectional" must "be calculated correctly" in {
    val sparkConf = new SparkConf().setAppName("CrossSectionalTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val longitudinalDataset = Seq(
      getExampleLongitudinal("a"), getExampleLongitudinal("b")
    ).toDS

    val actual = longitudinalDataset.map(new CrossSectional(_))
    val expected = Seq(
      getExampleCrossSectional("a"),
      getExampleCrossSectional("b")
    ).toDS

    assert(compareDS(actual, expected))
    sc.stop()
  }

  "DataSetRows" must "distinguish between unequal rows" in {
    val l1 = getExampleLongitudinal("id")
    val l2 = getExampleLongitudinal("other_id")

    assert(l1 != l2)
  }

  it must "acknowledge equal rows" in {
    val l1 = getExampleLongitudinal("id")
    val l2 = getExampleLongitudinal("id")

    println(l1.valSeq.hashCode)
    println(l2.valSeq.hashCode)
    assert(l1 == l2)
  }
}
