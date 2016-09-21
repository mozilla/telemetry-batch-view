package com.mozilla.telemetry

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.views._
import CrossSectionalView._
import org.scalatest.{FlatSpec, TestFailedException}
import org.apache.spark.sql.Dataset

class CrossSectionalViewTest extends FlatSpec {

  // Add compare method to classes extending the Product trait (including
  // case classes)
  // TODO(harterrt):Consider moving to triple equals: http://www.scalatest.org/user_guide/using_assertions
  implicit class ComparableProduct(p: Product) {
    def compare(that: Product, epsilon: Double = 1E-7) = {
      // Performs fuzzy comparison of two datasets containing Products (usually
      // case classes)
      //
      // Fuzzy matching handles floating point comparison properly.
      // It's better not to override equality because this functions will not
      // necessarily preserve transitivity.
      // Do the comparison
      def compareElement(pair: (Any, Any)) = {
        pair match {
          case (Some(first: Double), Some(second: Double)) => Math.abs(first - second) < epsilon
          case (first: Any, second: Any) => first == second
          case _ => false
        }
      }

      def productToSeq(prod: Product): Seq[Any] = {
        // Creates a Seq containing the fields of an object extending the 
        // Product trait
        (0 until prod.productArity).map(prod.productElement(_))
      }

      p.productArity == that.productArity && 
        (productToSeq(p) zip productToSeq(that))
          .foldLeft(true)((acc, pair) => acc && compareElement(pair)
      )
    }
  }

  def compareDS(actual: Dataset[CrossSectional], expected: Dataset[CrossSectional]) = {
    actual.collect.zip(expected.collect)
      .foldLeft(true)((acc, pair) => acc && (pair._1 compare  pair._2))
  }

  def getExampleLongitudinal(
    client_id: String, active_addons: Option[Seq[Map[String, ActiveAddons]]] = None
  ) = {
    new Longitudinal(
        client_id = client_id
      , normalized_channel = "release"
      , submission_date = Some(Seq("2016-01-01T00:00:00.0+00:00"
      ,   "2016-01-02T00:00:00.0+00:00", "2016-01-03T00:00:00.0+00:00"))
      , geo_country = Some(Seq("DE", "DE", "IT"))
      , session_length = Some(Seq(3600, 7200, 14400))
      , is_default_browser = Some(Seq(Some(true), Some(true), Some(true)))
      , default_search_engine = Some(Seq(Some("grep"), Some("grep"), Some("grep")))
      , locale = Some(Seq(Some("de_DE"), Some("de_DE"), None))
      , architecture = Some(Seq(None, Some("arch"), Some("arch")))
      , active_addons = active_addons
    )
  }

  def getExampleCrossSectional(
      client_id: String
    , addon_count_foreign_avg: Option[Double] = None
    , addon_count_foreign_cfgs: Option[Long] = None
    , addon_count_foreign_mode: Option[Long] = None
    , addon_count_avg: Option[Double] = None
    , addon_count_cfgs: Option[Long] = None
    , addon_count_mode: Option[Long] = None
  ) = {
    new CrossSectional(
        client_id = client_id
      , normalized_channel = "release"
      , active_hours_total = 25200 / 3600.0
      , active_hours_0_mon = 0.0
      , active_hours_1_tue = 0.0
      , active_hours_2_wed = 0.0
      , active_hours_3_thu = 0.0
      , active_hours_4_fri = 3600/3600.0
      , active_hours_5_sat = 7200/3600.0
      , active_hours_6_sun = 14400 / 3600.0
      , geo_mode = Some("IT")
      , geo_cfgs = 2
      , architecture_mode = Some("arch")
      , ffLocale_mode = None
      , addon_count_foreign_avg = addon_count_foreign_avg
      , addon_count_foreign_cfgs = addon_count_foreign_cfgs
      , addon_count_foreign_mode = addon_count_foreign_mode
      , addon_count_avg = addon_count_avg
      , addon_count_cfgs = addon_count_cfgs
      , addon_count_mode = addon_count_mode
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

  it must "summarize active_addons properly" in {
    def getExampleActiveAddon(foreign: Boolean = false) = {
      new ActiveAddons(
        Some(false), Some("Description"), Some("Name"), Some(false),
        Some(false), Some("Version"), Some(1), Some("Type"), Some(foreign),
        Some(false), Some(1), Some(1), Some(1)
      )
    }

    val actual = new CrossSectional(getExampleLongitudinal(
        client_id = "a"
      , active_addons = Some(Seq(
            Map(("one", getExampleActiveAddon()), ("two", getExampleActiveAddon()))
          , Map(("one", getExampleActiveAddon()), ("two", getExampleActiveAddon(true)))
          , Map(("one", getExampleActiveAddon()), ("two", getExampleActiveAddon(true)))
      ))
    ))

    val expected = getExampleCrossSectional(
        client_id = "a"
      , addon_count_foreign_avg = Some(2.0 / 3.0)
      , addon_count_foreign_cfgs = Some(2)
      , addon_count_foreign_mode = Some(1)
      , addon_count_avg = Some(2.0)
      , addon_count_cfgs = Some(2)
      , addon_count_mode = Some(2)
    )
    println(actual)
    println(expected)

    assert(actual compare expected)
  }
}
