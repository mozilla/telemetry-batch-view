package com.mozilla.telemetry

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.views._
import CrossSectionalView._
import org.scalatest.FlatSpec
import org.apache.spark.sql.Dataset

class CrossSectionalViewTest extends FlatSpec {
  def compareDS(actual: Dataset[CrossSectional], expected: Dataset[CrossSectional]) = {
    // Performs fuzzy comparison of two datasets containing Products (usually
    // case classes)
    //
    // Fuzzy matching handles floating point comparison properly.
    // It's better not to override equality because this functions will not
    // necessarily preserve transitivity.
    def compareRow(base: Product, test: Product, epsilon: Double = 1E-7) = {
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

      base.productArity == test.productArity && 
        (productToSeq(base) zip productToSeq(test))
          .foldLeft(true)((acc, pair) => acc && compareElement(pair)
      )
    }

    // Do the comparison
    actual.collect.zip(expected.collect)
      .foldLeft(true)((acc, pair) => acc && compareRow(pair._1, pair._2))
  }

  def getExampleLongitudinal(client_id: String) = {
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
    )
  }

  def getExampleCrossSectional(client_id: String) = {
    new CrossSectional(
        client_id = client_id
      , normalized_channel = "release"
      , active_hours_total = 25200
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
}
