package com.mozilla.telemetry


import com.mozilla.telemetry.utils.Aggregation._
import com.mozilla.telemetry.views._
import CrossSectionalView._
import org.scalatest.{FlatSpec}
import org.apache.spark.sql.Dataset

import hiveContext.implicits._

class CrossSectionalViewTest extends FlatSpec {
  val longitudinalDataset = Seq(
    Longitudinal("a", Option(Seq("DE", "DE", "IT")), Option(Seq(2, 3, 4))),
    Longitudinal("b", Option(Seq("EG", "EG", "DE")), Option(Seq(1, 1, 2)))
  ).toDS

  def compareDS(actual: Dataset[CrossSectional], expected: Dataset[CrossSectional]) = {
    actual.collect.zip(expected.collect)
      .map(xx => xx._1 == xx._2)
      .reduce(_ && _)
  }

  "CrossSectional" must "be calculated correctly" in {
    val expected = Seq(
      CrossSectional("a", Option("DE")),
      CrossSectional("b", Option("EG"))).toDS
    val actual = longitudinalDataset.map(generateCrossSectional)

    assert(compareDS(actual, expected))
  }

  "Modes" must "combine repeated keys" in {
    val ll = Longitudinal(
      "id",
      Option(Seq("DE", "IT", "DE")),
      Option(Seq(3, 6, 4)))
    val country = modalCountry(ll)
    assert(country == Some("DE"))
  }

  it must "respect session weight" in {
    val ll = Longitudinal(
      "id",
      Option(Seq("DE", "IT", "IT")),
      Option(Seq(3, 1, 1)))
    val country = modalCountry(ll)
    assert(country == Some("DE"))
  }
}
