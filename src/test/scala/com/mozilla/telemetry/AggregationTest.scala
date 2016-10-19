package com.mozilla.telemetry

import com.mozilla.telemetry.utils.aggregation
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class AggregationTest extends FlatSpec {
  private val tol= 1e-7

  "The weighted mode" must "combine repeated keys" in {
    val mode = aggregation.weightedMode(Seq("DE", "IT", "DE"), Seq(3, 6, 4))
    assert(mode == Some("DE"))
  }

  it must "respect weights" in {
    val mode = aggregation.weightedMode(Seq("DE", "IT", "IT"), Seq(3, 1, 1))
    assert(mode == Some("DE"))
  }

  "The weighted mean" must "handle empty sequences" in {
    assert(aggregation.weightedMean(Seq(), Seq()) == None)
  }
  
  it must "handle 0 weights" in {
    assert(aggregation.weightedMean(Seq(Some(1)), Seq(0)) == None)
  }

  it must "respect weights" in {
    aggregation.weightedMean(Seq(Some(1), Some(2)), Seq(1, 2)).getOrElse(0.0) shouldBe ((5.0 / 3) +- tol)
  }

  it must "exclude missing values" in {
    aggregation.weightedMean(Seq(Some(1), None), Seq(1, 2)).getOrElse(0.0) shouldBe (1.0 +- tol)
  }
}
