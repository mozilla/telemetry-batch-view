package com.mozilla.telemetry

import com.mozilla.telemetry.utils.aggregation
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class AggregationTest extends FlatSpec {
  private val tol= 1e-7

  "The weighted mode" must "combine repeated keys" in {
    val mode = aggregation.weightedMode(Seq(("DE", 3l), ("IT", 6l), ("DE", 4l)))
    assert(mode == Some("DE"))
  }

  it must "respect weights" in {
    val mode = aggregation.weightedMode(Seq(("DE", 3l), ("IT", 1l), ("IT", 1l)))
    assert(mode == Some("DE"))
  }

  "The weighted mean" must "handle empty sequences" in {
    assert(aggregation.weightedMean(Seq()) == None)
  }
  
  it must "handle 0 weights" in {
    assert(aggregation.weightedMean(Seq((1, 0))) == None)
  }

  it must "respect weights" in {
    aggregation.weightedMean(Seq((1, 1), (2, 2))).getOrElse(0.0) shouldBe ((5.0 / 3) +- tol)
  }
}
