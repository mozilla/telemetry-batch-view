package com.mozilla.telemetry

import com.mozilla.telemetry.utils.aggregation
import org.scalatest.FlatSpec

class AggregationTest extends FlatSpec {
  "The weighted mode" must "combine repeated keys" in {
    val mode = aggregation.weightedMode(Seq("DE", "IT", "DE"), Seq(3, 6, 4))
    assert(mode == "DE")
  }

  it must "respect session weight" in {
    val mode = aggregation.weightedMode(Seq("DE", "IT", "IT"), Seq(3, 1, 1))
    assert(mode == "DE")
  }
}
