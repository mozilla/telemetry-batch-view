/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.mozilla.telemetry.utils.aggregation
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class AggregationTest extends FlatSpec {
  private val tol = 1e-7

  "The weighted mode" must "combine repeated keys" in {
    val mode = aggregation.weightedMode(Seq(("DE", 3L), ("IT", 6L), ("DE", 4L)))
    assert(mode == Some("DE"))
  }

  it must "respect weights" in {
    val mode = aggregation.weightedMode(Seq(("DE", 3L), ("IT", 1L), ("IT", 1L)))
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
