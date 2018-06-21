/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}


class PermutationsTest extends FlatSpec with Matchers with DatasetSuiteBase {
  val cutoffs = List(0.25d, 0.5d, 0.75d, 1.0d)
  "Two generated permutations with the same inputs" must "have the same output" in {
    val first = Permutations.weightedGenerator(cutoffs, "hello", 100)("world")
    val second = Permutations.weightedGenerator(cutoffs, "hello", 100)("world")

    first should equal (second)
  }

  "Generated permutations" should "conform to weights" in {
    val numTrials = 1000000
    val output = Permutations.weightedGenerator(cutoffs, "hello", numTrials)("world")
    val counts = output.groupBy(identity).mapValues(_.size)
    counts(0) / numTrials.toDouble should be (0.25 +- 0.005)
  }
}
