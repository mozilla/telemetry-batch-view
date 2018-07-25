/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.statistics

import org.scalatest.{FlatSpec, Matchers}

class StatisticalComputationTest extends FlatSpec with Matchers {

  "PercentileComputation" must "generate correct ordinals" in {
    PercentileComputation(1).name shouldBe "1st Percentile"
    PercentileComputation(2).name shouldBe "2nd Percentile"
    PercentileComputation(3).name shouldBe "3rd Percentile"
    PercentileComputation(4).name shouldBe "4th Percentile"
    PercentileComputation(10).name shouldBe "10th Percentile"
    PercentileComputation(11).name shouldBe "11th Percentile"
    PercentileComputation(14).name shouldBe "14th Percentile"
    PercentileComputation(21).name shouldBe "21st Percentile"
    PercentileComputation(95).name shouldBe "95th Percentile"
    PercentileComputation(50).name shouldBe "Median"
  }

}
