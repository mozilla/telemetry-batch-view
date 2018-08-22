/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import java.time.{Clock, LocalDate, ZoneId}

import org.scalatest.{FlatSpec, Matchers}

class BatchJobBaseTest extends FlatSpec with Matchers {
  private val base = new BatchJobBase {
    override val clock: Clock = Clock.fixed(
      LocalDate.of(2018, 4, 5).atStartOfDay(ZoneId.of("UTC")).toInstant, ZoneId.of("UTC"))
  }

  "Base streaming job" should "generate range of parsed dates for querying Dataset API" in {
    base.datesBetween("20180401", Some("20180401")) should contain theSameElementsInOrderAs Seq("20180401")
    base.datesBetween("20180401", Some("20180403")) should contain theSameElementsInOrderAs Seq("20180401", "20180402", "20180403")
    base.datesBetween("20180403", None) should contain theSameElementsInOrderAs Seq("20180403", "20180404")
  }
}
