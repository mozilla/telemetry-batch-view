/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.dau

import java.util.UUID.randomUUID

class GenericDauConfTest extends GenericDauTraitTest {
  object AllConfigsDau extends GenericDauTrait {
    val jobName: String = "generic_dau_test"

    def getGenericDauConf(args: Array[String]): GenericDauConf = GenericDauConf(
      from = "2018-01-31",
      to = "2018-02-01",
      inputBasePath = "/dev/null",
      outputBasePath = "/dev/null",
      inputDateColumn = "activity_date", // non-default input date column name
      inputDatePattern = "yyyy-MM-dd", // non-default output date pattern
      outputDateColumn = "submission_date", // non-default output date column name
      outputDatePattern = "yyMMdd", // non-default output date pattern
      additionalOutputColumns = List("hll"), // include colliding hll column in output
      hllColumn = "hyperloglog", // non-default value to avoid collision with "hll" column
      countColumn = Some("id"), // generate hlls on the fly
      minDauThreshold = Some(3), // exclude rows with dau < 3
      where = "hll != 'a'", // exclude rows with hll=a
      top = List(("country", 2)) // exclude country "A"
    )

    override def main(args: Array[String]): Unit = {
      throw new Exception("it's very dark. you are likely to be eaten by a grue")
    }
  }

  "configs" must "work together" in {
    import spark.implicits._
    testGenericDauTraitAggregate(
      AllConfigsDau,
      Array(),
      input = (
        // 9 rows excluded by "where", count for "top" is 3
        (1 to 9).map{_=>("2018-02-01", randomUUID.toString, randomUUID.toString, "A", "a")}.toList :::
        (1 to 3).map{_=>("2018-02-01", randomUUID.toString, randomUUID.toString, "A", "b")}.toList :::
        // dau of 4 and mau of 8, count for "top" is 8
        (1 to 4).map{_=>("2018-01-31", randomUUID.toString, randomUUID.toString, "B", "b")}.toList :::
        (1 to 4).map{_=>("2018-02-01", randomUUID.toString, randomUUID.toString, "B", "b")}.toList :::
        // 4 rows excluded by minDau, different "hll" column values for no mau overlap, count for "top" is 10
        (1 to 3).map{_=>("2018-01-31", randomUUID.toString, randomUUID.toString, "C", "c")}.toList :::
        (1 to 2).map{_=>("2018-01-31", randomUUID.toString, randomUUID.toString, "C", "d")}.toList :::
        (1 to 2).map{_=>("2018-02-01", randomUUID.toString, randomUUID.toString, "C", "c")}.toList :::
        (1 to 3).map{_=>("2018-02-01", randomUUID.toString, randomUUID.toString, "C", "d")}.toList :::
        // pad the list so that all needed dates exist
        (4 to 30).map { d => (f"2018-01-$d%02d", null, null, null, null) }.toList
      ).toDF("activity_date", "id", "ignore", "country", "hll"),
      expect = List(
        (4, 4, 4.0, 1.0, "B", "b", "180131"),
        (4, 8, 4.0, 0.5, "B", "b", "180201"),
        (3, 3, 3.0, 1.0, "C", "c", "180131"),
        (3, 3, 3.0, 1.0, "C", "d", "180201")
      ).toDF("dau", "mau", "smoothed_dau", "er", "country", "hll", "submission_date")
    )
  }
}
