/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.utils

import org.apache.spark.sql.Column
import org.scalatest.{FlatSpec, Matchers}

object Cols extends ColumnEnumeration {
  val first_one, second_one = ColumnDefinition()
  val custom_definition = ColumnDefinition(new Column("base_column") / 5)
}

class ColumnEnumerationTest extends FlatSpec with Matchers {

  "implicit defs" can "be accessed" in {
    Cols.first_one.name shouldBe "first_one"
    Cols.first_one.col.toString shouldBe "first_one"
  }

  "custom definition" can "be accessed" in {
    Cols.custom_definition.name shouldBe "custom_definition"
    Cols.custom_definition.col.toString shouldBe "custom_definition"
    Cols.custom_definition.expr.toString shouldBe "(base_column / 5) AS `custom_definition`"
  }

}
