/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.views.AddonsView
import org.scalatest.{FlatSpec, Matchers}

case class Addon(addon_id: String,
                 blocklisted: Boolean,
                 name: String,
                 user_disabled: Boolean,
                 app_disabled: Boolean,
                 version: String,
                 scope: Integer,
                 `type`: String,
                 foreign_install: Boolean,
                 has_binary_components: Boolean,
                 install_day: Integer,
                 update_day: Integer,
                 signed_state: Integer,
                 is_system: Boolean,
                 is_web_extension: Boolean,
                 multiprocess_compatible: Boolean)

object Addon {
  def apply(addon_id: String, blocklisted: Boolean = true): Addon = {
    Addon(addon_id, blocklisted, "blah", true, true, "1", 1, "blah", true, true, 1, 1, 1, true, true, true)
  }
}

case class PartialMain(document_id: String,
                       client_id: String,
                       sample_id: Long,
                       subsession_start_date: String,
                       normalized_channel: String,
                       active_addons: Seq[Addon])

class AddonsViewTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  "Addon records" can "be extracted from MainSummary" in {
    import spark.implicits._

    val mains = Seq(
      PartialMain("doc1", "client1", 1L, "2016-10-01T00:00:00", "release", Seq(
        Addon("addon1", true, "addon1name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, true, false, true),
        Addon("addon2", true, "addon2name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, false, true, true))),
      PartialMain("doc2", "client2", 2L, "2016-10-01T00:00:00", "release", null),
      PartialMain("doc3", "client2", 2L, "2016-10-01T00:00:00", "release", Seq()),
      PartialMain("doc4", "client3", 3L, "2016-10-01T00:00:00", "release", Seq(
        Addon("addon1", true, "addon1name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, true, false, true),
        Addon("addon3", true, "addon3name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, false, true, false),
        Addon("addon4", true, "addon4name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, false, false, false)))
    ).toDS().toDF()

    mains.count() should be(4)
    val addons = AddonsView.addonsFromMain(mains)

    addons.count() should be(7)

    addons.where("addon_id is null").count() should be(2)
    addons.where("addon_id is not null").count() should be(5)

    addons.select("client_id").distinct().count() should be(3)

    // Null plus the 4 actual ids.
    addons.select("addon_id").distinct().count() should be(5)

    addons.where("addon_id == 'addon1'").select("client_id").distinct().count() should be(2)

    addons.where("multiprocess_compatible").count() should be(3)
  }
}
