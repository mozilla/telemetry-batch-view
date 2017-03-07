package com.mozilla.telemetry

import com.mozilla.telemetry.views.AddonsView
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
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
                 is_system: Boolean)

case class PartialMain(document_id: String,
                       client_id: String,
                       sample_id: Long,
                       subsession_start_date: String,
                       normalized_channel: String,
                       active_addons: Seq[Addon])

class AddonsViewTest extends FlatSpec with Matchers{
  "Addon records" can "be extracted from MainSummary" in {
    val spark = SparkSession
      .builder()
      .appName("AddonsViewTest")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    try {
      val mains = Seq(
        PartialMain("doc1", "client1", 1l, "2016-10-01T00:00:00", "release", Seq(
          Addon("addon1", true, "addon1name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, true),
          Addon("addon2", true, "addon2name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, false))),
        PartialMain("doc2", "client2", 2l, "2016-10-01T00:00:00", "release", null),
        PartialMain("doc3", "client2", 2l, "2016-10-01T00:00:00", "release", Seq()),
        PartialMain("doc4", "client3", 3l, "2016-10-01T00:00:00", "release", Seq(
          Addon("addon1", true, "addon1name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, true),
          Addon("addon3", true, "addon3name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, false),
          Addon("addon4", true, "addon4name", false, false, "1.0", 1, "plugin", true, false, 10, 11, 0, false)))
      ).toDS().toDF()

      mains.count() should be(4)
      val addons = AddonsView.addonsFromMain(mains)

      addons.count() should be(7)

      addons.where("addon_id is null").count() should be (2)
      addons.where("addon_id is not null").count() should be (5)

      addons.select("client_id").distinct().count() should be(3)

      // Null plus the 4 actual ids.
      addons.select("addon_id").distinct().count() should be(5)

      addons.where("addon_id == 'addon1'").select("client_id").distinct().count() should be(2)
    } finally {
      spark.stop()
    }
  }
}
