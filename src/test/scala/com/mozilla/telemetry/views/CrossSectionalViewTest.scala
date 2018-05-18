package com.mozilla.telemetry

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.mozilla.telemetry.views._
import org.apache.spark.sql.Dataset
import org.scalatest.FlatSpec

class CrossSectionalViewTest extends FlatSpec with DatasetSuiteBase {

  // Add compare method to classes extending the Product trait (including
  // case classes)
  // TODO(harterrt):Consider moving to triple equals: http://www.scalatest.org/user_guide/using_assertions
  implicit class ComparableProduct(p: Product) {
    def compare(that: Product, epsilon: Double = 1E-7) = {
      // Performs fuzzy comparison of two datasets containing Products (usually
      // case classes)
      //
      // Fuzzy matching handles floating point comparison properly.
      // It's better not to override equality because this functions will not
      // necessarily preserve transitivity.
      // Do the comparison
      def compareElement(pair: (Any, Any)) = {
        pair match {
          case (Some(first: Double), Some(second: Double)) => Math.abs(first - second) < epsilon
          case (first: Any, second: Any) => first == second
          case _ => false
        }
      }

      def productToSeq(prod: Product): Seq[Any] = {
        // Creates a Seq containing the fields of an object extending the 
        // Product trait
        (0 until prod.productArity).map(prod.productElement(_))
      }

      p.productArity == that.productArity && 
        (productToSeq(p) zip productToSeq(that))
          .foldLeft(true)((acc, pair) => acc && compareElement(pair)
      )
    }
  }

  def compareDS(actual: Dataset[CrossSectional], expected: Dataset[CrossSectional]): Boolean = {
    actual.collect.zip(expected.collect)
      .foldLeft(true)((acc, pair) => acc && (pair._1 compare  pair._2))
  }

  def getExampleActiveAddon(foreign: Boolean = false, name: String = "Name") = {
    new ActiveAddon(
      Some(false), Some("Description"), Some(name), Some(false),
      Some(false), Some("Version"), Some(1), Some("Type"), Some(foreign),
      Some(false), Some(1), Some(1), Some(1), Some(false)
    )
  }

  def getExampleActivePlugin() = {
    new ActivePlugin(
      Some("Name"), Some("Version"), Some("Description"), Some(true),
      Some(false), Some(false), Some(Seq("mime1", "mime2")), Some(123)
    )
  }

  def getExampleLongitudinal(
    client_id: String,
    active_addons: Option[Seq[Map[String, ActiveAddon]]] = None
  ) = {
    new Longitudinal(
      client_id = client_id,
      normalized_channel = "release",
      submission_date = Some(Seq("2016-01-01T00:00:00.0+00:00",
        "2016-01-02T00:00:00.0+00:00", "2016-01-07T00:00:00.0+00:00")),
      geo_country = Some(Seq("DE", "DE", "IT")),
      session_length = Some(Seq(3600, 7200, 14400)),
      is_default_browser = Some(Seq(Some(true), Some(false), Some(true))),
      default_search_engine = Some(Seq(Some("hooli"), Some("grep"), Some("grep"))),
      locale = Some(Seq(Some("de_DE"), Some("de_DE"), None)),
      architecture = Some(Seq(None, Some("arch"), Some("arch"))),
      active_addons = active_addons,
      bookmarks_sum = Some(Seq(Some(3), Some(2), Some(2))),
      cpu_count = Some(Seq(Some(1), Some(2), Some(1))),
      channel = Some(Seq(Some("release"),Some("release"),Some("release"))),
      subsession_start_date = Some(Seq("2016-01-01T00:00:00.0+00:00",
        "2016-01-02T00:00:00.0+00:00", "2016-01-05T00:00:00.0+00:00")),
      version = Some(Seq(Some("42.0"), Some("41.0"), Some("41.0"))),
      reason = Some(Seq("daily", "shutdown", "shutdown")),
      memory_mb = Some(Seq(Some(1023), Some(1023), Some(1023))),
      os_name = Some(Seq(Some("Windows_NT"), Some("Windows_NT"), Some("Windows_NT"))),
      os_version = Some(Seq(Some("5.1"), Some("5.1"), Some("5.1"))),
      pages_count = Some(Seq(Some(100l), Some(200l), Some(400l))),
      active_plugins = Some(Seq(
        Seq(getExampleActivePlugin(), getExampleActivePlugin()),
        Seq(getExampleActivePlugin()),
        Seq(getExampleActivePlugin(), getExampleActivePlugin())
      )),
      previous_subsession_id = Some(Seq("one", "one", "two")),
      profile_creation_date = Some(Seq("2016-04-21T00:00:00.000Z",
        "2016-04-21T00:00:00.000Z", "2016-04-21T00:00:00.000Z")),
      profile_subsession_counter = Some(Seq(3,2,1)),
      search_counts = Some(Map("dogpile" -> Seq(1l, 0l, 2l, 10l),
                               "ask_jeeves" -> Seq(20l, 0l, 0l, 0l),
                               "hooli" -> Seq(5l, 4l, 0l, 0l))),
      session_id = Some(Seq("A", "B", "C")),
      application_name = Some(Seq(Some("firefox"), Some("firefox"), Some("firefox")))
    )
  }

  def getExampleCrossSectional(
    client_id: String,
    addon_count_foreign_avg: Option[Double] = None,
    addon_count_foreign_configs: Option[Long] = None,
    addon_count_foreign_mode: Option[Long] = None,
    addon_count_avg: Option[Double] = None,
    addon_count_configs: Option[Long] = None,
    addon_count_mode: Option[Long] = None,
    addon_names_list: Option[Seq[Option[String]]] = None
  ) = {
    new CrossSectional(
      client_id = client_id,
      normalized_channel = "release",
      session_hours_total = Some(25200 / 3600.0),
      session_hours_0_mon = Some(0.0),
      session_hours_1_tue = Some(14400 / 3600.0),
      session_hours_2_wed = Some(0.0),
      session_hours_3_thu = Some(0.0),
      session_hours_4_fri = Some(3600/3600.0),
      session_hours_5_sat = Some(7200/3600.0),
      session_hours_6_sun = Some(0.0),
      geo_mode = Some("IT"),
      geo_configs = 2,
      architecture_mode = Some("arch"),
      ffLocale_mode = None,
      addon_count_foreign_avg = addon_count_foreign_avg,
      addon_count_foreign_configs = addon_count_foreign_configs,
      addon_count_foreign_mode = addon_count_foreign_mode,
      addon_count_avg = addon_count_avg,
      addon_count_configs = addon_count_configs,
      addon_count_mode = addon_count_mode,
      number_of_pings = Some(3),
      bookmarks_avg = Some(15.0 / 7),
      bookmarks_max = Some(3),
      bookmarks_min = Some(2),
      cpu_count_mode = Some(1),
      channel_configs = Some(1),
      channel_mode = Some("release"),
      days_active = Some(3),
      days_active_0_mon = Some(0),
      days_active_1_tue = Some(1),
      days_active_2_wed = Some(0),
      days_active_3_thu = Some(0),
      days_active_4_fri = Some(1),
      days_active_5_sat = Some(1),
      days_active_6_sun = Some(0),
      days_possible = Some(5),
      days_possible_0_mon = Some(1),
      days_possible_1_tue = Some(1),
      days_possible_2_wed = Some(0),
      days_possible_3_thu = Some(0),
      days_possible_4_fri = Some(1),
      days_possible_5_sat = Some(1),
      days_possible_6_sun = Some(1),
      default_pct = Some(5.0 / 7),
      locale_configs = Some(2),
      locale_mode = None,
      version_configs = Some(2),
      version_max = Some("42.0"),
      application_name_mode = Some("firefox"),
      addon_names_list = addon_names_list,
      main_ping_reason_num_aborted = 0,
      main_ping_reason_num_end_of_day = 1,
      main_ping_reason_num_env_change = 0,
      main_ping_reason_num_shutdown = 2,
      memory_avg = Some(1023.0),
      memory_configs = Some(1),
      os_name_mode = Some("Windows_NT"),
      os_version_mode = Some("5.1"),
      os_version_configs = Some(1),
      pages_count_avg = Some(2100.0 / 7),
      pages_count_min = Some(100),
      pages_count_max = Some(400),
      plugins_count_avg = Some(12.0 / 7),
      plugins_count_configs = Some(2),
      plugins_count_mode = Some(2),
      start_date_oldest = Some("2016-01-01"),
      start_date_newest = Some("2016-01-05"),
      subsession_length_badTimer = Some(0),
      subsession_length_negative = Some(0),
      subsession_length_tooLong = Some(0),
      previous_subsession_id_repeats = Some(2),
      profile_creation_date = Some("2016-04-21T00:00:00.000Z"),
      profile_subsession_counter_min = Some(1),
      profile_subsession_counter_max = Some(3),
      profile_subsession_counter_configs = Some(3),
      search_counts_total = Some(42),
      search_default_configs = Some(2),
      search_default_mode = Some("grep"),
      session_num_total = Some(3),
      subsession_branches = Some(1),
      date_skew_per_ping_avg = Some(2.0 / 3),
      date_skew_per_ping_max = Some(2),
      date_skew_per_ping_min = Some(0)
    )
  }

  "CrossSectional" must "be calculated correctly" in {
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val longitudinalDataset = Seq(
      getExampleLongitudinal("a"), getExampleLongitudinal("b")
    ).toDS

    val actual = longitudinalDataset.map(new CrossSectional(_))
    val expected = Seq(
      getExampleCrossSectional("a"),
      getExampleCrossSectional("b")
    ).toDS

    assert(compareDS(actual, expected))
  }

  it must "summarize active_addons properly" in {
    def getExampleActiveAddon(foreign: Boolean = false, name: String = "Name") = {
      new ActiveAddon(
        Some(false), Some("Description"), Some(name), Some(false),
        Some(false), Some("Version"), Some(1), Some("Type"), Some(foreign),
        Some(false), Some(1), Some(1), Some(1), Some(false)
      )
    }
    val actual = new CrossSectional(getExampleLongitudinal(
      client_id = "a",
      active_addons = Some(Seq(
          Map(("one", getExampleActiveAddon()), ("two", getExampleActiveAddon())),
          Map(("one", getExampleActiveAddon(true)), ("two", getExampleActiveAddon(true, name="Test"))),
          Map(("two", getExampleActiveAddon(true, name="Test")), ("one", getExampleActiveAddon(true)))
      ))
    ))

    val expected = getExampleCrossSectional(
      client_id = "a",
      addon_count_foreign_avg = Some(12.0 / 7),
      addon_count_foreign_configs = Some(2),
      addon_count_foreign_mode = Some(2),
      addon_count_avg = Some(2.0),
      addon_count_configs = Some(1),
      addon_count_mode = Some(2),
      addon_names_list = Some(Seq(Some("Name"), Some("Test")))
    )

    assert(actual compare expected)
  }

  it must "identify adblockers properly" in {
    val actual = new CrossSectional(getExampleLongitudinal(
        client_id = "a"
      , active_addons = Some(Seq(
            Map(("one", getExampleActiveAddon()), ("two", getExampleActiveAddon()))
          , Map(("one", getExampleActiveAddon()), ("two", getExampleActiveAddon(name = "UBloCKer Plus Plus")))
          , Map(("one", getExampleActiveAddon()), ("two", getExampleActiveAddon(foreign = true)))
      ))
    ))

    val expected = getExampleCrossSectional(
      client_id = "a",
      addon_count_foreign_avg = Some(4.0 / 7.0),
      addon_count_foreign_configs = Some(2),
      addon_count_foreign_mode = Some(1),
      addon_count_avg = Some(2.0),
      addon_count_configs = Some(1),
      addon_count_mode = Some(2),
      addon_names_list = Some(Seq(Some("Name"), Some("UBloCKer Plus Plus")))
    )

    assert(actual compare expected)
  }

  it must "filter bad session lengths" in {
    val actual = new CrossSectional(
      getExampleLongitudinal("a").copy(session_length = Some(Seq(3600, 7200, -14400)))
    )

    val expected = getExampleCrossSectional("a").copy(
      subsession_length_negative = Some(1),
      bookmarks_avg = Some(7.0 / 3),
      memory_avg = Some(1023.0),
      pages_count_avg = Some(500.0 / 3),
      default_pct = Some(1.0 / 3),
      session_hours_total = Some(10800 / 3600.0),
      session_hours_0_mon = Some(0.0),
      session_hours_1_tue = Some(0.0),
      session_hours_2_wed = Some(0.0),
      session_hours_3_thu = Some(0.0),
      session_hours_4_fri = Some(3600/3600.0),
      session_hours_5_sat = Some(7200/3600.0),
      session_hours_6_sun = Some(0.0),
      geo_mode = Some("DE"),
      architecture_mode = Some("arch"),
      ffLocale_mode = Some("de_DE"),
      cpu_count_mode = Some(2),
      channel_mode = Some("release"),
      locale_mode = Some("de_DE"),
      os_name_mode = Some("Windows_NT"),
      os_version_mode = Some("5.1"),
      plugins_count_avg = Some(4.0 / 3),
      plugins_count_mode = Some(1),
      search_default_mode = Some("grep")
    )

    assert(actual compare expected)
  }

  it must "handle bad dates" in {
    val actual = new CrossSectional(
      getExampleLongitudinal("a").copy(
        submission_date = Some(Seq("2016-01-01T00:00:00.0+00:00", //Odd Date
          "invalid-02T00:00:00.0+00:00", "2016-01-07T00:00:00.0+00:00"))
      )
    )

    val expected = getExampleCrossSectional("a").copy(
      date_skew_per_ping_avg = Some(2.0 / 2),
      date_skew_per_ping_max = Some(2),
      date_skew_per_ping_min = Some(0)
    )

    assert(actual compare expected)
  }
}
