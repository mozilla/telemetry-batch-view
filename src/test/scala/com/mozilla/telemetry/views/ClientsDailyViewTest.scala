/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.views.ClientsDailyViewTestHelpers._
import org.scalatest.{FlatSpec, Matchers}

class ClientsDailyViewTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  def test(table: List[MainSummaryRow], expect: Map[String, Any]): Unit = {
    import spark.implicits._
    val aggregated = ClientsDailyView.extractDayAggregates(table.toDF)
    expect.foreach { pair =>
      (pair._1, aggregated.selectExpr(pair._1).collect.last(0)) should be (pair)
    }
  }

  "aggregates" must "handle 'first' properly" in {
    // test profile_age_in_days and profile_creation_date
    // WARNING requires UTC, can be set on the jvm with -Duser.timezone=UTC
    test(
      List(
        // these rows do not handle nulls gracefully
        MainSummaryRow(
          subsession_start_date = Some("2018-01-05"),
          profile_creation_date = Some(17532) // 2018-01-01
        )
      ),
      Map(
        "profile_age_in_days" -> 4,
        "profile_creation_date" -> "2018-01-01 00:00:00"
      )
    )
    // test that first collects non-null values
    test(
      List(
        MainSummaryRow(),
        getRowAggFirst(Some("first"), Some(true), Some(1)),
        getRowAggFirst(Some("second"), Some(false), Some(0))
      ),
      getExpectAggFirst("first", true, 1)
    )
    // test that first collects falsey values
    test(
      List(
        getRowAggFirst(Some(""), Some(false), Some(0)),
        getRowAggFirst(Some("second"), Some(true), Some(1))
      ),
      getExpectAggFirst("", false, 0)
    )
  }

  it must "handle 'max' properly" in {
    test(
      List(
        getRowAggMax(Some(1)),
        getRowAggMax(Some(3)),
        MainSummaryRow()
      ),
      getExpectAggMax(3)
    )
  }

  it must "handle 'mean' properly" in {
    // test that mean works with single values
    test(
      List(
        getRowAggMean(Some(100))
      ),
      getExpectAggMean(100)
    )
    // test that mean ignores null values
    test(
      List(
        getRowAggMean(Some(100)),
        getRowAggMean(Some(0)),
        MainSummaryRow()
      ),
      getExpectAggMean(50)
    )
  }

  it must "handle 'sum' properly" in {
    // test that sum handles null values, and collects values <= 0 correctly
    test(
      List(
        getRowAggSum(Some(1)),
        getRowAggSum(Some(0)),
        getRowAggSum(Some(-2)),
        getRowAggSum(Some(5)),
        MainSummaryRow()
      ),
      getExpectAggSum(4)
    )
  }

  it must "handle 'experiments' properly" in {
    test(
      List(
        MainSummaryRow(),
        MainSummaryRow(experiments = Some(Map("A" -> None, "B" -> None))),
        MainSummaryRow(experiments = Some(Map("A" -> Some("1"), "B" -> Some("2"), "C" -> None, "D" -> None))),
        MainSummaryRow(experiments = Some(Map("C" -> Some("3")))),
        MainSummaryRow(experiments = Some(Map("D" -> Some("4")))),
        MainSummaryRow(experiments = Some(Map("A" -> Some("4"), "B" -> Some("3"), "C" -> Some("2"), "D" -> Some("1")))),
        MainSummaryRow(experiments = Some(Map("B" -> Some("1"), "C" -> Some("2"))))
      ),
      Map(
        "experiments['A']" -> "1",
        "experiments['B']" -> "2",
        "experiments['C']" -> "3",
        "experiments['D']" -> "4"
      )
    )
  }

  it must "handle 'geolocation' properly" in {
    // test geo aggregates as a set on presence of country
    test(
      List(
        MainSummaryRow(),
        MainSummaryRow(country = Some("??"), city = Some("Tilehurst"), geo_subdivision1 = Some("ENG"), geo_subdivision2 = Some("WBK")),
        MainSummaryRow(country = Some("CA")),
        MainSummaryRow(country = Some("US"), geo_subdivision1 = Some("WA")),
        MainSummaryRow(country = Some("US"), city = Some("Portland"), geo_subdivision1 = Some("OR")),
        MainSummaryRow(country = Some("GB"), city = Some("Tilehurst"), geo_subdivision1 = Some("ENG"), geo_subdivision2 = Some("WBK")),
        MainSummaryRow()
      ),
      Map(
        "country" -> "CA",
        "city" -> "??",
        "geo_subdivision1" -> "??",
        "geo_subdivision2" -> "??"
      )
    )
    test(
      List(
        MainSummaryRow(),
        MainSummaryRow(country = Some("US"), geo_subdivision1 = Some("WA")),
        MainSummaryRow(country = Some("US"), city = Some("Portland"), geo_subdivision1 = Some("OR")),
        MainSummaryRow(country = Some("GB"), city = Some("Tilehurst"), geo_subdivision1 = Some("ENG"), geo_subdivision2 = Some("WBK")),
        MainSummaryRow()
      ),
      Map(
        "country" -> "US",
        "city" -> "??",
        "geo_subdivision1" -> "WA",
        "geo_subdivision2" -> "??"
      )
    )
    test(
      List(
        MainSummaryRow(),
        MainSummaryRow(country = Some("US"), city = Some("Portland"), geo_subdivision1 = Some("OR")),
        MainSummaryRow(country = Some("GB"), city = Some("Tilehurst"), geo_subdivision1 = Some("ENG"), geo_subdivision2 = Some("WBK")),
        MainSummaryRow()
      ),
      Map(
        "country" -> "US",
        "city" -> "Portland",
        "geo_subdivision1" -> "OR",
        "geo_subdivision2" -> "??"
      )
    )
  }

  it must "handle 'search_counts' properly" in {
    // test search_counts
    test(
      List(
        // two rows with counts to get summed
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(1)),
          SearchCount(None, Some("contextmenu"), Some(2)),
          SearchCount(None, Some("newtab"), Some(3)),
          SearchCount(None, Some("searchbar"), Some(4)),
          SearchCount(None, Some("system"), Some(5)),
          SearchCount(None, Some("urlbar"), Some(6)),
          SearchCount(None, Some("invalid"), Some(7)),
          SearchCount(None, None, Some(8))
        ))),
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(1)),
          SearchCount(None, Some("contextmenu"), Some(2)),
          SearchCount(None, Some("newtab"), Some(3)),
          SearchCount(None, Some("searchbar"), Some(4)),
          SearchCount(None, Some("system"), Some(5)),
          SearchCount(None, Some("urlbar"), Some(6)),
          SearchCount(None, Some("invalid"), Some(7)),
          SearchCount(None, None, Some(8))
        ))),
        // a row of invalid counts
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(-1)),
          SearchCount(None, Some("contextmenu"), Some(-2)),
          SearchCount(None, Some("newtab"), Some(-3)),
          SearchCount(None, Some("searchbar"), Some(-4)),
          SearchCount(None, Some("system"), Some(-5)),
          SearchCount(None, Some("urlbar"), Some(-6)),
          SearchCount(None, Some("invalid"), Some(-7)),
          SearchCount(None, None, Some(-8))
        ))),
        // an empty row
        MainSummaryRow(),
        // a row of null counts
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), None),
          SearchCount(None, Some("contextmenu"), None),
          SearchCount(None, Some("newtab"), None),
          SearchCount(None, Some("searchbar"), None),
          SearchCount(None, Some("system"), None),
          SearchCount(None, Some("urlbar"), None),
          SearchCount(None, Some("invalid"), None),
          SearchCount(None, None, None)
        ))),
        // a row of 1s and a row of 0s for good measure
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(1)),
          SearchCount(None, Some("contextmenu"), Some(1)),
          SearchCount(None, Some("newtab"), Some(1)),
          SearchCount(None, Some("searchbar"), Some(1)),
          SearchCount(None, Some("system"), Some(1)),
          SearchCount(None, Some("urlbar"), Some(1)),
          SearchCount(None, Some("invalid"), Some(1)),
          SearchCount(None, None, Some(1))
        ))),
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(0)),
          SearchCount(None, Some("contextmenu"), Some(0)),
          SearchCount(None, Some("newtab"), Some(0)),
          SearchCount(None, Some("searchbar"), Some(0)),
          SearchCount(None, Some("system"), Some(0)),
          SearchCount(None, Some("urlbar"), Some(0)),
          SearchCount(None, Some("invalid"), Some(0)),
          SearchCount(None, None, Some(0))
        )))
      ),
      Map(
        "search_count_all" -> 48,
        "search_count_abouthome" -> 3,
        "search_count_contextmenu" -> 5,
        "search_count_newtab" -> 7,
        "search_count_searchbar" -> 9,
        "search_count_system" -> 11,
        "search_count_urlbar" -> 13
      )
    )
  }

  it must "handle 'subsessions_started_on_this_day' properly" in {
    // test subsessions_started_on_this_day counts 1s
    test(
      List(
        MainSummaryRow(subsession_counter = Some(1)),
        MainSummaryRow(subsession_counter = Some(0)),
        MainSummaryRow(subsession_counter = Some(1)),
        MainSummaryRow(subsession_counter = Some(2)),
        MainSummaryRow()
      ),
      Map("sessions_started_on_this_day" -> 2)
    )
  }

  it must "sum count histograms properly" in {
    test(
      List(
        MainSummaryRow(),
        getRowCountHistograms(100),
        getRowCountHistograms(1)
      ),
      getExpectCountHistogramSum(101)
    )
  }
}
