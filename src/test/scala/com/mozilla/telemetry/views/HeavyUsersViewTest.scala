package com.mozilla.telemetry

import com.github.nscala_time.time.Imports._
import com.mozilla.telemetry.views.{HeavyUsersView, MainSummaryRow, HeavyUsersRow}
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll}

import scala.io.Source

class HeavyUsersViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val spark = getOrCreateSparkSession(HeavyUsersView.DatasetPrefix)
  private val fmt = DateTimeFormat.forPattern("yyyyMMdd")

  import spark.implicits._

  val mainSummary = List(
    MainSummaryRow("20170801", "client1", "42", 1),
    MainSummaryRow("20170801", "client2", "84", 1),
    MainSummaryRow("20170801", "client3", "21", 2)).toDS

  val cutoffs = Map(
    "20170801" -> 1.1,
    "20160801" -> 0.9
  )

  def makeTestHeavyUsers(clientId: String, sampleId: String, date: String): Dataset[HeavyUsersRow] = {
    val firstDay = fmt.parseDateTime(date)
    val hus = (i: Int) => if(i == 27) Some(true) else None
    (0 until 28)
      .map(d => (d, fmt.print(firstDay.plusDays(d))))
      .map{ case (i, date) => HeavyUsersRow(date, clientId, sampleId, 1, i + 1, hus(i), hus(i)) }
      .toDS
  }

  // case regular history
  "Heavy Users View" can "run with enough data" in {
    val (clientId, sampleId, date) = ("client1", "42", "20170704")
    val heavyUsers = makeTestHeavyUsers(clientId, sampleId, date)

    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801").collect()
    aggregate should contain (HeavyUsersRow("20170801", clientId, sampleId, 1, 28, Some(true), Some(true)))
  }

  it can "remove users who aren't active in past 28 days" in {
    val heavyUsers = List(
      HeavyUsersRow("20170704", "missingclient", "42", 2, 2, Some(false), Some(false)),
      HeavyUsersRow("20170731", "missingclient", "42", 0, 2, Some(false), Some(false))).toDS

    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801").collect()
    aggregate.map(_.client_id) should not contain ("missingclient")
  }

  //case no current data
  it can "bootstrap initial history" in {
    val heavyUsers = (Nil: List[HeavyUsersRow]).toDS

    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801").collect()
    aggregate.toSet should be (
      mainSummary.collect().map{ r =>
        HeavyUsersRow(
          r.submission_date_s3, r.client_id, r.sample_id,
          r.active_ticks, r.active_ticks, None, None)
      } toSet
    )
  }

  //case some current data (not full history)
  it can "boostrap on partial history" in {
    val heavyUsers = List(HeavyUsersRow("20170731", "client1", "42", 1, 2, Some(false), Some(false))).toDS

    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801").collect()
    aggregate should contain (HeavyUsersRow("20170801", "client3", "21", 2, 2, None, None))
  }

  //case user not active on current day - still contain
  it can "include user even if not active on current day" in {
    val (clientId, sampleId, date) = ("client5", "4", "20170704")
    val heavyUsers = makeTestHeavyUsers(clientId, sampleId, date)

    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801").collect()
    aggregate should contain (HeavyUsersRow("20170801", clientId, sampleId, 0, 27, Some(true), Some(true)))
  }

  //case user only active on current day
  it can "include user if only active on current day" in {
    val (clientId, sampleId, date) = ("client5", "4", "20170704")
    val heavyUsers = makeTestHeavyUsers(clientId, sampleId, date)

    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801").collect()
    aggregate should contain (HeavyUsersRow("20170801", "client3", "21", 2, 2, Some(true), Some(true)))
  }

  //case normal - full history, user active on current day
  it can "properly include user active on current day" in {
    val (clientId, sampleId, date) = ("client3", "21", "20170704")
    val heavyUsers = makeTestHeavyUsers(clientId, sampleId, date)
    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801").collect()

    // had 28 active_ticks_period yesterday, +2 today, -1 from the 29 days ago
    aggregate should contain (HeavyUsersRow("20170801", clientId, sampleId, 2, 29, Some(true), Some(true)))
  }

  it can "set heavy_user to NULL if no comparison" in {
    val heavyUsers = List(
      HeavyUsersRow("20170704", "client1", "42", 2, 2, Some(false), Some(false)),
      HeavyUsersRow("20170731", "client1", "42", 0, 2, Some(false), Some(false))).toDS

    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, Map(), "20170801").collect()
    aggregate should not contain (HeavyUsersRow("20170801", "client1", "42", 1, 3, None, None))
  }

  it can "use the nearest date for comparison" in {
    val heavyUsers = List(
      HeavyUsersRow("20170704", "client1", "42", 2, 2, Some(false), Some(false)),
      HeavyUsersRow("20170731", "client1", "42", 0, 2, Some(false), Some(false))).toDS

    val aggregate = HeavyUsersView.aggregate(mainSummary, heavyUsers, Map("20170705" -> 0), "20170801").collect()
    aggregate should not contain (HeavyUsersRow("20170801", "client1", "42", 1, 3, Some(true), Some(true)))
  }

  override def afterAll() = {
    spark.stop()
  }
}
