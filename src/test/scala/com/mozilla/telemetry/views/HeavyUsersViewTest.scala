package com.mozilla.telemetry

import com.github.nscala_time.time.Imports._
import com.mozilla.telemetry.views.HeavyUsersView
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.Assertions._

import scala.io.Source

class HeavyUsersViewTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val spark = getOrCreateSparkSession(HeavyUsersView.DatasetPrefix)
  private val fmt = DateTimeFormat.forPattern("yyyyMMdd")

  import spark.implicits._

  val mainSummary = List(
    HeavyUsersView.MainSummaryRow("20170801", "client1", "42", 1, 1),
    HeavyUsersView.MainSummaryRow("20170801", "client2", "84", 2, 1),
    HeavyUsersView.MainSummaryRow("20170801", "client3", "21", 3, 2)).toDS

  val cutoffs = Map(
    "20160801" -> 0.9
  )

  def makeTestHeavyUsers(clientId: String, sampleId: Int, pcd: Long, date: String): Dataset[HeavyUsersView.HeavyUsersRow] = {
    val firstDay = fmt.parseDateTime(date)
    val hus = (i: Int) => if(i == 27) Some(true) else None
    (0 until 28)
      .map(d => (d, fmt.print(firstDay.plusDays(d))))
      .map{ case (i, date) => HeavyUsersView.HeavyUsersRow(date, clientId, sampleId, pcd, 1, i + 1, hus(i), hus(i)) }
      .toDS
  }

  def dummyRow(d: String) = HeavyUsersView.HeavyUsersRow(d, "client1", 42, 1, 2, 2, Some(false), Some(false))

  // case regular history
  "Heavy Users View" can "run with enough data" in {
    val (clientId, sampleId, pcd, date) = ("client1", 42, 1, "20170704")
    val heavyUsers = makeTestHeavyUsers(clientId, sampleId, pcd, date)

    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801")
    aggregate.collect() should contain (HeavyUsersView.HeavyUsersRow("20170801", clientId, sampleId, pcd, 1, 28, HeavyUsersView.cmpCutoff(cutoff, 28), Some(true)))
  }

  it can "remove users who aren't active in past 28 days" in {
    val heavyUsers = List(
      HeavyUsersView.HeavyUsersRow("20170704", "missingclient", 42, 10, 2, 2, Some(false), Some(false)),
      HeavyUsersView.HeavyUsersRow("20170731", "missingclient", 42, 10, 0, 2, Some(false), Some(false))).toDS

    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801")
    aggregate.collect().map(_.client_id) should not contain ("missingclient")
  }

  //case no current data
  it can "bootstrap initial history" in {
    val heavyUsers = (Nil: List[HeavyUsersView.HeavyUsersRow]).toDS

    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801")
    aggregate.collect().toSet should be (
      mainSummary.collect().map{ r =>
        HeavyUsersView.HeavyUsersRow(
          r.submission_date_s3, r.client_id, r.sample_id.toInt, r.profile_creation_date,
          r.active_ticks, r.active_ticks, None, None)
      }.toSet
    )
  }

  //case some current data (not full history)
  it can "boostrap on partial history" in {
    val heavyUsers = List(HeavyUsersView.HeavyUsersRow("20170731", "client1", 42, 1, 1, 2, Some(false), Some(false))).toDS

    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801")
    aggregate.collect() should contain (HeavyUsersView.HeavyUsersRow("20170801", "client3", 21, 3, 2, 2, None, None))
  }

  //case user not active on current day - still contain
  it can "include user even if not active on current day" in {
    val (clientId, sampleId, pcd, date) = ("client5", 4, 5, "20170704")
    val heavyUsers = makeTestHeavyUsers(clientId, sampleId, pcd, date)

    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801")
    aggregate.collect() should contain (HeavyUsersView.HeavyUsersRow("20170801", clientId, sampleId, pcd, 0, 27, HeavyUsersView.cmpCutoff(cutoff, 27), Some(true)))
  }

  //case user only active on current day
  it can "include user if only active on current day" in {
    val (clientId, sampleId, pcd, date) = ("client5", 4, 5, "20170704")
    val heavyUsers = makeTestHeavyUsers(clientId, sampleId, pcd, date)

    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801")
    aggregate.collect() should contain (HeavyUsersView.HeavyUsersRow("20170801", "client3", 21, 3, 2, 2, HeavyUsersView.cmpCutoff(cutoff, 2), Some(true)))
  }

  //case normal - full history, user active on current day
  it can "properly include user active on current day" in {
    val (clientId, sampleId, pcd, date) = ("client3", 21, 3, "20170704")
    val heavyUsers = makeTestHeavyUsers(clientId, sampleId, pcd, date)
    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, cutoffs, "20170801")

    // had 28 active_ticks_period yesterday, +2 today, -1 from the 29 days ago
    aggregate.collect() should contain (HeavyUsersView.HeavyUsersRow("20170801", clientId, sampleId, pcd, 2, 29, HeavyUsersView.cmpCutoff(cutoff, 29), Some(true)))
  }

  it can "set heavy_user to NULL if no comparison" in {
    val heavyUsers = List(
      HeavyUsersView.HeavyUsersRow("20170704", "client1", 42, 1, 2, 2, Some(false), Some(false)),
      HeavyUsersView.HeavyUsersRow("20170731", "client1", 42, 1, 0, 2, Some(false), Some(false))).toDS

    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, Map(), "20170801")
    aggregate.collect() should not contain (HeavyUsersView.HeavyUsersRow("20170801", "client1", 42, 1, 1, 3, None, None))
  }

  it can "use the nearest date for comparison" in {
    val heavyUsers = List(
      HeavyUsersView.HeavyUsersRow("20170704", "client1", 42, 1, 2, 2, Some(false), Some(false)),
      HeavyUsersView.HeavyUsersRow("20170731", "client1", 42, 1, 0, 2, Some(false), Some(false))).toDS

    val (aggregate, cutoff) = HeavyUsersView.aggregate(mainSummary, heavyUsers, Map("20170705" -> 0), "20170801")
    aggregate.collect() should not contain (HeavyUsersView.HeavyUsersRow("20170801", "client1", 42, 1, 1, 3, HeavyUsersView.cmpCutoff(cutoff, 3), Some(true)))
  }

  it can "correctly calculate a cutoff" in {
    val ds = (1 until 100).map(HeavyUsersView.UserActiveTicks("someclient", 42, 1, 1, _)).toDS
    HeavyUsersView.getCutoff(ds, false).get should be (90)
  }

  it can "sense that previous data exists" in {
    val ds = List(dummyRow("20170901")).toDS

    // will fail if this throws IllegalStateException
    HeavyUsersView.senseExistingData(ds, "20170902", false)
  }

  it can "sense that previous data is missing" in {
    val ds = List(dummyRow("20170901")).toDS

    intercept[java.lang.IllegalStateException] {
      HeavyUsersView.senseExistingData(ds, "20170903", false)
    }
  }

  it can "sense that previous data exists in the middle" in {
    val ds = List(dummyRow("20170901"),
                  dummyRow("20170902"),
                  dummyRow("20170904")).toDS

    HeavyUsersView.senseExistingData(ds, "20170903", false)
  }

  it can "sense that previous data is missing in the middle" in {
    val ds = List(dummyRow("20170901"),
                  dummyRow("20170903"),
                  dummyRow("20170904")).toDS

    intercept[java.lang.IllegalStateException] {
      HeavyUsersView.senseExistingData(ds, "20170903", false)
    }
  }

  it can "ignore missing data on first run" in {
    HeavyUsersView.senseExistingData(spark.emptyDataset[HeavyUsersView.HeavyUsersRow], "20170903", true)
  }

  override def afterAll() = {
    spark.stop()
  }
}
