package com.mozilla.telemetry.views

import java.lang.Long

import com.github.nscala_time.time.Imports._
import com.mozilla.telemetry.utils.deletePrefix
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.expressions.scalalang.typed.sumLong
import org.apache.spark.sql.functions.{col, min}
import org.rogach.scallop._
import scala.util.Try

/**
 * Heavy Users View
 *
 * This dataset will have one row for each user on each day.
 * It will indicate whether they are a heavy user, as well as
 * a total of active_ticks for that day.
 *
 * Info on this is in bug 1388867.
 * The metabug for the data engineering effort for heavy users
 * is bug 1388732.
 **/
object HeavyUsersView {

  val CutoffsDirectory = "heavy_users_cutoffs/cutoffs"
  val DatasetPrefix = "heavy_users"
  val DefaultMainSummaryBucket = "telemetry-parquet"
  val Version = "v1"
  val NumFiles = 10 // ~3.5GB in total, per day
  val WriteMode = "overwrite"
  val SubmissionDatePartitionName = "submission_date_s3"
  val TimeWindow = 28.days


  /**
   * The input type - a projection of main_summary,
   * given by the static Columns field.
   **/
  case class MainSummaryRow (
    submission_date_s3: String,
    client_id: String,
    sample_id: String,
    profile_creation_date: Long,
    active_ticks: Long)

  object MainSummaryRow {
    val Columns =
      col("submission_date_s3") ::
      col("client_id") ::
      col("sample_id") ::
      col("profile_creation_date") ::
      col("active_ticks") :: Nil
  }

  /**
   * The output type
   **/
  case class HeavyUsersRow (
    submission_date_s3: String,
    client_id: String,
    sample_id: String,
    profile_creation_date: Long,
    active_ticks: Long,
    active_ticks_period: Long,
    heavy_user: Option[Boolean],
    prev_year_heavy_user: Option[Boolean])

  /**
   * An interim type, used only during the job
   **/
  case class UserActiveTicks (
    client_id: String,
    sample_id: String,
    profile_creation_date: Long,
    active_ticks: Long,
    active_ticks_period: Long)


  class Conf(args: Array[String]) extends ScallopConf(args) {
    val bucket = opt[String](
      descr = "Destination bucket for parquet data")
    val date = opt[String](
      descr = "Submission date to process. Defaults to yesterday",
      required = false)
    val mainSummaryBucket = opt[String](
      descr = "Main summary bucket",
      required = false,
      default = Some(DefaultMainSummaryBucket))
    verify()
  }

  private val fmt = DateTimeFormat.forPattern("yyyyMMdd")

  private def getDate(conf: Conf): String = {
    conf.date.get match {
      case Some(t) => t
      case _ => fmt.print(DateTime.now.minusDays(1))
    }
  }

  /*
   * Get the cutoff for a given date.
   *
   * If the exact date is not present in the data, gets the cutoff
   * of the closest date.
   */
  def getCutoff(date: DateTime, cutoffs: Map[String, Double]): Option[Double] = {
    cutoffs.size match {
      case 0 => None
      case _ =>
        val closest = cutoffs.keys.map(k => (k, fmt.parseDateTime(k)))
          .map{ case (k, k_date) => (k, if(k_date < date) (k_date to date).millis else if (k_date > date) (date to k_date).millis else 0)}
          .minBy(_._2)._1
        Some(cutoffs(closest))
    }
  }

  /*
   * Add today's active_ticks to yesterday's totals
   */
  private def addActiveTicksToHeavyUserRow(r: (UserActiveTicks, HeavyUsersRow)): UserActiveTicks = {
    val (current, prev) = r
    UserActiveTicks(
      Try(current.client_id).toOption.getOrElse(prev.client_id),
      Try(current.sample_id).toOption.getOrElse(prev.sample_id),
      Try(current.profile_creation_date).toOption.getOrElse(prev.profile_creation_date),
      Try(current.active_ticks).toOption.getOrElse(0: Long),
      Try(current.active_ticks).toOption.getOrElse(0: Long) + Try(prev.active_ticks_period).toOption.getOrElse(0: Long))
  }

  /*
   * Subtract data from 29 days ago
   * Remove clients with no active_ticks over that period
   */
  private def subtractHeavyUserFromActiveTicks(r: (UserActiveTicks, HeavyUsersRow)): UserActiveTicks = {
    val (current, toSubtract) = r
    val newActiveTicksPeriod = current.active_ticks_period - Try(toSubtract.active_ticks).toOption.getOrElse(0: Long)
    current.copy(active_ticks_period = newActiveTicksPeriod)
  }

  /*
   * For the first 27 days, we can't actually calculate
   * whether they are a heavy user, we can only sum the
   * active_ticks. For those, we will just boostrap the
   * table, and store NULL for heavy_user and
   * prev_year_heavy_user.
   */
  def getShouldBootstrap(ds: Dataset[HeavyUsersRow], date: DateTime): Boolean = {
    val firstDay = fmt.print(date - TimeWindow + 1.days)
    val earliest = ds.toDF()
      .agg(min(ds(SubmissionDatePartitionName)))
      .head.getAs[String](0)

    Option(earliest).getOrElse(firstDay) >= firstDay
  }

  def getCutoff(ds: Dataset[UserActiveTicks], bootstrap: Boolean): Option[Double] = {
    bootstrap match {
      case true => None
      case false => Some(
        ds.toDF().stat.approxQuantile("active_ticks_period", Array(0.9), 0.0001).head)
    }
  }

  def cmpCutoff(cutoff: Option[Double], activeTicks: Long): Option[Boolean] = {
    cutoff match {
      case Some(c) => Some(activeTicks >= c)
      case None => None
    }
  }

  /*
   * Given bootstrap status and cutoffs, get a finalized HeavyUsersRow for the client's data
   */
  private def addCutoffs(date: String, bootstrap: Boolean, cutoff: Option[Double], prevYearCutoff: Option[Double])(r: UserActiveTicks): HeavyUsersRow = {
    val heavyUser = if(bootstrap) None else cmpCutoff(cutoff, r.active_ticks_period)
    val prevYearHeavyUser = if(bootstrap) None else cmpCutoff(prevYearCutoff, r.active_ticks_period)
    HeavyUsersRow(date, r.client_id, r.sample_id, r.profile_creation_date, r.active_ticks, r.active_ticks_period, heavyUser, prevYearHeavyUser)
  }

  /*
   * Aggregate a new day of data.
   *
   * @param ds Dataset of raw `main_summary` data
   * @param existingData Dataset of the existing `heavy_users` data
   * @param cutoffs A Map of dates and their q90 for `active_ticks`
   * @param date The String date to run on
   */
  def aggregate(ds: Dataset[MainSummaryRow], existingData: Dataset[HeavyUsersRow], cutoffs: Map[String, Double], date: String):
               (Dataset[HeavyUsersRow], Option[Double]) = {
    import ds.sparkSession.implicits._

    // Get cutoffs
    val datetime = fmt.parseDateTime(date)
    val prevYear = datetime - 1.years

    // Get data - need today, yesterday, and 28 days ago
    val prevDay = fmt.print(datetime - 1.days)
    val subtractDay = fmt.print(datetime - TimeWindow)

    val prevData: Dataset[HeavyUsersRow] = existingData
      .filter(_.submission_date_s3 == prevDay)
    val subtractData: Dataset[HeavyUsersRow] = existingData
      .filter(_.submission_date_s3 == subtractDay)

    // Aggregate today's main_summary data
    val aggregated: Dataset[UserActiveTicks] = ds
      .filter(r => r.submission_date_s3 == date && Option(r.active_ticks).getOrElse(0: Long) > 0)
      .groupByKey(r => (r.client_id, r.sample_id, r.profile_creation_date))
      .agg(sumLong[MainSummaryRow](_.active_ticks))
      .map{ case((cid, sid, pcd), at) => UserActiveTicks(cid, sid, pcd, at, null) }
      .as[UserActiveTicks]

    // Add today's total to yesterday's 28 days total
    val added: Dataset[UserActiveTicks] = aggregated
      .joinWith(prevData, (aggregated("client_id") === prevData("client_id")) && (aggregated("sample_id") === prevData("sample_id")), "outer")
      .map(addActiveTicksToHeavyUserRow)
      .as[UserActiveTicks]

    // Subtract 28 days ago's active_ticks
    val current: Dataset[UserActiveTicks] = added
      .joinWith(subtractData, (added("client_id") === subtractData("client_id")) && (added("sample_id") === subtractData("sample_id")), "left_outer")
      .map(subtractHeavyUserFromActiveTicks)
      .as[UserActiveTicks]
      .filter(_.active_ticks_period > 0)
      .cache()

    // This is a bootstrap date if we don't have 28 days of data
    val bootstrap = getShouldBootstrap(existingData, datetime)
    val cutoff = getCutoff(current, bootstrap)
    val prevYearCutoff = getCutoff(prevYear, cutoffs)

    val withCutoffs = current
      .map(addCutoffs(date, bootstrap, cutoff, prevYearCutoff))
      .as[HeavyUsersRow]

    (withCutoffs, cutoff)
  }

  private def getCutoffs(spark: SparkSession, bucket: String): Map[String, Double] = {
    val cutoffsPath = s"s3://$bucket/$CutoffsDirectory"
    spark.read.csv(cutoffsPath)
      .collect()
      .map(e => e.getAs[String](0) -> e.getAs[String](1).toDouble)
      .toMap
  }

  private def storeCutoffs(spark: SparkSession, cutoffs: Map[String, Double], date: String, bucket: String): Unit = {
    import spark.implicits._
    val cutoffsPath = s"s3://$bucket/$CutoffsDirectory"
    val current = cutoffs.toList.toDF().repartition(1)
    current.write.mode("overwrite").csv(cutoffsPath)
    current.write.mode("overwrite").csv(s"$cutoffsPath-$date")
  }

  private def deleteDate(bucket: String, date: String): Unit = {
    val prefix = s"$DatasetPrefix/$Version/$SubmissionDatePartitionName=$date"
    deletePrefix(bucket, prefix)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .appName(s"$DatasetPrefix $Version Job")
      .getOrCreate()

    import spark.implicits._

    val date = getDate(conf)
    deleteDate(conf.bucket(), date)

    val df = spark.read.option("mergeSchema", "true")
      .parquet(s"s3://${conf.mainSummaryBucket()}/${MainSummaryView.jobName}/${MainSummaryView.schemaVersion}")
      .select(MainSummaryRow.Columns: _*)
      .as[MainSummaryRow]

    /*
     * We have to include the schema here in the case that
     * there is no existing data. Without it, spark would
     * error out.
     */
    val heavyUsersSchema = Encoders.product[HeavyUsersRow].schema
    val existingData = spark.read.option("mergeSchema", "true")
      .schema(heavyUsersSchema)
      .parquet(s"s3://${conf.bucket()}/$DatasetPrefix/$Version")
      .as[HeavyUsersRow]

    val cutoffs = getCutoffs(spark, conf.bucket())

    val (newDS, cutoff) = aggregate(df, existingData, cutoffs, date)

    // Always overwrite previous cutoff on backfill
    cutoff match {
      case Some(c) =>
        val updatedCutoffs = cutoffs + (date -> c)
        storeCutoffs(spark, updatedCutoffs, date, conf.bucket())
      case None =>
    }

    newDS
      .repartition(NumFiles, $"sample_id")
      .sortWithinPartitions($"sample_id")
      .drop(SubmissionDatePartitionName)
      .write
      .mode(WriteMode)
      .parquet(s"s3://${conf.bucket()}/$DatasetPrefix/$Version/$SubmissionDatePartitionName=$date")

    spark.stop()
  }
}
