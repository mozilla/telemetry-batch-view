package com.mozilla.telemetry.views

import java.lang.Long
import com.github.nscala_time.time.Imports._
import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._
import com.mozilla.telemetry.utils.UDFs._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed.sumLong
import org.apache.spark.sql.SQLContext
import org.rogach.scallop._
import scala.util.Try

/**
 * The input type - a projection of main_summary,
 * given by the static Columns field.
 */
case class MainSummaryRow (
  submission_date_s3: String,
  client_id: String,
  sample_id: String,
  active_ticks: Long)

object MainSummaryRow {
  val Columns = 
    col("submission_date_s3") ::
    col("client_id") ::
    col("sample_id") ::
    col("active_ticks") :: Nil
}

/**
 * The output type
 */
case class HeavyUsersRow (
  submission_date_s3: String,
  client_id: String,
  sample_id: String,
  active_ticks: Long,
  active_ticks_period: Long,
  heavy_user: Option[Boolean],
  prev_year_heavy_user: Option[Boolean])

/**
 * An interim type, used only during the job
 */
case class UserActiveTicks (
  client_id: String,
  sample_id: String,
  active_ticks: Long,
  active_ticks_period: Long)

/**
  * Heavy Users View
  *
  * This dataset will have one row for each user on each day.
  * It will indicate whether they are a heavy user, as well as
  * a total of active_ticks for that day.
  * 
  * Info on this is in bug 1388867.
  * The metabug for the data engineering effort for heavy users
  * is in bug 1388732.
  */
object HeavyUsersView {

  val DatasetPrefix = "heavy_users"
  val DefaultMainSummaryBucket = "telemetry-parquet"
  val Version = "v1"
  val NumFiles = 10 // ~3.5GB in total, per day
  val WriteMode = "overwrite"
  val SubmissionDatePartitionName = "submission_date_s3"
  val TimeWindow = 28.days

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val bucket = opt[String](
      descr = "Destination bucket for parquet data")
    val cutoffsLocation = opt[String](
      descr = "Location of the cutoffs file")
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

  /**
   * Get the cutoff for a given date.
   *
   * If the exact date is not present in the data, gets the cutoff
   * of the closest date.
   **/
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

  /**
   * Aggregate a new day of data.
   *
   * @param ds Dataset of raw `main_summary` data
   * @param existingData Dataset of the existing `heavy_users` data
   * @param cutoffs A Map of dates and their q90 for `active_ticks`
   * @param date A string date to run the job for
   **/
  def aggregate(ds: Dataset[MainSummaryRow], existingData: Dataset[HeavyUsersRow], cutoffs: Map[String, Double], date: String): Dataset[HeavyUsersRow] = {
    import ds.sparkSession.implicits._

    // Get cutoffs
    val datetime = fmt.parseDateTime(date)
    val prevYear = datetime - 1.years

    val dateCutoff = getCutoff(datetime, cutoffs)
    val prevYearCutoff = getCutoff(prevYear, cutoffs)

    // Get data - need today, yesterday, and 28 days ago
    val prevDay = fmt.print(datetime - 1.days)
    val subtractDay = fmt.print(datetime - TimeWindow)

    val prevData: Dataset[HeavyUsersRow] = existingData
      .filter(_.submission_date_s3 == prevDay)

    val subtractData: Dataset[HeavyUsersRow] = existingData
      .filter(_.submission_date_s3 == subtractDay)

    /**
     * Aggregate today's main_summary data
     */
    val aggregated: Dataset[UserActiveTicks] = ds
      .filter(r => r.submission_date_s3 == date && Option(r.active_ticks).getOrElse(0: Long) > 0)
      .groupByKey(r => (r.client_id, r.sample_id))
      .agg(sumLong[MainSummaryRow](_.active_ticks))
      .map(r => UserActiveTicks(r._1._1, r._1._2, r._2, r._2))
      .as[UserActiveTicks]

    /*
     * Add today's active_ticks to yesterday's totals
     */
    def addActiveTicksToHeavyUserRow(r: (UserActiveTicks, HeavyUsersRow)): UserActiveTicks = {
      val (current, prev) = r
      UserActiveTicks(
        Try(current.client_id).toOption.getOrElse(prev.client_id),
        Try(current.sample_id).toOption.getOrElse(prev.sample_id),
        Try(current.active_ticks).toOption.getOrElse(0: Long),
        Try(current.active_ticks_period).toOption.getOrElse(0: Long) + Try(prev.active_ticks_period).toOption.getOrElse(0: Long))
    }

    val added: Dataset[UserActiveTicks] = aggregated
      .joinWith(prevData, aggregated("client_id") === prevData("client_id"), "outer")
      .map(addActiveTicksToHeavyUserRow)
      .as[UserActiveTicks]

    /*
     * Subtract data from 29 days ago
     * Remove clients with no active_ticks over that period
     *
     * bootstrap explanation:
     * For the first 27 days, we can't actually calculate
     * whether they are a heavy user, we can only sum the
     * active_ticks. For those, we will just boostrap the
     * table, and store NULL for heavy_user and
     * prev_year_heavy_user.
     */
    def subtractHeavyUserFromPeriodActiveTicks(bootstrap: Boolean)(r: (UserActiveTicks, HeavyUsersRow)): HeavyUsersRow = {
      val (current, toSubtract) = r
      val newActiveTicksPeriod = current.active_ticks_period - Try(toSubtract.active_ticks).toOption.getOrElse(0: Long)

      val getStatus = (v: Long, cutoff: Option[Double]) => for{ e <- cutoff } yield v > e
      val heavyUser = if(bootstrap) None else getStatus(newActiveTicksPeriod, dateCutoff)
      val prevYearHeavyUser = if(bootstrap) None else getStatus(newActiveTicksPeriod, prevYearCutoff)

      HeavyUsersRow(
        date, current.client_id, current.sample_id, current.active_ticks,
        newActiveTicksPeriod, heavyUser, prevYearHeavyUser)
    }

    val bootstrap = subtractData.count() == 0

    added.joinWith(subtractData, added("client_id") === subtractData("client_id"), "left_outer")
      .map(subtractHeavyUserFromPeriodActiveTicks(bootstrap))
      .as[HeavyUsersRow]
      .filter(_.active_ticks_period > 0)
  }

  def getCutoffs(spark: SparkSession, location: String): Map[String, Double] = {
    spark.read.csv(location)
      .collect()
      .map(e => e.getAs[String](0) -> e.getAs[Double](1))
      .toMap
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .appName(s"$DatasetPrefix $Version Job")
      .getOrCreate()

    import spark.implicits._
    spark.registerUDFs

    val date = getDate(conf)

    val df = spark.read.option("mergeSchema", "true")
      .parquet(s"s3://${conf.mainSummaryBucket()}/${MainSummaryView.jobName}/${MainSummaryView.schemaVersion}")
      .select(MainSummaryRow.Columns: _*)
      .as[MainSummaryRow]

    /**
     * We have to include the schema here in the case that
     * there is no existing data. Without it, spark would
     * error out.
     */
    val heavyUsersSchema = Encoders.product[HeavyUsersRow].schema
    val existingData = spark.read.option("mergeSchema", "true")
      .schema(heavyUsersSchema)
      .parquet(s"s3://${conf.bucket()}/$DatasetPrefix/$Version")
      .as[HeavyUsersRow]

    val cutoffs = getCutoffs(spark, conf.cutoffsLocation())

    aggregate(df, existingData, cutoffs, date)
      .repartition(NumFiles, $"sample_id")
      .sortWithinPartitions($"sample_id")
      .write
      .mode(WriteMode)
      .parquet(s"s3://${conf.bucket()}/$DatasetPrefix/$Version/$SubmissionDatePartitionName=$date")

    spark.stop()
  }
}
