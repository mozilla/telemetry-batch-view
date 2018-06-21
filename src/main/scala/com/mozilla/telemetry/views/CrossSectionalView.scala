/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils.{S3Store, aggregation, getOrCreateSparkSession}
import org.joda.time.DateTimeConstants._
import org.joda.time.{Days, LocalDate}
import org.rogach.scallop._

class IfDefinedOption[A](val from: Option[A]) {
  // A simple alias for an Option's map function.  We have a lot of
  // Option[Seq[A]] in this dataset, and calling _.map(_.map(...)) obfuscates
  // the code. This alias allows us to instead write: _.ifDefined(_.map(...))
  def ifDefined[B](f: (A) => B): Option[B] = from.map(f)
}

class SafeIterable[A](val from: Iterable[A]) {
  def ifNonEmpty[B](f: (Iterable[A]) => B): Option[B] = {
    if (from.nonEmpty) Some(f(from)) else None
  }

  def optMin[B >: A](implicit cmp: Ordering[B]): Option[A] = ifNonEmpty(_.min(cmp))
  def optMax[B >: A](implicit cmp: Ordering[B]): Option[A] = ifNonEmpty(_.max(cmp))
  def optMinBy[B](f: (A) => B)(implicit cmp: Ordering[B]): Option[A] = ifNonEmpty(_.minBy(f)(cmp))
  def optMaxBy[B](f: (A) => B)(implicit cmp: Ordering[B]): Option[A] = ifNonEmpty(_.maxBy(f)(cmp))
}

object Implicits {
  implicit def opt2ifDefinedOpt[A](from: Option[A]): IfDefinedOption[A] = new IfDefinedOption(from)
  implicit def seq2SafeIterable[A](from: Iterable[A]): SafeIterable[A] = new SafeIterable(from)
}

import com.mozilla.telemetry.views.Implicits._

case class ActiveAddon (
  val blocklisted: Option[Boolean],
  val description: Option[String],
  val name: Option[String],
  val user_disabled: Option[Boolean],
  val app_disabled: Option[Boolean],
  val version: Option[String],
  val scope: Option[Long],
  val `type`: Option[String],
  val foreign_install: Option[Boolean],
  val has_binary_components: Option[Boolean],
  val install_day: Option[Long],
  val update_day: Option[Long],
  val signed_state: Option[Long],
  val is_system: Option[Boolean]
)

case class ActivePlugin (
  val name: Option[String],
  val version: Option[String],
  val description: Option[String],
  val blocklisted: Option[Boolean],
  val disabled: Option[Boolean],
  val clicktoplay: Option[Boolean],
  val mime_types: Option[Seq[String]],
  val update_day: Option[Long]
)

object Longitudinal {
}

case class Longitudinal (
  val client_id: String,
  val normalized_channel: String,
  val submission_date: Option[Seq[String]],
  val geo_country: Option[Seq[String]],
  val session_length: Option[Seq[Long]],
  val is_default_browser: Option[Seq[Option[Boolean]]],
  val default_search_engine: Option[Seq[Option[String]]],
  val locale: Option[Seq[Option[String]]],
  val architecture: Option[Seq[Option[String]]],
  val active_addons: Option[Seq[Map[String, ActiveAddon]]],
  val bookmarks_sum: Option[Seq[Option[Long]]],
  val cpu_count: Option[Seq[Option[Long]]],
  val channel: Option[Seq[Option[String]]],
  val subsession_start_date: Option[Seq[String]],
  val version: Option[Seq[Option[String]]],
  val reason: Option[Seq[String]],
  val memory_mb: Option[Seq[Option[Long]]],
  val os_name: Option[Seq[Option[String]]],
  val os_version: Option[Seq[Option[String]]],
  val pages_count: Option[Seq[Option[Long]]],
  val active_plugins: Option[Seq[Seq[ActivePlugin]]],
  val previous_subsession_id: Option[Seq[String]],
  val profile_creation_date: Option[Seq[String]],
  val profile_subsession_counter: Option[Seq[Long]],
  val search_counts: Option[scala.collection.Map[String, Seq[Long]]],
  val session_id: Option[Seq[String]],
  val application_name: Option[Seq[Option[String]]]
) {

  def cleanSessionLength(): Option[Seq[Option[Long]]] = {
    this.session_length.ifDefined(
      _.map(x => if ((x <= -1) || (x > SECONDS_PER_DAY)) None else Some(x))
    )
  }

  private def collate[A, B](seqPair: (Seq[A], Seq[B])): Option[Seq[(A, B)]] = {
    val (first, second) = seqPair
    if (first.size == second.size) Some(first zip second) else None
  }

  def weightedMean(values: Option[Seq[Option[Long]]]): Option[Double] = {
    val cleanPairs = for {
      v <- values.toSeq
      csl <- this.cleanSessionLength.toSeq
      pairs <- collate(v, csl).toSeq
      pair <- pairs
      value <- pair._1
      weight <- pair._2
    } yield (value, weight)

    aggregation.weightedMean(cleanPairs)
  }

  def weightedMode[A](values: Option[Seq[A]]): Option[A] = {
    val cleanPairs = for {
      v <- values.toSeq
      csl <- this.cleanSessionLength.toSeq
      pairs <- collate(v, csl).toSeq
      pair <- pairs
      weight <- pair._2.toSeq
    } yield (pair._1, weight)

    aggregation.weightedMode(cleanPairs)
  }

  def addonNames(): Option[Seq[Option[String]]] = {
    this.active_addons.ifDefined(
      _.foldLeft(Seq[Option[String]]())((acc, x) => acc ++ x.values.map(_.name)).distinct
    )
  }

  def addonCount(): Option[Seq[Option[Long]]] = {
    this.active_addons.ifDefined(_.map(x => Some(x.size.toLong)))
  }

  def foreignAddons(): Option[Seq[Map[String, ActiveAddon]]] = {
    this.active_addons.ifDefined(
      _.map( // for each ping
        _.filter(_._2.foreign_install.getOrElse(false))
      )
    )
  }

  def foreignAddonCount(): Option[Seq[Option[Long]]] = {
    this.foreignAddons.ifDefined(_.map(x => Some(x.size.toLong)))
  }

  def pluginsCount(): Option[Seq[Option[Long]]] = {
    this.active_plugins.ifDefined(_.map(x => Some(x.length.toLong)))
  }

  def parsedStartDate(): Option[Seq[Option[LocalDate]]] = {
    this.subsession_start_date.ifDefined(_.map(parseDate))
  }

  def sessionHoursByDOW(dow: Int): Option[Double] = {
    val optPairs = for {
      csl <- this.cleanSessionLength
      psd <- this.parsedStartDate
      pairs <- collate(csl, psd)
    } yield pairs

    if (optPairs.isDefined) {
      val hours = for {
        pairs <- optPairs.toSeq
        pair <- pairs
        date <- pair._2
        if date.getDayOfWeek == dow
        sessionLength <- pair._1
      } yield {sessionLength.toDouble / SECONDS_PER_HOUR}

      Some(hours.sum)
    } else {
      None
    }
  }

  def activeDaysByDOW(dow: Int): Option[Long] = {
    this.parsedStartDate.ifDefined(_.flatten.filter(_.getDayOfWeek == dow).distinct.length)
  }

  def rangeOfPossibleDates(): Option[Seq[LocalDate]] = {
    for {
      psd <- this.parsedStartDate
      start <- psd.flatten.optMinBy(_.toDate)
      end <- psd.flatten.optMaxBy(_.toDate)
      between = Days.daysBetween(start, end).getDays
    } yield (0 to between).map(start.plusDays(_))
  }

  def daysPossibleByDOW(dow: Int): Option[Long] = {
    this.rangeOfPossibleDates.ifDefined(_.filter(_.getDayOfWeek == dow).size)
  }

  def countPingReason(value: String): Long = {
    this.reason.getOrElse(Seq()).filter(_ == value).size
  }

  def previousSubsessionIdCounts(): Option[Map[String, Long]] = {
      this.previous_subsession_id.ifDefined(_.groupBy(identity).mapValues(_.size))
  }

  def ssStartToSubmission(): Option[Seq[Option[Long]]] = {
    for {
      ssd <- this.subsession_start_date
      sd <- this.submission_date
      pairs <- collate(ssd, sd)
    } yield {
      pairs.map(pair => getDateDiff(pair._1, pair._2))
    }
  }

  private def getDateDiff(begin: String, end: String): Option[Long] = {
    for {
      startDate <- parseDate(begin)
      endDate <- parseDate(end)
    } yield {
      Days.daysBetween(startDate, endDate).getDays.toLong
    }
  }

  private def parseDate(sdate: String): Option[LocalDate] = {
    // Date format is "YYYY-MM-DDT00:00:00.0+00:00"
    val cleanDate = sdate.split("T")(0)
    try {
      Some(new LocalDate(cleanDate))
    } catch {
      case e: java.lang.IllegalArgumentException => None
    }
  }
}

object CrossSectional {
  private val MarginOfError = 1.05
}

case class CrossSectional (
  val client_id: String,
  val normalized_channel: String,
  val session_hours_total: Option[Double],
  val session_hours_0_mon: Option[Double],
  val session_hours_1_tue: Option[Double],
  val session_hours_2_wed: Option[Double],
  val session_hours_3_thu: Option[Double],
  val session_hours_4_fri: Option[Double],
  val session_hours_5_sat: Option[Double],
  val session_hours_6_sun: Option[Double],
  val geo_mode: Option[String],
  val geo_configs: Long,
  val architecture_mode: Option[String],
  val ffLocale_mode: Option[String],
  val addon_count_foreign_avg: Option[Double],
  val addon_count_foreign_configs: Option[Long],
  val addon_count_foreign_mode: Option[Long],
  val addon_count_avg: Option[Double],
  val addon_count_configs: Option[Long],
  val addon_count_mode: Option[Long],
  val number_of_pings: Option[Long],
  val bookmarks_avg: Option[Double],
  val bookmarks_max: Option[Long],
  val bookmarks_min: Option[Long],
  val cpu_count_mode: Option[Long],
  val channel_configs: Option[Long],
  val channel_mode: Option[String],
  val days_active: Option[Long],
  val days_active_0_mon: Option[Long],
  val days_active_1_tue: Option[Long],
  val days_active_2_wed: Option[Long],
  val days_active_3_thu: Option[Long],
  val days_active_4_fri: Option[Long],
  val days_active_5_sat: Option[Long],
  val days_active_6_sun: Option[Long],
  val days_possible: Option[Long],
  val days_possible_0_mon: Option[Long],
  val days_possible_1_tue: Option[Long],
  val days_possible_2_wed: Option[Long],
  val days_possible_3_thu: Option[Long],
  val days_possible_4_fri: Option[Long],
  val days_possible_5_sat: Option[Long],
  val days_possible_6_sun: Option[Long],
  val default_pct: Option[Double],
  val locale_configs: Option[Long],
  val locale_mode: Option[String],
  val version_configs: Option[Long],
  val version_max: Option[String],
  val application_name_mode: Option[String],
  val addon_names_list: Option[Seq[Option[String]]],
  val main_ping_reason_num_aborted: Long,
  val main_ping_reason_num_end_of_day: Long,
  val main_ping_reason_num_env_change: Long,
  val main_ping_reason_num_shutdown: Long,
  val memory_avg: Option[Double],
  val memory_configs: Option[Long],
  val os_name_mode: Option[String],
  val os_version_mode: Option[String],
  val os_version_configs: Option[Long],
  val pages_count_avg: Option[Double],
  val pages_count_min: Option[Long],
  val pages_count_max: Option[Long],
  val plugins_count_avg: Option[Double],
  val plugins_count_configs: Option[Long],
  val plugins_count_mode: Option[Long],
  val start_date_oldest: Option[String],
  val start_date_newest: Option[String],
  val subsession_length_badTimer: Option[Long],
  val subsession_length_negative: Option[Long],
  val subsession_length_tooLong: Option[Long],
  val previous_subsession_id_repeats: Option[Long],
  val profile_creation_date: Option[String],
  val profile_subsession_counter_min: Option[Long],
  val profile_subsession_counter_max: Option[Long],
  val profile_subsession_counter_configs: Option[Long],
  val search_counts_total: Option[Long],
  val search_default_configs: Option[Long],
  val search_default_mode: Option[String],
  val session_num_total: Option[Long],
  val subsession_branches: Option[Long],
  val date_skew_per_ping_avg: Option[Double],
  val date_skew_per_ping_max: Option[Long],
  val date_skew_per_ping_min: Option[Long]
) {
  def this(base: Longitudinal) = {
    this(
      client_id = base.client_id,
      normalized_channel = base.normalized_channel,
      session_hours_total = base.cleanSessionLength.ifDefined(_.flatten.sum.toDouble / SECONDS_PER_HOUR),
      session_hours_0_mon = base sessionHoursByDOW(MONDAY),
      session_hours_1_tue = base sessionHoursByDOW(TUESDAY),
      session_hours_2_wed = base sessionHoursByDOW(WEDNESDAY),
      session_hours_3_thu = base sessionHoursByDOW(THURSDAY),
      session_hours_4_fri = base sessionHoursByDOW(FRIDAY),
      session_hours_5_sat = base sessionHoursByDOW(SATURDAY),
      session_hours_6_sun = base sessionHoursByDOW(SUNDAY),
      geo_mode = base.weightedMode(base.geo_country),
      geo_configs = base.geo_country.getOrElse(Seq()).distinct.length,
      architecture_mode = base.weightedMode(base.architecture).flatten,
      ffLocale_mode = base.weightedMode(base.locale).flatten,
      addon_count_foreign_avg = base.weightedMean(base.foreignAddonCount),
      addon_count_foreign_configs = base.foreignAddonCount.ifDefined(_.distinct.length),
      addon_count_foreign_mode = base.weightedMode(base.foreignAddonCount).flatten,
      addon_count_avg = base.weightedMean(base.addonCount),
      addon_count_configs = base.addonCount.ifDefined(_.distinct.length),
      addon_count_mode = base.weightedMode(base.addonCount).flatten,
      number_of_pings = base.session_length.ifDefined(_.length),
      bookmarks_avg = base.weightedMean(base.bookmarks_sum),
      bookmarks_max = base.bookmarks_sum.ifDefined(_.flatten.optMax).flatten,
      bookmarks_min = base.bookmarks_sum.ifDefined(_.flatten.optMin).flatten,
      cpu_count_mode = base.weightedMode(base.cpu_count).flatten,
      channel_configs = base.channel.ifDefined(_.distinct.length),
      channel_mode = base.weightedMode(base.channel).flatten,
      days_active = base.parsedStartDate.ifDefined(_.distinct.length),
      days_active_0_mon = base.activeDaysByDOW(MONDAY),
      days_active_1_tue = base.activeDaysByDOW(TUESDAY),
      days_active_2_wed = base.activeDaysByDOW(WEDNESDAY),
      days_active_3_thu = base.activeDaysByDOW(THURSDAY),
      days_active_4_fri = base.activeDaysByDOW(FRIDAY),
      days_active_5_sat = base.activeDaysByDOW(SATURDAY),
      days_active_6_sun = base.activeDaysByDOW(SUNDAY),
      days_possible = base.rangeOfPossibleDates.ifDefined(_.size),
      days_possible_0_mon = base.daysPossibleByDOW(MONDAY),
      days_possible_1_tue = base.daysPossibleByDOW(TUESDAY),
      days_possible_2_wed = base.daysPossibleByDOW(WEDNESDAY),
      days_possible_3_thu = base.daysPossibleByDOW(THURSDAY),
      days_possible_4_fri = base.daysPossibleByDOW(FRIDAY),
      days_possible_5_sat = base.daysPossibleByDOW(SATURDAY),
      days_possible_6_sun = base.daysPossibleByDOW(SUNDAY),
      default_pct = base.weightedMean(base.is_default_browser.ifDefined(_.map(_.ifDefined(x => if(x) 1L else 0L)))),
      locale_configs = base.locale.ifDefined(_.distinct.length),
      locale_mode = base.weightedMode(base.locale).flatten,
      version_configs = base.version.ifDefined(_.distinct.length),
      version_max = base.version.ifDefined(_.optMax.flatten).flatten,
      application_name_mode = base.weightedMode(base.application_name).flatten,
      addon_names_list = base.addonNames,
      main_ping_reason_num_aborted = base.countPingReason("aborted-session"),
      main_ping_reason_num_end_of_day = base.countPingReason("daily"),
      main_ping_reason_num_env_change  = base.countPingReason("environment-change"),
      main_ping_reason_num_shutdown  = base.countPingReason("shutdown"),
      memory_avg = base.weightedMean(base.memory_mb),
      memory_configs = base.memory_mb.ifDefined(_.distinct.length),
      os_name_mode = base.weightedMode(base.os_name).flatten,
      os_version_mode = base.weightedMode(base.os_version).flatten,
      os_version_configs = base.os_version.ifDefined(_.distinct.length),
      pages_count_avg = base.weightedMean(base.pages_count),
      pages_count_min = base.pages_count.ifDefined(_.flatten.optMin).flatten,
      pages_count_max = base.pages_count.ifDefined(_.flatten.optMax).flatten,
      plugins_count_avg = base.weightedMean(base.pluginsCount),
      plugins_count_configs = base.pluginsCount.ifDefined(_.distinct.length),
      plugins_count_mode = base.weightedMode(base.pluginsCount).flatten,
      start_date_oldest = base.parsedStartDate.ifDefined(_.flatten.optMinBy(_.toDate).ifDefined(_.toString)).flatten,
      start_date_newest = base.parsedStartDate.ifDefined(_.flatten.optMaxBy(_.toDate).ifDefined(_.toString)).flatten,
      subsession_length_badTimer = base.session_length.ifDefined(_.filter(_ == -1).length),
      subsession_length_negative = base.session_length.ifDefined(_.filter(_ < -1).length),
      subsession_length_tooLong = base.session_length.ifDefined(_.filter(_ > SECONDS_PER_DAY * CrossSectional.MarginOfError).length),
      previous_subsession_id_repeats = base.previousSubsessionIdCounts.ifDefined(_.values.optMax).flatten,
      profile_creation_date = base.profile_creation_date.ifDefined(_.head),
      profile_subsession_counter_min = base.profile_subsession_counter.ifDefined(_.optMin).flatten,
      profile_subsession_counter_max = base.profile_subsession_counter.ifDefined(_.optMax).flatten,
      profile_subsession_counter_configs = base.profile_subsession_counter.ifDefined(_.distinct.length),
      search_counts_total = base.search_counts.ifDefined(_.values.foldLeft(0L)(_ + _.sum)),
      search_default_configs = base.default_search_engine.ifDefined(_.distinct.length),
      search_default_mode = base.weightedMode(base.default_search_engine).flatten,
      session_num_total = base.session_id.ifDefined(_.distinct.length),
      subsession_branches = base.previousSubsessionIdCounts.ifDefined(_.values.filter(_ > 1).size),
      date_skew_per_ping_avg = base.ssStartToSubmission.flatMap(x => aggregation.mean(x.flatten)),
      date_skew_per_ping_max = base.ssStartToSubmission.ifDefined(_.flatten.optMax).flatten,
      date_skew_per_ping_min = base.ssStartToSubmission.ifDefined(_.flatten.optMin).flatten
    )
  }
}

object CrossSectionalView {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val outputBucket = opt[String](
      "outputBucket",
      descr = "Bucket in which to save data",
      required = false,
      default=Some("telemetry-test-bucket"))
    val localTable = opt[String](
      "localTable",
      descr = "Optional path to a local Parquet file with longitudinal data",
      required = false)
    val outName = opt[String](
      "outName",
      descr = "Name for the output of this run",
      required = true)
    val dryRun = opt[Boolean](
      "dryRun",
      descr = "Calculate the dataset, but do not write to S3",
      required = false,
      default=Some(false))
    verify()
  }

  def main(args: Array[String]): Unit = {
    // Setup spark
    val spark = getOrCreateSparkSession(this.getClass.getName, enableHiveSupport = true)

    import spark.implicits._

    // Parse command line options
    val opts = new Opts(args)

    // Read local parquet data, if supplied
    if(opts.localTable.isSupplied) {
      val localTable = opts.localTable()
      val data = spark.read.parquet(localTable)
      data.createOrReplaceTempView("longitudinal")
    }

    // Generate and save the view
    val ds = spark
      .sql("SELECT * FROM longitudinal")
      .selectExpr(
        "client_id", "normalized_channel", "submission_date", "geo_country",
        "session_length", "settings.locale", "settings.is_default_browser",
        "settings.default_search_engine", "build.architecture", "active_addons",
        "places_bookmarks_count.sum as bookmarks_sum", "system_cpu.count as cpu_count",
        "settings.update.channel", "subsession_start_date", "build.version",
        "reason", "system.memory_mb", "system_os.name as os_name",
        "system_os.version as os_version", "places_pages_count.sum as pages_count",
        "active_plugins", "previous_subsession_id", "profile_creation_date",
        "profile_subsession_counter", "search_counts", "session_id",
        "build.application_name"
      )
      .as[Longitudinal]
    val output = ds.map(new CrossSectional(_))

    // Save to S3
    if (!opts.dryRun()) {
      val prefix = s"cross_sectional/${opts.outName()}"
      val outputBucket = opts.outputBucket()
      val path = s"s3://${outputBucket}/${prefix}"


      require(S3Store.isPrefixEmpty(outputBucket, prefix),
        s"${path} already exists!")

      output.toDF().write.parquet(path)
    } else {
      // Count to ensure the entire dataset is created in a dry-run.
      logger.info(s"Resulting rows: ${output.count}")
      val ex = output.take(1)
      logger.info("="*80 + "\n" + ex + "\n" + "="*80)
    }

    spark.stop()
  }
}
