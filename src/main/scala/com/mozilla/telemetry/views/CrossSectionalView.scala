package com.mozilla.telemetry.views

import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.utils.S3Store
import com.mozilla.telemetry.utils.aggregation
import org.joda.time.{Days, LocalDate, Seconds}
import org.joda.time.DateTimeConstants._

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
  val session_id: Option[Seq[String]]
) {

  def cleanSessionLength(): Option[Seq[Option[Long]]] = {
    this.session_length.map(
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
    this.active_addons.map( // if active_addons isn't empty
      _.foldLeft(Seq[Option[String]]())((acc, x) => acc ++ x.values.map(_.name)).distinct
    )
  }

  def addonCount(): Option[Seq[Option[Long]]] = {
    this.active_addons.map(_.map(x => Some(x.size.toLong)))
  }

  def foreignAddons(): Option[Seq[Map[String, ActiveAddon]]] = {
    this.active_addons.map( // If array exists
      _.map( // for each ping
        _.filter(_._2.foreign_install.getOrElse(false))
      )
    )
  }

  def foreignAddonCount(): Option[Seq[Option[Long]]] = {
    this.foreignAddons.map(_.map(x => Some(x.size.toLong)))
  }

  def pluginsCount(): Option[Seq[Option[Long]]] = {
    this.active_plugins.map(_.map(x => Some(x.length.toLong)))
  }

  def parsedStartDate(): Option[Seq[Option[LocalDate]]] = {
    this.subsession_start_date.map(_.map(parseDate))
  }

  def activeHoursByDOW(dow: Int): Option[Double] = {
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
    this.parsedStartDate.map(_.flatten.filter(_.getDayOfWeek == dow).distinct.length)
  }

  def rangeOfPossibleDates(): Option[Seq[LocalDate]] = {
    this.parsedStartDate.map(
      psd => {
        val start = psd.flatten.minBy(_.toDate)
        val end = psd.flatten.maxBy(_.toDate)
        val between = Days.daysBetween(start, end).getDays

        (0 to between).map(start.plusDays(_))
      }
    )
  }

  def daysPossibleByDOW(dow: Int): Option[Long] = {
    this.rangeOfPossibleDates.map(_.filter(_.getDayOfWeek == dow).size)
  }

  def countPingReason(value: String): Long = {
    this.reason.getOrElse(Seq()).filter(_ == value).size
  }

  def previousSubsessionIdCounts(): Option[Map[String, Int]] = {
      this.previous_subsession_id.map(_.groupBy(identity).mapValues(_.size))
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
  val active_hours_total: Option[Double],
  val active_hours_0_mon: Option[Double],
  val active_hours_1_tue: Option[Double],
  val active_hours_2_wed: Option[Double],
  val active_hours_3_thu: Option[Double],
  val active_hours_4_fri: Option[Double],
  val active_hours_5_sat: Option[Double],
  val active_hours_6_sun: Option[Double],
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
      active_hours_total = base.cleanSessionLength.map(_.flatten.sum.toDouble / SECONDS_PER_HOUR),
      active_hours_0_mon = base.activeHoursByDOW(MONDAY),
      active_hours_1_tue = base.activeHoursByDOW(TUESDAY),
      active_hours_2_wed = base.activeHoursByDOW(WEDNESDAY),
      active_hours_3_thu = base.activeHoursByDOW(THURSDAY),
      active_hours_4_fri = base.activeHoursByDOW(FRIDAY),
      active_hours_5_sat = base.activeHoursByDOW(SATURDAY),
      active_hours_6_sun = base.activeHoursByDOW(SUNDAY),
      geo_mode = base.weightedMode(base.geo_country),
      geo_configs = base.geo_country.getOrElse(Seq()).distinct.length,
      architecture_mode = base.weightedMode(base.architecture).getOrElse(None),
      ffLocale_mode = base.weightedMode(base.locale).getOrElse(None),
      addon_count_foreign_avg = base.weightedMean(base.foreignAddonCount),
      addon_count_foreign_configs = base.foreignAddonCount.map(_.distinct.length),
      addon_count_foreign_mode = base.weightedMode(base.foreignAddonCount).getOrElse(None),
      addon_count_avg = base.weightedMean(base.addonCount),
      addon_count_configs = base.addonCount.map(_.distinct.length),
      addon_count_mode = base.weightedMode(base.addonCount).getOrElse(None),
      number_of_pings = base.session_length.map(_.length),
      bookmarks_avg = base.weightedMean(base.bookmarks_sum),
      bookmarks_max = base.bookmarks_sum.map(_.flatten.max),
      bookmarks_min = base.bookmarks_sum.map(_.flatten.min),
      cpu_count_mode = base.weightedMode(base.cpu_count).getOrElse(None),
      channel_configs = base.channel.map(_.distinct.length),
      channel_mode = base.weightedMode(base.channel).getOrElse(None),
      days_active = base.parsedStartDate.map(_.distinct.length),
      days_active_0_mon = base.activeDaysByDOW(MONDAY),
      days_active_1_tue = base.activeDaysByDOW(TUESDAY),
      days_active_2_wed = base.activeDaysByDOW(WEDNESDAY),
      days_active_3_thu = base.activeDaysByDOW(THURSDAY),
      days_active_4_fri = base.activeDaysByDOW(FRIDAY),
      days_active_5_sat = base.activeDaysByDOW(SATURDAY),
      days_active_6_sun = base.activeDaysByDOW(SUNDAY),
      days_possible = base.rangeOfPossibleDates.map(_.size),
      days_possible_0_mon = base.daysPossibleByDOW(MONDAY),
      days_possible_1_tue = base.daysPossibleByDOW(TUESDAY),
      days_possible_2_wed = base.daysPossibleByDOW(WEDNESDAY),
      days_possible_3_thu = base.daysPossibleByDOW(THURSDAY),
      days_possible_4_fri = base.daysPossibleByDOW(FRIDAY),
      days_possible_5_sat = base.daysPossibleByDOW(SATURDAY),
      days_possible_6_sun = base.daysPossibleByDOW(SUNDAY),
      default_pct = base.weightedMean(base.is_default_browser.map(_.map(_.map(x => if(x) 1l else 0l)))),
      locale_configs = base.locale.map(_.distinct.length),
      locale_mode = base.weightedMode(base.locale).getOrElse(None),
      version_configs = base.version.map(_.distinct.length),
      version_max = base.version.map(_.max).getOrElse(None),
      addon_names_list = base.addonNames,
      main_ping_reason_num_aborted = base.countPingReason("aborted-session"),
      main_ping_reason_num_end_of_day = base.countPingReason("daily"),
      main_ping_reason_num_env_change  = base.countPingReason("environment-change"),
      main_ping_reason_num_shutdown  = base.countPingReason("shutdown"),
      memory_avg = base.weightedMean(base.memory_mb),
      memory_configs = base.memory_mb.map(_.distinct.length),
      os_name_mode = base.weightedMode(base.os_name).getOrElse(None),
      os_version_mode = base.weightedMode(base.os_version).getOrElse(None),
      os_version_configs = base.os_version.map(_.distinct.length),
      pages_count_avg = base.weightedMean(base.pages_count),
      pages_count_min = base.pages_count.map(_.flatten.min),
      pages_count_max = base.pages_count.map(_.flatten.max),
      plugins_count_avg = base.weightedMean(base.pluginsCount),
      plugins_count_configs = base.pluginsCount.map(_.distinct.length),
      plugins_count_mode = base.weightedMode(base.pluginsCount).getOrElse(None),
      start_date_oldest = base.parsedStartDate.map(_.flatten.minBy(_.toDate).toString),
      start_date_newest = base.parsedStartDate.map(_.flatten.maxBy(_.toDate).toString),
      subsession_length_badTimer = base.session_length.map(_.filter(_ == -1).length),
      subsession_length_negative = base.session_length.map(_.filter(_ < -1).length),
      subsession_length_tooLong = base.session_length.map(_.filter(_ > SECONDS_PER_DAY * CrossSectional.MarginOfError).length),
      previous_subsession_id_repeats = base.previousSubsessionIdCounts.map(_.values.max),
      profile_creation_date = base.profile_creation_date.map(_.head),
      profile_subsession_counter_min = base.profile_subsession_counter.map(_.min),
      profile_subsession_counter_max = base.profile_subsession_counter.map(_.max),
      profile_subsession_counter_configs = base.profile_subsession_counter.map(_.distinct.length),
      search_counts_total = base.search_counts.map(_.values.foldLeft(0l)(_ + _.sum)),
      search_default_configs = base.default_search_engine.map(_.distinct.length),
      search_default_mode = base.weightedMode(base.default_search_engine).getOrElse(None),
      session_num_total = base.session_id.map(_.distinct.length),
      subsession_branches = base.previousSubsessionIdCounts.map(_.values.filter(_ > 1).size),
      date_skew_per_ping_avg = base.ssStartToSubmission.flatMap(x => aggregation.mean(x.flatten)),
      date_skew_per_ping_max = base.ssStartToSubmission.map(_.flatten.max),
      date_skew_per_ping_min = base.ssStartToSubmission.map(_.flatten.min)
    )
  }
}

object CrossSectionalView {
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
    // Setup spark contexts
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    // Parse command line options
    val opts = new Opts(args)

    // Read local parquet data, if supplied
    if(opts.localTable.isSupplied) {
      val localTable = opts.localTable()
      val data = hiveContext.read.parquet(localTable)
      data.registerTempTable("longitudinal")
    }

    // Generate and save the view
    val ds = hiveContext
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
        "profile_subsession_counter", "search_counts", "session_id"
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
      println(s"Resulting rows: ${output.count}")
      val ex = output.take(1)
      println("="*80 + "\n" + ex + "\n" + "="*80)
    }

    sc.stop()
  }
}
