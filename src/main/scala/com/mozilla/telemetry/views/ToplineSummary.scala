package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, format}
import org.joda.time.format.DateTimeFormat
import org.rogach.scallop.ScallopConf

import scala.util.Try
import scala.util.matching.Regex

object ToplineSummary {
  private val main_summary_url: String = "s3://telemetry-parquet/main_summary/v3"

  private val logger: Logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)
  private val sparkConf: SparkConf = new SparkConf().setAppName("FirefoxSummary")
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
  implicit val sc = SparkContext.getOrCreate(sparkConf)
  private val spark = SparkSession.builder.config(sparkConf).getOrCreate()
  import spark.implicits._


  private val SecondsInHour: Int = 60 * 60
  private val SecondsInDay: Int = SecondsInHour * 24

  /* Returns the subsession_length from seconds to hours */
  private val convertHours: UserDefinedFunction = udf {
    (uptime: Double) =>
      uptime match {
        case ut if ut >= 0 && ut < 180 * SecondsInDay => ut / SecondsInHour
        case _ => 0.0
      }
  }

  /* Returns the profile creation date in seconds since unix epoch */
  private val convertProfileCreation: UserDefinedFunction = udf {
    (timestamp: Long) =>
      timestamp match {
        case ts if ts > 0 => ts * SecondsInDay
        case _ => 0
      }
  }

  /**
    * Normalizes a string to a set of labels by matching them against a pattern.
    *
    * This assumes that we will always match with some pattern. The default pattern
    * by convention will be the last element corresponding to ('.*', 'Other'). As a
    * result, we need to be careful with what we feed this function. Null values should
    * be removed before passing data into this function.
    *
    * @param patterns a list of compiled regex patterns
    * @param labels label to attach to a given string once it has been identified
    * @param str input string
    * @return the label of the string
    */
  private def normalize(patterns: List[Regex], labels: List[String], str: String): String = {
    (patterns zip labels)
      .map { case (pattern, label) => Try(str match { case pattern() => label }).toOption }
      .filter(_.isDefined)
      .head
      .getOrElse(labels.last)
  }

  private val normalizeChannel: UserDefinedFunction = udf {
    (channel: String) => normalize(
      List("release", "beta", "nightly|nightly-cck-.*", "aurora", ".*")
        .map(pattern => pattern.r),
      List("release", "beta", "nightly", "aurora", "Other"),
      channel)
  }

  private val normalizeOS: UserDefinedFunction = udf {
    (os: String) => normalize(
      // NOTE: (?:foo) is a non-capturing group, see also the difference between using
      // `case pattern()` vs `case pattern(_)` in the match statement in normalize
      List("Windows.*|WINNT", "Darwin", ".*(?:Linux|BSD|SunOS).*", ".*")
        .map(pattern => pattern.r),
      List("Windows", "Mac", "Linux", "Other"),
      os)
  }

  // Also used to create the pivot table
  private val SearchLabels = List("google", "bing", "yahoo", "other")
  private val normalizeSearch = udf {
    (searches: String) => normalize(
      List("[Gg]oogle", "[Bb]ing", "[Yy]ahoo", ".*")
        .map(pattern => pattern.r),
      SearchLabels,
      searches)
  }

  private val countryNames = Set(
    "AD","AE","AF","AG","AI","AL","AM","AO","AQ","AR","AS","AT","AU",
    "AW","AX","AZ","BA","BB","BD","BE","BF","BG","BH","BI","BJ","BL","BM","BN",
    "BO","BQ","BR","BS","BT","BV","BW","BY","BZ","CA","CC","CD","CF","CG","CH",
    "CI","CK","CL","CM","CN","CO","CR","CU","CV","CW","CX","CY","CZ","DE","DJ",
    "DK","DM","DO","DZ","EC","EE","EG","EH","ER","ES","ET","FI","FJ","FK","FM",
    "FO","FR","GA","GB","GD","GE","GF","GG","GH","GI","GL","GM","GN","GP","GQ",
    "GR","GS","GT","GU","GW","GY","HK","HM","HN","HR","HT","HU","ID","IE","IL",
    "IM","IN","IO","IQ","IR","IS","IT","JE","JM","JO","JP","KE","KG","KH","KI",
    "KM","KN","KP","KR","KW","KY","KZ","LA","LB","LC","LI","LK","LR","LS","LT",
    "LU","LV","LY","MA","MC","MD","ME","MF","MG","MH","MK","ML","MM","MN","MO",
    "MP","MQ","MR","MS","MT","MU","MV","MW","MX","MY","MZ","NA","NC","NE","NF",
    "NG","NI","NL","NO","NP","NR","NU","NZ","OM","PA","PE","PF","PG","PH","PK",
    "PL","PM","PN","PR","PS","PT","PW","PY","QA","RE","RO","RS","RU","RW","SA",
    "SB","SC","SD","SE","SG","SH","SI","SJ","SK","SL","SM","SN","SO","SR","SS",
    "ST","SV","SX","SY","SZ","TC","TD","TF","TG","TH","TJ","TK","TL","TM","TN",
    "TO","TR","TT","TV","TW","TZ","UA","UG","UM","US","UY","UZ","VA","VC","VE",
    "VG","VI","VN","VU","WF","WS","YE","YT","ZA","ZM","ZW")

  private val normalizeCountry: UserDefinedFunction = udf {
    (country: String) => if (countryNames contains country) { country } else { "Other" }
  }

  /**
    * Create the dataset required to generate the report.
    *
    * @param mainSummary DataFrame of the main summary
    * @param startDate start date in yyyymmdd format
    * @param endDate end date in yyyymmdd format
    * @return Report DataFrame with normalized fields (aside from search_counts).
    */
  private def createReportDataset(mainSummary: DataFrame, startDate: String, endDate: String): DataFrame = {
    mainSummary
      .filter($"submission_date_s3" >= startDate)
      .filter($"submission_date_s3" <= endDate)
      .filter($"app_name" === "Firefox")
      // preprocess columns before feeding into normalize functions
      .na.fill("", Seq("country", "channel", "os"))
      .na.fill(0, Seq("profile_creation_date", "subsession_length"))
      .select(
        $"client_id",
        $"submission_date",
        $"is_default_browser",
        $"search_counts",
        normalizeCountry($"country").alias("country"),
        convertProfileCreation($"profile_creation_date").alias("profile_creation_date"),
        normalizeChannel($"channel").alias("channel"),
        normalizeOS($"os").alias("os"),
        convertHours($"subsession_length".cast(DoubleType)).alias("hours")
      )
  }

  /**
    * Create the dataset required for counting the number of crashes
    *
    * This is currently implemented by searching through the main pings. This could be done using
    * the crash summary dataset.
    *
    * @param startDate start date in yyyymmdd
    * @param endDate end date in yyyymmdd
    * @return Dataset where every row represents a single crash
    */
  private def createCrashDataset(startDate: String, endDate: String): DataFrame = {
    val CrashSchema = StructType(List(
      StructField("country", StringType, nullable = true),
      StructField("channel", StringType, nullable = true),
      StructField("os", StringType, nullable = true)))

    val messages: RDD[Message] = Dataset("telemetry")
      .where("docType") { case "crash" => true }
      .where("appName") { case "Firefox" => true }
      .where("submissionDate") { case date => startDate <= date && date <= endDate }
      .records()

    val crashRDD = messages.map(messageToRow)
    val crashData = spark.createDataFrame(crashRDD, CrashSchema)

    crashData.select(
      normalizeCountry($"country").alias("country"),
      normalizeChannel($"channel").alias("channel"),
      normalizeOS($"os").alias("os"))
  }

  private def messageToRow(message: Message): Row = {
    val fields = message.fieldsAsMap

    Row(
      fields.getOrElse("geoCountry", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("appUpdateChannel", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("os", None) match {
        case x: String => x
        case _ => ""
      }
    )
  }

  /**
    * Create the aggregates for search counts.
    *
    * This is done by exploding the nested search_counts structure and applying a pivot over the engines.
    *
    * @param reportData dataframe containing the necessary columns
    * @return dataframe with aggregated search counts with a column per search engine
    */
  private def searchAggregates(reportData: DataFrame): DataFrame = {
    val searchSchema = MainSummaryView.buildSearchSchema

    val searchData = reportData
      .where($"search_counts".isNotNull)
      .withColumn("search_counts", explode($"search_counts"))
      .select(
        $"country",
        $"channel",
        $"os",
        normalizeSearch($"search_counts.engine").alias("engine"),
        $"search_counts.count".alias("count"))
      .na.fill(0, Seq("count"))

    searchData
      .groupBy("country", "channel", "os")
      .pivot("engine", SearchLabels)
      .agg(sum("count"))
      .na.fill(0, SearchLabels)
  }

  /**
    * Create the easy aggregates: total running hours, crashes, and search counts per country, os, and channel.
    *
    * @param reportData dataframe containing information for hours and searches
    * @param crashData dataframe containing information for crashes
    * @return dataframe with aggregated values
    */
  private def easyAggregates(reportData: DataFrame, crashData: DataFrame): DataFrame = {
    val crashAggregate: DataFrame = crashData
      .groupBy("country", "channel", "os")
      .agg(count("*").alias("crashes"))

    val searchAggregate = searchAggregates(reportData)

    val hourAggregate = reportData
      .select("country", "channel", "os", "hours")
      .groupBy("country", "channel", "os")
      .agg(sum("hours").alias("hours"))

    val groups = Seq("country", "channel", "os")
    hourAggregate
      .join(crashAggregate, groups)
      .join(searchAggregate, groups)
  }

  /**
    * Gather information about clients: new, default, and actve clients.
    *
    * @param reportData dataframe including relevant information about clients
    * @param reportDate date used as the cutoff for new clients
    * @return dataframe with client aggregates
    */
  private def clientValues(reportData: DataFrame, reportDate: String): DataFrame = {
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val dt: DateTime = fmt.parseDateTime(reportDate)
    val reportTimestamp = dt.getMillis() / 1000

    val clientsData: DataFrame = reportData
      .select(
        $"client_id",
        $"country",
        $"channel",
        $"os",
        when($"profile_creation_date" >= reportTimestamp, 1)
          .otherwise(0)
          .alias("new_client"),
        when($"is_default_browser", 1)
          .otherwise(0)
          .alias("default_client"),
        row_number()
          .over(Window.partitionBy("client_id")
            .orderBy(desc("submission_date")))
          .alias("clientid_rank"))
      .select(
        $"client_id",
        $"country",
        $"channel",
        $"os",
        $"new_client",
        $"default_client")
      .where($"clientid_rank" === 1)

    clientsData
      .groupBy("country", "channel", "os")
      .agg(
        count("*").alias("active"),
        sum("new_client").alias("new_client"),
        sum("default_client").alias("default_client"))
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val reportStart = opt[String]("report_start", descr = "Start day of the reporting period (YYYYMMDD)", required = true)
    val mode = opt[String]("mode", descr = "Report mode: weekly or monthly", required = true)
    val outputBucket = opt[String]("bucket", descr = "bucket", required = false)
    val debug = opt[Boolean]("debug", descr = "debug", required = false)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val reportStart = opts.reportStart()
    val mode = opts.mode()
    val debug = opts.debug()
    val outputBucket = "net-mozaws-prod-us-west-2-pipeline-analysis"

    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = formatter.parseDateTime(reportStart)

    val from = reportStart
    val to = from // TODO: to date should be manipulated by mode

    try {
      val mainSummaryData: DataFrame = spark.read.parquet(main_summary_url)
        .filter($"sample_id" === 1)
      val reportData = createReportDataset(mainSummaryData, from, to)
      val crashData = createCrashDataset(from, to)

      if (debug) {
        reportData.count()
        crashData.count()
      }

      val finalReport = easyAggregates(reportData, crashData)
        .join(clientValues(reportData, from), Seq("country", "channel", "os"))

      val s3prefix = s"amiyaguchi/topline/mode=$mode/report_start=$reportStart"
      val s3path = s"s3://$outputBucket/$s3prefix"
      finalReport.write.mode("overwrite").parquet(s3path)
      logger.info(s"Topline Report completed for $reportStart")
    } finally {
      spark.stop()
    }
  }
}
