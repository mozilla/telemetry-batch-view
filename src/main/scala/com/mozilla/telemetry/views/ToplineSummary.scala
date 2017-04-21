package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, format}
import org.joda.time.format.DateTimeFormat
import org.rogach.scallop.ScallopConf

import scala.annotation.tailrec
import scala.util.matching.Regex
import scala.util.Random

object ToplineSummary {
  private val MainSummaryURL: String = "s3://telemetry-parquet/main_summary/v3"

  private val sparkConf: SparkConf = new SparkConf().setAppName("ToplineSummary")
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))

  def session: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  private val SecondsInHour: Int = 60 * 60
  private val SecondsInDay: Int = SecondsInHour * 24

  /* Returns the number of hours represented by seconds. Ignore values that are older than
   * 180 days, the length of time that we store any telemetry ping. */
  private val convertSecondToHours: UserDefinedFunction = udf {
    (seconds: Double) =>
      seconds match {
        case s if s >= 0 && s < 180 * SecondsInDay => s / SecondsInHour
        case _ => 0.0
      }
  }

  /* Converts days into seconds, for positive values */
  private val convertDaysToSeconds: UserDefinedFunction = udf {
    (timestamp: Long) =>
      timestamp match {
        case ts if ts > 0 => ts * SecondsInDay
        case _ => 0
      }
  }

  private val clampToPositive = udf { x: Long => if (x < 0) 0 else x }

  /**
    * Normalizes a string to a set of labels by matching them against a pattern.
    *
    * @param patterns a list of compiled regex patterns
    * @param labels label to attach to a given string once it has been identified
    * @param default default label if regexes don't match
    * @param str input string
    * @return the label of the string
    */
  private def normalize(patterns: List[Regex], labels: List[String], default: String, str: String): String = {
    @tailrec
    def go(mapping: List[(Regex, String)], str: String): String =
      mapping match {
        case Nil => default
        case (pattern, label) :: xs =>
          pattern findFirstIn str match {
          case Some(_) => label
          case None => go(xs, str)
        }
      }
    go(patterns zip labels, str match {case ""|null => "" case x => x})
  }

  private val normalizeOS: UserDefinedFunction = udf {
    (os: String) => normalize(
      List("^[Windows|WINNT]", "^Darwin", "(?:Linux|BSD|SunOS)")
        .map(pattern => pattern.r),
      List("Windows", "Mac", "Linux"),
      "Other",
      os)
  }

  // Also used to create the pivot table
  private val SearchLabels = List("google", "bing", "yahoo", "other")
  private val normalizeSearch = udf {
    (searches: String) => normalize(
      List("[Gg]oogle", "[Bb]ing", "[Yy]ahoo")
        .map(pattern => pattern.r),
      SearchLabels.init,
      SearchLabels.last,
      searches)
  }

  private val CountryNames = Set(
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
    (country: String) => if (!country.isEmpty && (CountryNames contains country)) { country } else { "Other" }
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
    val s = session
    import s.implicits._

    mainSummary
      .filter($"submission_date_s3" >= startDate)
      .filter($"submission_date_s3" < endDate)
      .filter($"app_name" === "Firefox")
      .na.fill("", Seq("country"))
      .dropDuplicates("document_id")
      .select(
        $"client_id",
        $"submission_date",
        $"is_default_browser",
        $"search_counts",
        normalizeCountry($"country").alias("country"),
        convertDaysToSeconds($"profile_creation_date").alias("profile_creation_date"),
        $"normalized_channel".alias("channel"),
        normalizeOS($"os").alias("os"),
        convertSecondToHours($"subsession_length".cast(DoubleType)).alias("hours"))
  }

  /**
    * Create the dataset required for counting the number of crashes
    *
    * This is currently implemented by searching through the raw pings. This could be done using
    * the crash summary dataset.
    *
    * @param startDate start date in yyyymmdd
    * @param endDate end date in yyyymmdd
    * @return Dataset where every row represents a single crash
    */
  private def createCrashDataset(startDate: String, endDate: String): DataFrame = {
    val s = session
    import s.implicits._

    val CrashSchema = StructType(List(
      StructField("document_id", StringType, nullable = false),
      StructField("country", StringType, nullable = true),
      StructField("channel", StringType, nullable = true),
      StructField("os", StringType, nullable = true)))

    implicit val sc = SparkContext.getOrCreate(sparkConf)
    val messages: RDD[Message] = Dataset("telemetry")
      .where("docType") { case "crash" => true }
      .where("appName") { case "Firefox" => true }
      .where("submissionDate") { case date => startDate <= date && date < endDate }
      .records()


    val crashRDD = messages.flatMap(m => {
      messageToRow(m) match {
        case None => None
        case x => x
      }
    })
    val crashData = s.createDataFrame(crashRDD, CrashSchema)

    crashData
      .dropDuplicates("document_id")
      .select(
        normalizeCountry($"country").alias("country"),
        $"channel",
        normalizeOS($"os").alias("os"))
  }

  private def messageToRow(message: Message): Option[Row] = {
    val fields = message.fieldsAsMap

    val row = Row(
      fields.getOrElse("documentId", None) match {
        case x: String => x
        case _ => return None
      },
      fields.getOrElse("geoCountry", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("normalizedChannel", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("os", None) match {
        case x: String => x
        case _ => ""
      }
    )

    Some(row)
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
    val s = session
    import s.implicits._

    val searchData = reportData
      .where($"search_counts".isNotNull)
      .withColumn("search_counts", explode($"search_counts"))
      .select(
        $"country",
        $"channel",
        $"os",
        normalizeSearch($"search_counts.engine").alias("engine"),
        clampToPositive($"search_counts.count").alias("count"))

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
      .join(crashAggregate, groups, "outer")
      .join(searchAggregate, groups, "outer")
  }

  /**
    * Gather information about clients: new, default, and active clients.
    *
    * @param reportData dataframe including relevant information about clients
    * @param reportDate date used as the cutoff for new clients
    * @return dataframe with client aggregates
    */
  private def clientValues(reportData: DataFrame, reportDate: String): DataFrame = {
    val s = session
    import s.implicits._

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
        count("*").alias("actives"),
        sum("new_client").alias("new_records"),
        sum("default_client").alias("default"))
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val reportStart = opt[String]("report_start", descr = "Start day of the reporting period (YYYYMMDD)", required = true)
    val mode = opt[String]("mode", descr = "Report mode: weekly or monthly", default = Some("monthly"), required = true)
    val outputBucket = opt[String]("bucket", descr = "bucket", required = true)
    val outputPrefix = opt[String]("prefix", descr = "prefix", required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val s = session
    import s.implicits._

    val opts = new Opts(args)
    val reportStart = opts.reportStart()
    val mode = opts.mode()
    val bucket = opts.outputBucket()
    val prefix = opts.outputPrefix()
    val version = "v1"

    val s3path = s"s3://$bucket/$prefix/$version/mode=$mode/report_start=$reportStart"

    // find the date range for the report
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = formatter.parseDateTime(reportStart)

    if (mode != "weekly"  && mode != "monthly") {
      s.stop()
      throw new RuntimeException(s"Unknown run mode '$mode'. Should be either 'weekly' or 'monthly'")
    }

    val toDate: DateTime = mode match {
      case "weekly" => fromDate.plusWeeks(1)
      case "monthly" => fromDate.plusMonths(1)
    }

    val from = formatter.print(fromDate)
    val to = formatter.print(toDate)

    if (toDate.isAfterNow()) {
      s.stop()
      throw new RuntimeException(s"Report is still gathering data until $to $mode")
    }

    println(s"Starting report from $from to $to with mode $mode")
    try {
      val dataset: DataFrame = s.read.parquet(MainSummaryURL)

      val reportData = createReportDataset(dataset, from, to)
      val crashData = createCrashDataset(from, to)

      val reportFormat = format.DateTimeFormat.forPattern("yyyy-mm-dd")
      val finalReport = easyAggregates(reportData, crashData)
        .join(clientValues(reportData, from), Seq("country", "channel", "os"), "outer")
        .withColumn("date", lit(reportFormat.print(fromDate)))
        .withColumnRenamed("country", "geo")
        // replace nulls in outer joins
        .na.fill(0, Seq("hours", "crashes", "google", "bing", "yahoo", "other", "actives", "new_records", "default"))

      println(s"Saving report to $s3path")
      finalReport.write.mode("overwrite").parquet(s3path)
      println(s"Topline Report completed for $reportStart")
    } finally {
      s.stop()
    }
  }
}
