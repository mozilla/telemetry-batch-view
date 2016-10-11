package com.mozilla.telemetry.views

import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.utils.S3Store
import com.mozilla.telemetry.utils.aggregation

object Longitudinal {
}

case class Longitudinal (
    val client_id: String
  , val normalized_channel: String
  , val submission_date: Option[Seq[String]]
  , val geo_country: Option[Seq[String]]
  , val session_length: Option[Seq[Long]]
  , val is_default_browser: Option[Seq[Option[Boolean]]]
  , val default_search_engine: Option[Seq[Option[String]]]
  , val locale: Option[Seq[Option[String]]]
  , val architecture: Option[Seq[Option[String]]]
) {
  def weightedMode[A](values: Option[Seq[A]]): Option[A] = {
    (values, this.session_length) match {
      case (Some(v), Some(sl)) => Some(aggregation.weightedMode(v, sl))
      case _ => None
    }
  }

  def parsedSubmissionDate() = {
    val date_parser =  new java.text.SimpleDateFormat("yyyy-MM-dd")
    this.submission_date.getOrElse(Seq()).map(date_parser.parse(_))
  }

  def activeHoursByDOW(dow: Int) = {
    (this.session_length.getOrElse(Seq()) zip this.parsedSubmissionDate)
      .filter(_._2.getDay == dow).map(_._1).sum/3600.0
  }
}

case class CrossSectional (
    val client_id: String
  , val normalized_channel: String
  , val active_hours_total: Double
  , val active_hours_0_mon: Double
  , val active_hours_1_tue: Double
  , val active_hours_2_wed: Double
  , val active_hours_3_thu: Double
  , val active_hours_4_fri: Double
  , val active_hours_5_sat: Double
  , val active_hours_6_sun: Double
  , val geo_mode: Option[String]
  , val geo_cfgs: Long
  , val architecture_mode: Option[String]
  , val ffLocale_mode: Option[String]
) {
  def this(base: Longitudinal) = {
    this(
        client_id = base.client_id
      , normalized_channel = base.normalized_channel
      , active_hours_total = base.session_length.getOrElse(Seq()).sum
      , active_hours_0_mon = base.activeHoursByDOW(1)
      , active_hours_1_tue = base.activeHoursByDOW(2)
      , active_hours_2_wed = base.activeHoursByDOW(3)
      , active_hours_3_thu = base.activeHoursByDOW(4)
      , active_hours_4_fri = base.activeHoursByDOW(5)
      , active_hours_5_sat = base.activeHoursByDOW(6)
      , active_hours_6_sun = base.activeHoursByDOW(0)
      , geo_mode = base.weightedMode(base.geo_country)
      , geo_cfgs = base.geo_country.getOrElse(Seq()).distinct.length
      , architecture_mode = base.weightedMode(base.architecture).getOrElse(None)
      , ffLocale_mode = base.weightedMode(base.locale).getOrElse(None)
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
        "settings.default_search_engine", "build.architecture"
      )
      .as[Longitudinal]
    val output = ds.map(new CrossSectional(_))

    // Save to S3
    val prefix = s"cross_sectional/${opts.outName()}"
    val outputBucket = opts.outputBucket()
    val path = s"s3://${outputBucket}/${prefix}"


    require(S3Store.isPrefixEmpty(outputBucket, prefix),
      s"${path} already exists!")

    output.toDF().write.parquet(path)

    val ex = output.take(2)
    println("="*80 + "\n" + ex + "\n" + "="*80)

    sc.stop()
  }
}
