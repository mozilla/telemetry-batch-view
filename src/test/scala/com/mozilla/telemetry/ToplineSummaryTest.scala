package com.mozilla.telemetry

import com.mozilla.telemetry.views.ToplineSummary
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructField, StructType}
import org.joda.time.format
import org.scalatest._

import scala.util.Try


case class Crashes(country: String,
                   channel: String,
                   os: String)

case class SearchCounts(engine: String,
                        source: String,
                        count: Int)

case class PartialToplineMain(app_name: String,
                       channel: String,
                       client_id: String,
                       country: String,
                       is_default_browser: Boolean,
                       profile_creation_date: Int,
                       os: String,
                       search_counts: Seq[SearchCounts],
                       submission_date: String,
                       subsession_length: Int,
                       submission_date_s3: String)


class ToplineSummaryTest extends FlatSpec
                            with Matchers
                            with PrivateMethodTester
                            with DataFrameSuiteBase {
  private val dateFormat = format.DateTimeFormat.forPattern("yyyyMMdd")
  private val fake_date = "20161201"
  private val past_date = "20161101"
  private val future_date = "20161215"

  private val fake_ping =
    PartialToplineMain(
      app_name = "Firefox",
      country = "US",
      channel = "beta",
      os = "Linux",
      client_id = "fake_client",
      is_default_browser = true,
      profile_creation_date = 10000, // days since epoch
      search_counts = Seq(
        SearchCounts(
          engine = "yahoo",
          source = "",
          count = 1),
        SearchCounts(
          engine = "yahoo",
          source = "awesomebar",
          count = 3)
      ),
      submission_date = fake_date,
      subsession_length = 3600,
      submission_date_s3 = fake_date)

  private val fake_crash =
    Crashes(
      country = "US",
      channel = "beta",
      os = "Linux")

  /* Private methods that we will be testing. The `createReportDataset` method will be the
   * primary method of testing the UDFs, since testing them directly is neigh impossible. */
  private val createReportDataset = PrivateMethod[DataFrame]('createReportDataset)(_: DataFrame, fake_date, fake_date)
  private val clientValues = PrivateMethod[DataFrame]('clientValues)
  private val easyAggregates = PrivateMethod[DataFrame]('easyAggregates)
  private val searchAggregates = PrivateMethod[DataFrame]('searchAggregates)

  /**
    * Set all properties of a dataframe to be nullable.
    *
    * The implicit conversion of the case class into a spark schema will set nullable to false.
    * Once way to solve this is to wrap the datatypes with in an Optional type, but this defeats
    * the purpose of using the spark implicits due to verbosity. For example, if we wanted to have
    * a nullable `PartialMain.channel` field, we would set its type to `Some[String]`, but this
    * would subsequently require wrapping all our input fed into the tests.
    *
    * Source: http://stackoverflow.com/questions/33193958/change-nullable-property-of-column-in-spark-dataframe
    *
    * @param df source DataFrame
    * @return DataFrame where all columns are nullable
    */
  private def createNullableDataFrame(df: DataFrame): DataFrame = {
    val schema = StructType(df.schema.map {
      case StructField(c, t, _, m) => StructField(c, t, nullable = true, m)
    })
    df.sqlContext.createDataFrame(df.rdd, schema)
  }

  private def hasColumn(df: DataFrame, column_name: String): Boolean = Try(df(column_name)).isSuccess


  "createReportDataset" should "rename a column that goes through a udf" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping).toDF()
    val df = ToplineSummary invokePrivate createReportDataset(data)
    df.show()
    assert(hasColumn(df, "country"))
  }

  "[UDF] convertHours" should "be a double" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(subsession_length = 1800)).toDF()
    val expect = 0.5

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  it should "ignore negative numbers" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(subsession_length = -3600)).toDF()
    val expect = 0

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  it should "ignore values greater than 180 days" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(subsession_length = 181 * 24 * 60 * 60)).toDF()
    val expect = 0

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  it should "handle null values" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[Int] }
    val data = createNullableDataFrame(Seq(fake_ping).toDF())
      .withColumn("subsession_length", nullify())
    val expect = 0

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  "[UDF] convertProfileCreation" should "handle null values" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[Int] }
    val data = createNullableDataFrame(Seq(fake_ping).toDF())
      .withColumn("profile_creation_date", nullify())
    val expect = 0

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Int]("profile_creation_date")

    assert(expect == result)
  }

  "[UDF] normalizeCountry" should "recognize valid country IDs" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(country = "US")).toDF()
    val expect = "US"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("country")

    assert(expect == result)
  }

  it should "handle invalid country IDs" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(country = "Atlantis")).toDF()
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("country")

    assert(expect == result)
  }

  it should "handle null values" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[String] }
    val data = createNullableDataFrame(Seq(fake_ping).toDF())
      .withColumn("country", nullify())
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("country")

    assert(expect == result)
  }


  "[UDF] normalizeChannel" should "recognize release as a valid channel" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(channel = "release")).toDF()
    val expect = "release"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("channel")

    assert(expect == result)
  }

  it should "recognize release_withjunk as other" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(channel = "release_withjunk")).toDF()
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("channel")

    assert(expect == result)
  }

  it should "handle null values" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[String] }
    val data = createNullableDataFrame(Seq(fake_ping).toDF())
      .withColumn("channel", nullify())
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("channel")

    assert(expect == result)
  }


  "[UDF] normalizeOS" should "recognize `Windows 98` as Windows" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(os = "Windows 98")).toDF()
    val expect = "Windows"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  it should "recognize `FreeBSD` as Linux" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(os = "FreeBSD")).toDF()
    val expect = "Linux"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  it should "recognize `Ubuntu Linux` as Linux" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(os = "Ubuntu Linux")).toDF()
    val expect = "Linux"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  it should "recognize `BeOS` as Other" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping.copy(os = "BeOS")).toDF()
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  "searchAggregates" should "aggregate the number of yahoo counts" in {
    import sqlContext.implicits._
    val main_df = Seq(fake_ping, fake_ping).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    // 2 x (ping.yahoo = 4)
    val expect = 8

    val df = ToplineSummary invokePrivate searchAggregates(reportData)
    val result = df.head().getAs[Long]("yahoo")

    assert(expect == result)
  }

  it should "default to 0 search counts" in {
    import sqlContext.implicits._
    val data = Seq(fake_ping).toDF()
    val expect = 0

    val df = ToplineSummary invokePrivate searchAggregates(data)
    val result = df.head().getAs[Long]("bing")

    assert(expect == result)
  }

  "easyAggregates" should "aggregate the number of total crashes" in {
    // assumes that country, channel, os are the same for groupby
    import sqlContext.implicits._
    val reportData = ToplineSummary invokePrivate createReportDataset(Seq(fake_ping).toDF())
    val crashData = Seq(fake_crash, fake_crash).toDF()
    val expect = 2

    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)
    val result = df.head().getAs[Long]("crashes")

    assert(expect == result)

  }

  it should "aggregate the number of hours correctly" in {
    import sqlContext.implicits._
    val main_df = Seq(fake_ping, fake_ping).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    val crashData = Seq(fake_crash, fake_crash).toDF()
    // 2 x (ping.hours = 1)
    val expect = 2

    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  it should "join the report and crash data together" in {
    import sqlContext.implicits._
    val main_df = Seq(fake_ping, fake_ping).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    val crashData = Seq(fake_crash, fake_crash).toDF()

    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)

    assert(hasColumn(df, "crashes") && hasColumn(df, "hours") && hasColumn(df, "yahoo"))
  }

  "clientValues" should "count a new client" in {
    import sqlContext.implicits._
    val future = dateFormat.parseDateTime(future_date).getMillis() / (1000 * 3600 * 24)
    val data = Seq(fake_ping.copy(profile_creation_date = future.toInt)).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 1

    val df = ToplineSummary invokePrivate clientValues(reportData, fake_date)
    val result = df.head().getAs[Long]("new_client")

    assert(expect == result)
  }

  it should "not count a client as new" in {
    import sqlContext.implicits._
    val past = dateFormat.parseDateTime(past_date).getMillis() / (1000 * 3600 * 24)
    val data = Seq(fake_ping.copy(profile_creation_date = past.toInt)).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 0

    val df = ToplineSummary invokePrivate clientValues(reportData, fake_date)
    val result = df.head().getAs[Long]("new_client")

    assert(expect == result)
  }

  it should "count a default client" in {
    import sqlContext.implicits._
    val data = Seq(
      fake_ping.copy(client_id = "foo", is_default_browser = true),
      fake_ping.copy(client_id = "bar", is_default_browser = true),
      fake_ping.copy(client_id = "baz", is_default_browser = false)
    ).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 2

    val df = ToplineSummary invokePrivate clientValues(reportData, fake_date)
    val result = df.head().getAs[Long]("default_client")

    assert(expect == result)
  }

  it should "select only the most recent client" in {
    import sqlContext.implicits._
    val data = Seq(
      fake_ping.copy(client_id = "foo", is_default_browser = true),
      fake_ping.copy(client_id = "foo", is_default_browser = true)
    ).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 1

    val df = ToplineSummary invokePrivate clientValues(reportData, fake_date)
    val result = df.head().getAs[Long]("default_client")

    assert(expect == result)

  }

  it should "aggregate the number of active users" in {
    import sqlContext.implicits._
    val data = Seq(
      fake_ping.copy(client_id = "foo", is_default_browser = true),
      fake_ping.copy(client_id = "bar", is_default_browser = true),
      fake_ping.copy(client_id = "baz", is_default_browser = false)
    ).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 3

    val df = ToplineSummary invokePrivate clientValues(reportData, fake_date)
    val result = df.head().getAs[Long]("active")

    assert(expect == result)
  }
}
