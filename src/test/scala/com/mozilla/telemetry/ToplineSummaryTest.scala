package com.mozilla.telemetry

import com.mozilla.telemetry.views.ToplineSummary
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructField, StructType}
import org.joda.time.format
import org.scalatest._

import scala.util.Try


case class Crashes(document_id: String,
                   country: String,
                   channel: String,
                   os: String)


case class SearchCounts(engine: String,
                        source: String,
                        count: Some[Long])


case class PartialToplineMain(app_name: String,
                              document_id: String,
                              channel: String,
                              normalized_channel: String,
                              client_id: String,
                              country: String,
                              is_default_browser: Boolean,
                              profile_creation_date: Long,
                              os: String,
                              search_counts: Seq[SearchCounts],
                              submission_date: String,
                              subsession_length: Long,
                              submission_date_s3: String)


class ToplineSummaryTest extends FlatSpec
                            with Matchers
                            with PrivateMethodTester
                            with DataFrameSuiteBase {
  private val dateFormat = format.DateTimeFormat.forPattern("yyyyMMdd")
  private val fakeDate = "20161201"
  private val pastDate = "20161101"
  private val futureDate = "20161203"
  private val endPeriodDate = "20161208"

  private val fakePing =
    PartialToplineMain(
      app_name = "Firefox",
      document_id = "unique_id",
      country = "US",
      channel = "beta",
      normalized_channel = "beta",
      os = "Linux",
      client_id = "fake_client",
      is_default_browser = true,
      profile_creation_date = 10000, // days since epoch
      search_counts = Seq(
        SearchCounts(
          engine = "yahoo",
          source = "",
          count = Some(1)),
        SearchCounts(
          engine = "yahoo",
          source = "awesomebar",
          count = Some(3))
      ),
      submission_date = fakeDate,
      subsession_length = 3600,
      submission_date_s3 = fakeDate)

  private val fake_crash =
    Crashes(
      document_id = "unique_id",
      country = "US",
      channel = "beta",
      os = "Linux")

  /* Private methods that we will be testing. The `createReportDataset` method will be the
   * primary method of testing the UDFs, since testing them directly is neigh impossible. */
  private val createReportDataset = PrivateMethod[DataFrame]('createReportDataset)(_: DataFrame, fakeDate, endPeriodDate)
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
  private def uniqueMain(main: Seq[PartialToplineMain]): Seq[PartialToplineMain] = {
    main.zip(1 to main.length).map({ case (ping, id) => ping.copy(document_id = id.toString)})
  }
  private def uniqueCrashes(crashes: Seq[Crashes]): Seq[Crashes] = {
    crashes.zip(1 to crashes.length).map({ case (ping, id) => ping.copy(document_id = id.toString)})
  }

  "createReportDataset" should "rename a column that goes through a udf" in {
    import sqlContext.implicits._
    val data = Seq(fakePing).toDF()
    val df = ToplineSummary invokePrivate createReportDataset(data)
    assert(hasColumn(df, "country"))
  }

  it should "not count duplicate pings" in {
    import sqlContext.implicits._
    val data = Seq(fakePing, fakePing).toDF()
    val df = ToplineSummary invokePrivate createReportDataset(data)
    assert(df.count() == 1)
  }

  it should "not count pings after reporting period" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(submission_date_s3 = endPeriodDate)).toDF()
    val df = ToplineSummary invokePrivate createReportDataset(data)
    assert(df.count() == 0)
  }

  it should "handle empty column values" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(country = "", os = "")).toDF()
    val df = ToplineSummary invokePrivate createReportDataset(data)
    var expect = "Other"

    val cols = Seq("country", "os")
    for (col <- cols) {
      val result = df.head().getAs[String](col)
      assert(result == expect)
    }
  }

  "[UDF] convertHours" should "be a double" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(subsession_length = 1800)).toDF()
    val expect = 0.5

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  it should "ignore negative numbers" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(subsession_length = -3600)).toDF()
    val expect = 0

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  it should "ignore values greater than 180 days" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(subsession_length = 181 * 24 * 60 * 60)).toDF()
    val expect = 0

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  it should "handle null values" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[Long] }
    val data = createNullableDataFrame(Seq(fakePing).toDF())
      .withColumn("subsession_length", nullify())
    val expect = 0

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  "[UDF] convertProfileCreation" should "handle null values" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[Long] }
    val data = createNullableDataFrame(Seq(fakePing).toDF())
      .withColumn("profile_creation_date", nullify())
    val expect = 0

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[Long]("profile_creation_date")

    assert(expect == result)
  }

  "[UDF] normalizeCountry" should "recognize valid country IDs" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(country = "US")).toDF()
    val expect = "US"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("country")

    assert(expect == result)
  }

  it should "handle invalid country IDs" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(country = "Atlantis")).toDF()
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("country")

    assert(expect == result)
  }

  it should "handle null values" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[String] }
    val data = createNullableDataFrame(Seq(fakePing).toDF())
      .withColumn("country", nullify())
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("country")

    assert(expect == result)
  }

  "[UDF] normalizeOS" should "recognize `Windows 98` as Windows" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(os = "Windows 98")).toDF()
    val expect = "Windows"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  it should "recognize `FreeBSD` as Linux" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(os = "FreeBSD")).toDF()
    val expect = "Linux"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  it should "recognize `Ubuntu Linux` as Linux" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(os = "Ubuntu Linux")).toDF()
    val expect = "Linux"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  it should "recognize `BeOS` as Other" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(os = "BeOS")).toDF()
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  it should "recognize `ltext-Windows` as Other" in {
    import sqlContext.implicits._
    val data = Seq(fakePing.copy(os = "ltext-Windows")).toDF()
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }

  it should "handle null values" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[String] }
    val data = createNullableDataFrame(Seq(fakePing).toDF())
      .withColumn("os", nullify())
    val expect = "Other"

    val df = ToplineSummary invokePrivate createReportDataset(data)
    val result = df.head().getAs[String]("os")

    assert(expect == result)
  }


  "searchAggregates" should "aggregate the number of yahoo counts" in {
    import sqlContext.implicits._
    val main_df = uniqueMain(Seq(fakePing, fakePing)).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    // 2 x (ping.yahoo = 4)
    val expect = 8

    val df = ToplineSummary invokePrivate searchAggregates(reportData)
    val result = df.head().getAs[Long]("yahoo")

    assert(expect == result)
  }

  it should "handle null values" in {
    import sqlContext.implicits._
    val data = createNullableDataFrame(
        uniqueMain(
          Seq(fakePing, fakePing.copy(search_counts=null))
      ).toDF())
    val expect = 4

    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val df = ToplineSummary invokePrivate searchAggregates(reportData)
    val result = df.head().getAs[Long]("yahoo")

    assert(expect == result)
  }

  it should "handle negative values" in {
    import sqlContext.implicits._
    val data = createNullableDataFrame(
      uniqueMain(Seq(
        fakePing,
        fakePing.copy(
          search_counts = Seq(
            SearchCounts(
              engine = "yahoo",
              source = "",
              count = Some(-100))))))
        .toDF())
    val expect = 4

    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val df = ToplineSummary invokePrivate searchAggregates(reportData)
    val result = df.head().getAs[Long]("yahoo")

    assert(expect == result)
  }

  it should "handle zero searches" in {
    import sqlContext.implicits._
    val nullify = udf { () => None: Option[Seq[SearchCounts]] }
    val data = createNullableDataFrame(Seq(fakePing).toDF())
      .withColumn("search_counts", nullify())
    val expect = 0

    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val df = ToplineSummary invokePrivate searchAggregates(reportData)
    val result = df.count()

    assert(expect == result)
  }


  it should "default to 0 search counts" in {
    import sqlContext.implicits._
    val data = Seq(fakePing).toDF()
    val expect = 0

    val df = ToplineSummary invokePrivate searchAggregates(data)
    val result = df.head().getAs[Long]("bing")

    assert(expect == result)
  }

  "easyAggregates" should "aggregate the number of total crashes" in {
    // assumes that country, channel, os are the same for groupby
    import sqlContext.implicits._
    val reportData = ToplineSummary invokePrivate createReportDataset(Seq(fakePing).toDF())
    val crashData = Seq(fake_crash, fake_crash).toDF()
    val expect = 2

    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)
    val result = df.head().getAs[Long]("crashes")

    assert(expect == result)
  }

  it should "aggregate the number of hours correctly" in {
    import sqlContext.implicits._
    val main_df = uniqueMain(Seq(fakePing, fakePing)).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    val crashData = uniqueCrashes(Seq(fake_crash, fake_crash)).toDF()
    // 2 x (ping.hours = 1)
    val expect = 2

    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)
    val result = df.head().getAs[Double]("hours")

    assert(expect == result)
  }

  it should "join the report and crash data together" in {
    import sqlContext.implicits._
    val main_df = uniqueMain(Seq(fakePing, fakePing)).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    val crashData = uniqueCrashes(Seq(fake_crash, fake_crash)).toDF()

    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)

    assert(hasColumn(df, "crashes") && hasColumn(df, "hours") && hasColumn(df, "yahoo"))
    // assert that the pings and crashes are in the same bucket
    assert(df.count() == 1)
  }

  it should "keep rows in report data not crash data" in {
    import sqlContext.implicits._
    val main_df = uniqueMain(Seq(fakePing.copy(country="DE"))).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    val crashData = uniqueCrashes(Seq(fake_crash)).toDF()

    // assert that a value is not dropped
    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)
    assert(df.count() == 2)
  }

  it should "keep rows in crash data not report data" in {
    import sqlContext.implicits._
    val main_df = uniqueMain(Seq(fakePing)).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    val crashData = uniqueCrashes(Seq(fake_crash.copy(country="DE"))).toDF()

    // assert that a value is not dropped
    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)
    assert(df.count() == 2)
  }

  it should "keep the entire set of rows" in {
    import sqlContext.implicits._
    val main_df = uniqueMain(Seq(fakePing, fakePing.copy(country="DE"))).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(main_df)
    val crashData = uniqueCrashes(Seq(fake_crash, fake_crash.copy(country="BR"))).toDF()

    // assert that a value is not dropped
    val df = ToplineSummary invokePrivate easyAggregates(reportData, crashData)
    assert(df.count() == 3)
  }

  "clientValues" should "count a new client" in {
    import sqlContext.implicits._
    val future = dateFormat.parseDateTime(futureDate).getMillis() / (1000 * 3600 * 24)
    val data = Seq(fakePing.copy(profile_creation_date = future)).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 1

    val df = ToplineSummary invokePrivate clientValues(reportData, fakeDate)
    val result = df.head().getAs[Long]("new_records")

    assert(expect == result)
  }

  it should "not count a client as new" in {
    import sqlContext.implicits._
    val past = dateFormat.parseDateTime(pastDate).getMillis() / (1000 * 3600 * 24)
    val data = Seq(fakePing.copy(profile_creation_date = past)).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 0

    val df = ToplineSummary invokePrivate clientValues(reportData, fakeDate)
    val result = df.head().getAs[Long]("new_records")

    assert(expect == result)
  }

  it should "count a default client" in {
    import sqlContext.implicits._
    val data = uniqueMain(Seq(
      fakePing.copy(client_id = "foo", is_default_browser = true),
      fakePing.copy(client_id = "bar", is_default_browser = true),
      fakePing.copy(client_id = "baz", is_default_browser = false)
    )).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 2

    val df = ToplineSummary invokePrivate clientValues(reportData, fakeDate)
    val result = df.head().getAs[Long]("default")

    assert(expect == result)
  }

  it should "select only the most recent client" in {
    import sqlContext.implicits._
    val data = uniqueMain(Seq(
      fakePing.copy(client_id = "foo", is_default_browser = true),
      fakePing.copy(client_id = "foo", is_default_browser = true)
    )).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 1

    val df = ToplineSummary invokePrivate clientValues(reportData, fakeDate)
    val result = df.head().getAs[Long]("default")

    assert(expect == result)
  }

  it should "aggregate the number of active users" in {
    import sqlContext.implicits._
    val data = uniqueMain(Seq(
      fakePing.copy(client_id = "foo", is_default_browser = true),
      fakePing.copy(client_id = "bar", is_default_browser = true),
      fakePing.copy(client_id = "baz", is_default_browser = false)
    )).toDF()
    val reportData = ToplineSummary invokePrivate createReportDataset(data)
    val expect = 3

    val df = ToplineSummary invokePrivate clientValues(reportData, fakeDate)
    val result = df.head().getAs[Long]("actives")

    assert(expect == result)
  }
}
