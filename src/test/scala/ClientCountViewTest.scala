package telemetry.test

import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.{FlatSpec, Matchers}
import telemetry.views.ClientCountView

case class Submission(client_id: String,
                      app_name: String,
                      app_version: String,
                      normalized_channel: String,
                      submission_date: String,
                      subsession_start_date: String,
                      country: String,
                      e10s_enabled: Boolean,
                      e10s_cohort: String,
                      os: String,
                      os_version: String,
                      devtools_toolbox_opened_count: Int,
                      loop_activity_open_panel: Int)

object Submission{
  val dimensions = Map("client_id" -> List("x", "y", "z"),
                       "app_name" -> List("Firefox", "Fennec"),
                       "app_version" -> List("44.0"),
                       "normalized_channel" -> List("release", "nightly"),
                       "submission_date" -> List("20160107", "20160106"),
                       "subsession_start_date" -> List("2016-03-13T00:00:00.0+01:00"),
                       "country" -> List("IT", "US"),
                       "e10s_enabled" -> List(true, false),
                       "e10s_cohort" -> List("control", "test"),
                       "os" -> List("Windows", "Darwin"),
                       "os_version" -> List("1.0", "1.1"),
                       "devtools_toolbox_opened_count" -> List(0, 42),
                       "loop_activity_open_panel" -> List(0, 42))

  def randomList: List[Submission] = {
    for {
      clientId <- dimensions("client_id")
      appName <- dimensions("app_name")
      appVersion <- dimensions("app_version")
      normalizedChannel <- dimensions("normalized_channel")
      submissionDate <- dimensions("submission_date")
      subsessionStartDate <- dimensions("subsession_start_date")
      country <- dimensions("country")
      e10sEnabled <- dimensions("e10s_enabled")
      e10sCohort <- dimensions("e10s_cohort")
      os <- dimensions("os")
      osVersion <- dimensions("os_version")
      devtoolsToolboxOpenedCount <- dimensions("devtools_toolbox_opened_count")
      loopActivityOpenPanel <- dimensions("loop_activity_open_panel")
    } yield {
      Submission(clientId.asInstanceOf[String],
                 appName.asInstanceOf[String],
                 appVersion.asInstanceOf[String],
                 normalizedChannel.asInstanceOf[String],
                 submissionDate.asInstanceOf[String],
                 subsessionStartDate.asInstanceOf[String],
                 country.asInstanceOf[String],
                 e10sEnabled.asInstanceOf[Boolean],
                 e10sCohort.asInstanceOf[String],
                 os.asInstanceOf[String],
                 osVersion.asInstanceOf[String],
                 devtoolsToolboxOpenedCount.asInstanceOf[Int],
                 loopActivityOpenPanel.asInstanceOf[Int])
    }
  }
}

class ClientCountViewTest extends FlatSpec with Matchers{
  "Dataset" can "be aggregated" in {
    val sparkConf = new SparkConf().setAppName("KPI")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.udf.register("hll_create", hllCreate _)
    sqlContext.udf.register("hll_cardinality", hllCardinality _)
    import sqlContext.implicits._

    val dataset = sc.parallelize(Submission.randomList).toDF()
    val aggregates = ClientCountView.aggregate(dataset)

    val dimensions = Set(ClientCountView.dimensions:_*) -- Set("client_id")
    (Set(aggregates.columns:_*) -- Set("client_id", "hll", "sum")) should be (dimensions)

    var estimates = aggregates.select(expr("hll_cardinality(hll)")).collect()
    estimates.foreach{ x =>
      x(0) should be (Submission.dimensions("client_id").size)
    }

    val hllMerge = new HyperLogLogMerge
    val count = aggregates
      .select(col("hll"))
      .agg(hllMerge(col("hll")).as("hll"))
      .select(expr("hll_cardinality(hll)")).collect()

    count.size should be (1)
    count(0)(0) should be (Submission.dimensions("client_id").size)
  }
}
