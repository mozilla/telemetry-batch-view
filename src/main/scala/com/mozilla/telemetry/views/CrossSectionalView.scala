package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils.Aggregation._
import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

case class Longitudinal (
    client_id: String
  , geo_country: Option[Seq[String]]
  , session_length: Option[Seq[Long]]
)

case class CrossSectional (
    client_id: String
  , modal_country: Option[String]
)

object CrossSectionalView {
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
  val sc = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
     
  val hiveContext = new HiveContext(sc)

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val outputBucket = opt[String](
      "outputBucket",
      descr = "Bucket in which to save data",
      required = false,
      default=Some("telemetry-test-bucket/harter"))
    val localTable = opt[String](
      "localTable",
      descr = "Optional path to a local longitudinal table",
      required = false)
    val outName = opt[String](
      "outName",
      descr = "Name for the output of this run",
      required = true)
    verify()
  }

  def loadLocalData(filename: String) = {
    val data = sqlContext.read.parquet(filename)
    data.registerTempTable("longitudinal")
  }

  def main(args: Array[String]): Unit = {
    import hiveContext.implicits._
    val opts = new Opts(args)

    if(opts.localTable.isSupplied) {
      val localTable = opts.localTable()
      loadLocalData(localTable)
    }

    val ds = hiveContext.sql("SELECT * FROM longitudinal").selectExpr("client_id", "geo_country", "session_length").as[Longitudinal]
    val output = ds.map(generateCrossSectional)

    val prefix = s"s3://${opts.outputBucket()}/CrossSectional/${opts.outName}"
    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
