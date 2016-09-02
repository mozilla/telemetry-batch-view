package com.mozilla.telemetry.views

import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.utils.S3Store
import com.mozilla.telemetry.utils.aggregation

abstract class DataSetRow() extends Product {
  // This class is a work around the 22 field limit in case classes. The 22
  // field limit is removed in scala 2.11, but until we upgrade our spark
  // clusters, we have to work with 2.10.
  // Spark only includes encoders for primitive types and objects implementing
  // the Product interface.
  val valSeq: Array[Any]

  def productArity() = valSeq.length
  def productElement(n: Int) = valSeq(n)
  //TODO(harter): restrict equality to a data type
  def canEqual(that: Any) = true
  override def equals(that: Any) = {
    that match {
      case that: DataSetRow => that.canEqual(this) && this.valSeq.deep == that.valSeq.deep
      case _ => false
    }
  }
}

class Longitudinal (
    val client_id: String
  , val geo_country: Option[Seq[String]]
  , val session_length: Option[Seq[Long]]
) extends DataSetRow {
  override val valSeq = Array[Any](client_id, geo_country, session_length)

  def sessionWeightedMode(values: Option[Seq[String]]) = {
    (values, this.session_length) match {
      case (Some(gc), Some(sl)) => Some(aggregation.weightedMode(gc, sl))
      case _ => None
    }
  }
}

class CrossSectional (
    val client_id: String
  , val modal_country: Option[String]
) extends DataSetRow {
  override val valSeq = Array[Any](client_id, modal_country)

  def this(base: Longitudinal) = {
    this(
      client_id = base.client_id,
      modal_country = base.sessionWeightedMode(base.geo_country)
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
      .selectExpr("client_id", "geo_country", "session_length")
      .as[Longitudinal]
    val output = ds.map(new CrossSectional(_))

    // Save to S3
    val prefix = s"cross_sectional/${opts.outName()}"
    val outputBucket = opts.outputBucket()
    val path = s"s3://${outputBucket}/${prefix}"


    require(S3Store.isPrefixEmpty(outputBucket, prefix),
      s"${path} already exists!")

    output.toDF().write.parquet(path)

    // Force the computation, debugging purposes only
    // TODO(harterrt): Remove this
    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
