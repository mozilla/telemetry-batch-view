package com.mozilla.telemetry.views

import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

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
}

class CrossSectional (
    val client_id: String
  , val modal_country: Option[String]
) extends DataSetRow {
  override val valSeq = Array[Any](client_id, modal_country)

  def this(ll: Longitudinal) {
    this(
      ll.client_id,
      CrossSectionalView.Aggregation.modalCountry(ll)
    )
  }
}

object CrossSectionalView {
  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val outputBucket = opt[String](
      "outputBucket",
      descr = "Bucket in which to save data",
      required = false,
      default=Some("telemetry-test-bucket/harter"))
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

  private[telemetry] object Aggregation {
    // Spark triggers a strange error during distributed computation if these
    // functions are in the same scope as the main function. This appears to
    // only be an issue with Spark 1.6. 
    // TODO(harter): debug this error and remove this object if possible.
    def weightedMode(values: Seq[String], weights: Seq[Long]): Option[String] = {
      if (values.size > 0 && values.size == weights.size) {
        val pairs = values zip weights
        val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
        Some(agg.maxBy(_._2)._1)
      } else {
        None
      }
    }

    def modalCountry(row: Longitudinal): Option[String] = {
      (row.geo_country, row.session_length) match {
        case (Some(gc), Some(sl)) => weightedMode(gc, sl)
        case _ => None
      }
    } 
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val opts = new Opts(args)

    if(opts.localTable.isSupplied) {
      val localTable = opts.localTable()
      val data = sqlContext.read.parquet(localTable)
      data.registerTempTable("longitudinal")
    }

    val ds = hiveContext
      .sql("SELECT * FROM longitudinal")
      .selectExpr("client_id", "geo_country", "session_length")
      .as[Longitudinal]
    val output = ds.map(new CrossSectional(_))

    val prefix = s"s3://${opts.outputBucket()}/CrossSectional/${opts.outName}"
    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
