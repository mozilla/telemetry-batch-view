package telemetry

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import awscala.s3._
import com.github.nscala_time.time.Imports._
import com.typesafe.config._
import heka.{HekaFrame, Message}
import java.io.File
import java.util.UUID
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time.Days
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import scala.io.Source
import telemetry.parquet.ParquetFile
import telemetry.streams.{ExecutiveStream, E10sExperiment}

abstract class SimpleDerivedStream extends DerivedStream {
  protected def buildSchema: Schema

  protected def buildRecord(message: Message, schema: Schema): Option[GenericRecord]

  override def transform(sc: SparkContext, bucket: Bucket, input: RDD[ObjectSummary], from: String, to: String) {
    val tasks = input
      .groupBy(summary => prefixGroup(summary.key))
      .flatMap(x => groupBySize(x._2.toIterator).toIterator.zip(Iterator.continually{x._1}))
      .filter{ case (_, prefix) =>
        val partitionedPrefix = partitioning.partitionPrefix(prefix)
        if (!isS3PrefixEmpty(partitionedPrefix)) {
          println(s"Warning: can't process $prefix as data already exists!")
          false
        } else true }

    tasks
      .repartition(tasks.count().toInt)
      .foreach(x => transform(bucket, x._1.toIterator, x._2))
  }

  private def prefixGroup(key: String): String = {
    val Some(m) = "(.+)/.+".r.findFirstMatchIn(key)
    m.group(1)
  }

  private def transform(bucket: Bucket, keys: Iterator[ObjectSummary], prefix: String) {
    implicit val s3 = S3()
    val schema = buildSchema
    val records = for {
      key <- keys
      hekaFile = bucket
      .getObject(key.key)
      .getOrElse(throw new Exception("File missing on S3"))
      message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey())
      record <- buildRecord(message, schema)
    } yield record

    val partitionedPrefix = partitioning.partitionPrefix(prefix)
    while(!records.isEmpty) {
      val localFile = ParquetFile.serialize(records, schema, chunked=true)
      uploadLocalFileToS3(localFile, s"$partitionedPrefix")
      new File(localFile).delete()
    }
  }

  private def groupBySize(keys: Iterator[ObjectSummary]): List[List[ObjectSummary]] = {
    val threshold = 1L << 31
    keys.foldRight((0L, List[List[ObjectSummary]]()))(
      (x, acc) => {
        acc match {
          case (size, head :: tail) if size + x.size < threshold =>
            (size + x.size, (x :: head) :: tail)
          case (size, res) if size + x.size < threshold =>
            (size + x.size, List(x) :: res)
          case (_, res) =>
            (x.size, List(x) :: res)
        }
      })._2
  }
}
