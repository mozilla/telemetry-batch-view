package com.mozilla.telemetry

import awscala.s3._
import com.mozilla.telemetry.heka.{HekaFrame, Message}
import com.mozilla.telemetry.parquet.ParquetFile
import com.mozilla.telemetry.DerivedStream.s3
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class SimpleDerivedStream extends DerivedStream {
  protected val version = "v1"

  protected def buildSchema: Schema

  protected def buildRecord(message: Message, schema: Schema): Option[GenericRecord]

  override def transform(sc: SparkContext, bucket: Bucket, input: RDD[ObjectSummary], from: String, to: String) {
    val tasks = input
      .groupBy(summary => prefixGroup(summary.key))
      .flatMap(x => ObjectSummary.groupBySize(x._2.toIterator).toIterator.zip(Iterator.continually{x._1}))
      .filter{ case (_, prefix) =>
        val partitionedPrefix = partitioning.partitionPrefix(prefix, version)
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
    val schema = buildSchema
    val records = for {
      key <- keys
      hekaFile = bucket
      .getObject(key.key)
      .getOrElse(throw new Exception("File missing on S3"))
      message <- HekaFrame.parse(hekaFile.getObjectContent)
      record <- buildRecord(message, schema)
    } yield record

    val partitionedPrefix = partitioning.partitionPrefix(prefix, version)
    while(!records.isEmpty) {
      val localFile = ParquetFile.serialize(records, schema)
      uploadLocalFileToS3(localFile, s"$partitionedPrefix")
    }
  }
}
