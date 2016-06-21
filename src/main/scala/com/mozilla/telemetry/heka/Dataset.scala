package com.mozilla.telemetry.heka

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import com.mozilla.telemetry.utils.{AbstractS3Store, S3Store}
import com.mozilla.telemetry.ObjectSummary
import scala.io.Source

case class Schema(dimensions: List[Dimension])
case class Dimension(fieldName: String)

class Dataset private (bucket: String, schema: Schema, prefix: String,
                       clauses: Map[String, PartialFunction[String, Boolean]],
                       s3Store: => AbstractS3Store) extends java.io.Serializable {
  private object Logger extends Serializable {
    @transient lazy val log = org.apache.log4j.Logger.getLogger(Dataset.getClass.getName)
  }

  def where(dimension: String)(clause: PartialFunction[String, Boolean]): Dataset = {
    if (clauses.contains(dimension))
      throw new Exception(s"There should be only one clause for $dimension")

    if (!schema.dimensions.contains(Dimension(dimension)))
      throw new Exception(s"The dimension $dimension doesn't exists")

    new Dataset(bucket, schema, prefix, clauses + (dimension -> clause), s3Store)
  }

  def summaries(fileLimit: Option[Int] = None): Stream[ObjectSummary] = {
    def scan(dimensions: List[Dimension], prefixes: Stream[String]): Stream[String] = {
      if (dimensions.isEmpty) {
        prefixes
      } else {
        val dimension = dimensions.head
        val clause = clauses.getOrElse(dimension.fieldName, {case x => true}: PartialFunction[String, Boolean])
        val matched = prefixes
          .flatMap(s3Store.listFolders(bucket, _))
          .filter{ prefix =>
            val value = prefix.split("/").last
            clause.isDefinedAt(value) && clause(value)
          }
        scan(dimensions.tail, matched)
      }
    }

    val keys = scan(schema.dimensions, Stream(s"$prefix/"))
      .flatMap(s3Store.listKeys(bucket, _))

    fileLimit match {
      case Some(x) => keys.take(x)
      case _ => keys
    }
  }

  def records(fileLimit: Option[Int] = None)(implicit sc: SparkContext): RDD[Message] = {
    // Partition the files into groups of approximately-equal size
    val groups = ObjectSummary.groupBySize(summaries(fileLimit).toIterator)
    sc.parallelize(groups, groups.size).flatMap(x => x).flatMap(o => {
      HekaFrame.parse(s3Store.getKey(bucket, o.key), ex => Logger.log.warn(s"Failure to read file ${o.key}: ${ex.getMessage}"))
    })
  }
}

object Dataset {
  /* Note that s3Store parameter is used for testing purposes. An implicit cannot be used as Spark will try to
     serialize it and fail. Furthermore, mocking singleton objects in Scala is non trivial. */
  def apply(dataset: String, s3Store: => AbstractS3Store = S3Store): Dataset = {
    implicit val formats = DefaultFormats

    val metaBucket = "net-mozaws-prod-us-west-2-pipeline-metadata"
    val metaSources = parse(Source.fromInputStream(s3Store.getKey(metaBucket, "sources.json")).mkString)
    val JString(prefix) = metaSources \\ dataset \\ "prefix"
    val JString(bucketName) = metaSources \\ dataset \\ "bucket"
    val schema = parse(Source.fromInputStream(s3Store.getKey(metaBucket, s"$prefix/schema.json")).mkString)
      .camelizeKeys
      .extract[Schema]

    new Dataset(bucketName, schema, prefix, Map(), s3Store)
  }

  implicit def datasetToRDD(dataset: Dataset)(implicit sc: SparkContext): RDD[Message] = {
    dataset.records()
  }
}
