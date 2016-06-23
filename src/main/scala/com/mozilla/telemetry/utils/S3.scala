package com.mozilla.telemetry.utils

import java.io.InputStream
import awscala.s3.{Bucket, S3}
import scala.collection.JavaConverters._

// key is the S3 filename, size is the object size in bytes.
case class ObjectSummary(key: String, size: Long) // S3ObjectSummary can't be serialized

object ObjectSummary {
  def groupBySize(keys: Iterator[ObjectSummary]): List[List[ObjectSummary]] = {
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

abstract class AbstractS3Store {
  def listKeys(bucket: String, prefix: String): Stream[ObjectSummary]
  def listFolders(bucket: String, prefix: String, delimiter: String = "/"): Stream[String]
  def getKey(bucket: String, key: String): InputStream
}

object S3Store extends AbstractS3Store {
  protected implicit lazy val s3: S3 = S3()

  def getKey(bucket: String, key: String): InputStream = {
    Bucket(bucket).getObject(key).getOrElse(throw new Exception(s"File missing on S3: $key")).getObjectContent
  }

  def listKeys(bucket: String, prefix: String): Stream[ObjectSummary] = {
    s3.objectSummaries(Bucket(bucket), prefix)
      .map(summary => ObjectSummary(summary.getKey, summary.getSize))
  }

  def listFolders(bucket: String, prefix: String, delimiter: String = "/"): Stream[String] = {
    import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

    val request = new ListObjectsRequest().
      withBucketName(bucket).
      withPrefix(prefix).
      withDelimiter(delimiter)
    val firstListing = s3.listObjects(request)

    def completeStream(listing: ObjectListing): Stream[String] = {
      val prefixes = listing.getCommonPrefixes.asScala.toStream
      prefixes #:::
        (if (listing.isTruncated)
          completeStream(s3.listNextBatchOfObjects(listing))
        else
          Stream.empty)
    }

    completeStream(firstListing)
  }
}
