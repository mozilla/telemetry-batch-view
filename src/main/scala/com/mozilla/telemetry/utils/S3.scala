package com.mozilla.telemetry.utils

import java.io.InputStream
import awscala.s3.{Bucket, S3}
import com.mozilla.telemetry.ObjectSummary
import scala.collection.JavaConverters._

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
