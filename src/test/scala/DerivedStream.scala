package telemetry.test

import com.amazonaws.services.lambda.runtime.events.S3Event
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.event.S3EventNotification
import com.typesafe.config._
import org.scalatest.{FlatSpec, Matchers}
import telemetry.streams.ExampleStream

class DerivedStreamSpec extends FlatSpec with Matchers{
  val conf = ConfigFactory.load()
  val s3Client = new AmazonS3Client

  "A S3 event" should "be correctly handled" in {
    val key = conf.getString("app.testInputKey")
    val bucket = conf.getString("app.testInputBucket")
    val parquetBucket = conf.getString("app.parquetBucket")

    val jsonEvent = """ {"Records": [{"s3": {"object": {"key": "%s"}, "bucket": {"name": "%s"}}}]} """.format(key, bucket)
    val event = S3EventNotification.parseJson(jsonEvent)

    // Convert Heka file to derived Parquet file
    ExampleStream.transform(new S3Event(event.getRecords()))

    // Check that Parquet file was uploaded
    s3Client.getObject(parquetBucket, key)
  }
}
