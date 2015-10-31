package telemetry

import com.amazonaws.services.lambda.runtime.events.S3Event
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.event.S3EventNotification
import com.typesafe.config._
import heka.{HekaFrame, Message}
import java.io.File
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.parquet.ParquetFile

trait OnlineDerivedStream {
  private val conf = ConfigFactory.load()
  private val s3Client = new AmazonS3Client
  private val parquetBucket = conf.getString("app.parquetBucket")

  private def uploadLocalFileToS3(fileName: String, key: String) {
    val file = new File(fileName)
    s3Client.putObject(parquetBucket, key, file)
  }

  def buildSchema: Schema
  def buildRecord(message: Message, schema: Schema): Option[GenericRecord]

  def simulateEvent(bucket: String, key: String) = {
    val jsonEvent = """ {"Records": [{"s3": {"object": {"key": "%s"}, "bucket": {"name": "%s"}}}]} """.format(key, bucket)
    val event = S3EventNotification.parseJson(jsonEvent)
    transform(new S3Event(event.getRecords()))
  }

  def transform(event: S3Event) = {
    val records = event.getRecords.asScala

    for (r <- records) {
      val key = r.getS3.getObject.getKey
      val bucket = r.getS3.getBucket.getName()

      // Read Heka file from S3
      println("Fetching Heka file " + key)
      val hekaFile = s3Client.getObject(bucket, key).getObjectContent()
      val messages = HekaFrame.parse(hekaFile)

      // Create derived stream
      val schema = buildSchema
      println("Creating derived Avro records")
      val records = messages.map((m) => buildRecord(m, schema)).flatten

      // Write Parquet file to S3
      val clsName = this.getClass.getSimpleName.replace("$", "")  // Use classname as stream prefix on S3
      println("Uploading Parquet file to " + s"$parquetBucket/$clsName/$key")
      val localFile = ParquetFile.serialize(records, schema)
      uploadLocalFileToS3(localFile, s"$clsName/$key")
    }
  }
}
