package telemetry

import com.amazonaws.services.lambda.runtime.events.S3Event
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.event.S3EventNotification
import com.typesafe.config._
import heka.{Field, HekaFrame, Message}
import java.io.File
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._

object DerivedStream {
  private val conf = ConfigFactory.load()
  private val s3Client = new AmazonS3Client
  private val parquetBucket = conf.getString("app.parquetBucket")

  private val schema = {
    SchemaBuilder
      .record("System").fields
      .name("memoryMB").`type`().nullable().intType().noDefault()
      .name("cpu").`type`().record("CPU").fields
        .name("count").`type`().nullable().intType().noDefault()
        .name("vendor").`type`().nullable().stringType().noDefault()
        .endRecord()
        .noDefault()
      .endRecord
  }

  private def buildRecord(message: Message, schema: Schema): Option[GenericRecord] ={
    val fields = HekaFrame.fields(message)
    val parsed = fields("environment.system") match {
      case x: String => parse(x)
      case _ => return None
    }

    val cpuRecord = new GenericRecordBuilder(schema.getField("cpu").schema)
      .set("count", parsed \\ "cpu" \\ "count" match {
             case JInt(x) => x
             case _ => None})
      .set("vendor", parsed \\ "cpu" \\ "vendor" match {
             case JString(x) => x
             case _ => None})
      .build

    Some(new GenericRecordBuilder(schema)
      .set("memoryMB", parsed \\ "memoryMB" match {
             case JInt(x) => x
             case _ => None
           })
      .set("cpu", cpuRecord)
      .build)
  }

  private def uploadLocalFileToS3(fileName: String, key: String) {
    val file = new File(fileName)
    s3Client.putObject(parquetBucket, key, file)
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
      println("Creating derived Avro records")
      val records = messages.map((m) => buildRecord(m, schema)).flatten

      // Write Parquet file to S3
      println("Uploading Parquet file to " + s"$parquetBucket/$key")
      val localFile = ParquetFile.serialize(records, schema)
      uploadLocalFileToS3(localFile, key)
    }
  }
}
