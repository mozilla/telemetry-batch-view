package telemetry.test

import awscala._
import awscala.s3._
import com.amazonaws.services.lambda.runtime.events.S3Event
import com.amazonaws.services.s3.event.S3EventNotification
import com.amazonaws.services.s3.model.ObjectMetadata
import com.google.protobuf._
import com.typesafe.config._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.System
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory
import org.scalatest.{FlatSpec, Matchers}
import telemetry.ParquetConverter
import telemetry.heka.{Header, HekaFrame, Message}

object Resources {
  val schema = SchemaBuilder
    .record("person")
    .fields
    .name("name").`type`().stringType().noDefault()
    .name("address").`type`().stringType().noDefault()
    .name("ID").`type`().intType().noDefault()
    .endRecord

  val datum = new GenericRecordBuilder(schema)
    .set("name", "rvitillo")
    .set("address", "Floor 3, 101 St Martin's Ln, London WC2N 4AZ")
    .set("ID", 1)
    .build

  val datumJSON = {
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val baos = new ByteArrayOutputStream
    val jsonEncoder = EncoderFactory.get.jsonEncoder(schema, baos)
    writer.write(datum, jsonEncoder)
    jsonEncoder.flush
    baos.toByteArray
  }

  val message = {
    val uuid = ByteString.copyFromUtf8("e967f6a0-7b46-11e5-ac2e-0002a5d5c51b")
    Message(uuid, 0, None, None, None, Some(new String(datumJSON)))
  }

  val header = Header(message.toByteArray.size)

  val hekaFrame = {
    val baos = new ByteArrayOutputStream
    val bHeader = header.toByteArray
    val bMessage = message.toByteArray

    // see https://hekad.readthedocs.org/en/latest/message/index.html
    baos.write(0x1E)
    baos.write(bHeader.size)
    baos.write(bHeader, 0, bHeader.size)
    baos.write(0x1F)
    baos.write(bMessage, 0, bMessage.size)
    baos.toByteArray
  }

  def hekaFile(n: Integer = 42) = {
    val ba = new Array[Byte](n*hekaFrame.size)
    for (i <- 0 to n - 1) System.arraycopy(hekaFrame, 0, ba, i*hekaFrame.size, hekaFrame.size)
    ba
  }
}

class UnitSpec extends FlatSpec with Matchers{
  val nMessages = 42
  val conf = ConfigFactory.load()
  implicit val s3 = S3()

  "A Heka file" can "be parsed" in {
    val messages = HekaFrame.parse(new ByteArrayInputStream(Resources.hekaFile(nMessages)))
    messages.length should be (nMessages)

    val referenceBlob = new String(Resources.datumJSON)
    val jsonBlobs = HekaFrame.jsonBlobs(messages)
    for (blob <- jsonBlobs) blob should be (referenceBlob)
  }

  "A list of Avro records" can "be serialized to a Parquet file" in {
    val messages = HekaFrame.parse(new ByteArrayInputStream(Resources.hekaFile(nMessages)))
    val jsonBlobs = HekaFrame.jsonBlobs(messages)
    val data = ParquetConverter.readData(jsonBlobs, Resources.schema)
    val filename = ParquetConverter.writeParquetFile(data, Resources.schema)
    val readData = ParquetConverter.readParquetFile(filename)
    data should be (readData)
  }

  "A S3 event" should "be correctly handled" in {
    val hekaFile = Resources.hekaFile(nMessages)
    val bucketName = conf.getString("app.bucket")
    val bucket = s3.bucket(bucketName).getOrElse(throw new Exception("Error: bucket doesn't exist"))

    // Create a derived stream with a single Heka file and a schema
    s3.put(bucket, "test_stream/data/sample.heka", hekaFile, new ObjectMetadata())
    s3.put(bucket, "test_stream/schema.json", Resources.schema.toString().getBytes(), new ObjectMetadata())

    // Convert Heka file to Parquet file
    val jsonEvent = """ {"Records": [{"s3": {"object": {"key": "test_stream/data/sample.heka"}, "bucket": {"name": "telemetry-test-bucket"}}}]} """
    val event = S3EventNotification.parseJson(jsonEvent)
    ParquetConverter.transform(new S3Event(event.getRecords()))

    // Check that Parquet file exists
    val parquetPrefix = conf.getString("app.parquetPrefix")
    bucket.get(s"$parquetPrefix/test_stream/data/sample.parquet")
      .getOrElse(throw new Exception("Missing output file"))

    // Check that Heka file was deleted
    bucket.get("test_stream/data/sample.heka") should be (None)
  }
}
