package com.mozilla.telemetry

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.google.protobuf._
import com.mozilla.telemetry.heka.{Header, HekaFrame, Message}
import com.mozilla.telemetry.parquet.ParquetFile
import com.typesafe.config._
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FlatSpec, Matchers}

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

class ParquetSpec extends FlatSpec with Matchers{
  val nMessages = 42
  val conf = ConfigFactory.load()

  private def readData(jsonBlobs: Seq[String], schema: Schema) : Seq[GenericRecord] = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val factory = DecoderFactory.get()
    jsonBlobs.map(j => reader.read(null, factory.jsonDecoder(schema, j)))
  }

  "A Heka file" can "be parsed" in {
    val messages = HekaFrame.parse(new ByteArrayInputStream(Resources.hekaFile(nMessages)))
    messages.length should be (nMessages)

    val referenceBlob = new String(Resources.datumJSON)
    val jsonBlobs = messages.map(_.payload).flatten.toList
    for (blob <- jsonBlobs) blob should be (referenceBlob)
  }

  "A list of Avro records" can "be serialized to a Parquet file" in {
    val messages = HekaFrame.parse(new ByteArrayInputStream(Resources.hekaFile(nMessages)))
    val jsonBlobs = messages.map(_.payload).flatten.toList
    val data = readData(jsonBlobs, Resources.schema)
    val filePath = ParquetFile.serialize(data.toIterator, Resources.schema)
    data should be (ParquetFile.deserialize(filePath.toString()))
  }
}
