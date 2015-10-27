package telemetry

import awscala._
import awscala.s3._
import com.amazonaws.services.lambda.runtime.events.S3Event
import com.amazonaws.services.s3.event.S3EventNotification
import com.typesafe.config._
import heka.HekaFrame
import java.io.File
import java.rmi.dgc.VMID
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DecoderFactory, JsonDecoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import parquet.avro.{AvroParquetReader, AvroParquetWriter}
import parquet.hadoop.ParquetWriter
import parquet.hadoop.metadata.CompressionCodecName
import scala.collection.JavaConverters._

object ParquetConverter {
  val conf = ConfigFactory.load()
  implicit val s3 = S3()

  Logger.getRootLogger().setLevel(Level.OFF)

  def readData(jsonBlobs: Seq[String], schema: Schema) : Seq[GenericRecord] = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val factory = DecoderFactory.get()
    jsonBlobs.map(j => reader.read(null, factory.jsonDecoder(schema, j)))
  }

  def temporaryFileName(): String = {
    val vmid = new VMID().toString().replaceAll(":|-", "")
    val tmp = File.createTempFile(vmid, ".tmp")
    tmp.deleteOnExit
    tmp.delete
    tmp.getPath()
  }

  def writeParquetFile(data: Seq[GenericRecord], schema: Schema): String = {
    val tmp = temporaryFileName
    val parquetFile = new Path(tmp)

    val blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE
    val pageSize = ParquetWriter.DEFAULT_PAGE_SIZE
    val enableDict = ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED
    val conf = new Configuration()
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val parquetWriter = new AvroParquetWriter[GenericRecord](parquetFile, schema, CompressionCodecName.SNAPPY, blockSize, pageSize, enableDict, conf)
    for (d <- data) parquetWriter.write(d)
    parquetWriter.close
    tmp
  }

  def readParquetFile(filename: String): Seq[GenericRecord] = {
    val path = new Path(filename)
    val reader = new AvroParquetReader[GenericRecord](path)

    def loop(l: List[GenericRecord]): List[GenericRecord] = Option(reader.read) match {
      case Some(record) => record :: loop(l)
      case None => l
    }

    loop(List[GenericRecord]())
  }

  def uploadLocalFileToS3(fileName: String, key: String) {
    s3.bucket(conf.getString("app.bucket")) match {
      case Some(bucket) => bucket.put(key.take(1 + key.lastIndexOf(".")) + "parquet", new File(fileName))
      case None => new Exception("Error: failure to upload file to S3")
    }
  }

  def transform(event: S3Event) = {
    val records = event.getRecords.asScala

    for (r <- records) {
      val key = r.getS3.getObject.getKey

      val m = """^([^/]+)/(.+)$""".r.findFirstMatchIn(key).getOrElse(throw new Exception("Invalid key"))
      val prefix = m.group(1)
      val rest = m.group(2)
      val bucket = s3.bucket(r.getS3.getBucket.getName())

      // Fetch schema
      println("Accessing bucket " + r.getS3.getBucket.getName())
      println("Fetching schema " + s"$prefix/schema.json")
      val schema = bucket
        .flatMap((b) => b.get(s"$prefix/schema.json"))
        .map((o) => scala.io.Source.fromInputStream(o.getObjectContent()).mkString)
        .map((s) => new Schema.Parser().parse(s))
        .getOrElse(throw new Exception("Error: schema is missing"))

      // Read Heka file from S3
      println("Fetching Heka file " + key)
      val data = bucket
        .flatMap((b) => b.get(key))
        .map((k) => HekaFrame.parse(k.getObjectContent()))
        .map(HekaFrame.jsonBlobs(_))
        .map(readData(_, schema))
        .getOrElse(throw new Exception("Error: data is missing"))

      // Write Parquet file to S3
      val localFile = writeParquetFile(data, schema)
      val parquetPrefix = conf.getString("app.parquetPrefix")
      println("Writing Parquet file " + s"$parquetPrefix/$prefix/$rest")
      uploadLocalFileToS3(localFile, s"$parquetPrefix/$prefix/$rest")

      // Delete Heka file
      // bucket.get.delete(key)

      key
    }
  }
}
