package telemetry

import com.typesafe.config._
import java.io.File
import java.rmi.dgc.VMID
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import parquet.avro.{AvroParquetReader, AvroParquetWriter}
import parquet.hadoop.ParquetWriter
import parquet.hadoop.metadata.CompressionCodecName
import scala.collection.JavaConverters._

object ParquetFile {
  private val conf = ConfigFactory.load()
  Logger.getRootLogger().setLevel(Level.OFF)

  private def temporaryFileName(): String = {
    val vmid = new VMID().toString().replaceAll(":|-", "")
    val tmp = File.createTempFile(vmid, ".tmp")
    tmp.deleteOnExit
    tmp.delete
    tmp.getPath()
  }

  def serialize(data: Iterator[GenericRecord], schema: Schema): String = {
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

  def deserialize(filename: String): Seq[GenericRecord] = {
    val path = new Path(filename)
    val reader = new AvroParquetReader[GenericRecord](path)

    def loop(l: List[GenericRecord]): List[GenericRecord] = Option(reader.read) match {
      case Some(record) => record :: loop(l)
      case None => l
    }

    loop(List[GenericRecord]())
  }
}
