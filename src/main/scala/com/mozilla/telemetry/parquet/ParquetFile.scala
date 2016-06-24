package com.mozilla.telemetry.parquet

import java.util.logging.Logger
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

object ParquetFile {
  private val parquetLogger = Logger.getLogger("org.apache.parquet")
  private val hadoopConf = new Configuration()

  hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  // Note: clients shouldn't assume that serialize consumes the entirety of the iterator as the behaviour might
  // change in the future.
  def serialize(data: Iterator[GenericRecord], schema: Schema, blockSizeMultiplier: Int = 1): Path = {
    val blockSize = blockSizeMultiplier*ParquetWriter.DEFAULT_BLOCK_SIZE
    val pageSize = ParquetWriter.DEFAULT_PAGE_SIZE
    val enableDict = ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED
    val parquetFile = com.mozilla.telemetry.utils.temporaryFileName()
    val parquetWriter = new AvroParquetWriter[GenericRecord](parquetFile, schema, CompressionCodecName.SNAPPY, blockSize, pageSize, enableDict, hadoopConf)

    // Disable Parquet logging
    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)

    for (d <- data) {
      parquetWriter.write(d)
    }

    parquetWriter.close()
    parquetFile
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
