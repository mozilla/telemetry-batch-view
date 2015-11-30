package telemetry.parquet

import com.typesafe.config._
import java.io.File
import java.rmi.dgc.VMID
import java.util.logging.Logger
import org.apache.avro
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import scala.collection.JavaConverters._

object ParquetFile {
  private val conf = ConfigFactory.load()
  private val limit = 1L << 31
  private val parquetLogger = Logger.getLogger("org.apache.parquet")

  private def temporaryFileName(): String = {
    val vmid = new VMID().toString().replaceAll(":|-", "")
    val tmp = File.createTempFile(vmid, ".tmp")
    tmp.deleteOnExit
    tmp.delete
    tmp.getPath()
  }

  def serialize(data: Iterator[GenericRecord], schema: Schema, chunked: Boolean = false): String = {
    val tmp = temporaryFileName
    val parquetFile = new Path(tmp)
    val stat = new File(tmp)
    val parquetWriter = AvroParquetWriterLike(parquetFile, schema)

    // Disable Parquet logging
    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)

    if (chunked) {
      def loop: Unit = (parquetWriter.getDataSize(), data) match {
        case (s, _) if s > limit => parquetWriter.close
        case (_, d) if d.isEmpty => parquetWriter.close
        case (s, d) => {
          parquetWriter.write(d.next)
          loop
        }
      }
      loop
    } else {
      for (d <- data) parquetWriter.write(d)
      parquetWriter.close
    }

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
