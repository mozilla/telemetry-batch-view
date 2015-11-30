package telemetry

import java.net.URLClassLoader
import java.io.File
import java.net.URL
import java.lang.ClassLoader
import java.lang.reflect.Field
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.avro.{Schema, SchemaBuilder}
import com.typesafe.config._
import telemetry.parquet.AvroParquetWriterLike

object S3ClassLoader {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load()
    val path = conf.getString("app.jarDirectory")
    val classLoader = SelfFirstClassLoader(path)

    val schema = SchemaBuilder
      .record("person")
      .fields
      .name("name").`type`().stringType().noDefault()
      .name("address").`type`().stringType().noDefault()
      .name("ID").`type`().intType().noDefault()
      .endRecord


    def serialize(data: Iterator[GenericRecord], schema: Schema, chunked: Boolean = false): String = {
      val tmp = "/Users/vitillo/Downloads/FOOTEST"
      val parquetFile = new Path(tmp)
      val stat = new File(tmp)
      stat.delete()

      val parquetWriter = AvroParquetWriterLike(parquetFile, schema)
      println("SIZE:")
      val size = parquetWriter.getDataSize()
      println(size)
      parquetWriter.close()
      ""
    }

    serialize(Iterator[GenericRecord](), schema)
  }
}
