package telemetry

import java.net.URLClassLoader
import java.io.File
import java.net.URL
import java.lang.ClassLoader
import java.lang.reflect.Field
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.hadoop.fs.Path
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.avro.{Schema, SchemaBuilder}

// http://stackoverflow.com/questions/5445511/how-do-i-create-a-parent-last-child-first-classloader-in-java-or-how-to-overr
class SelfFirstLoader(classpath: Array[URL], parent: ClassLoader) extends URLClassLoader(classpath, parent) {
  @throws(classOf[ClassNotFoundException])
  override final def loadClass(className: String, resolve: Boolean): Class[_] = {
    val loaded = findLoadedClass(className)

    val found =
      if(loaded == null) {
        try {
          findClass(className)
        } catch {
          case _: ClassNotFoundException =>
            super.loadClass(className, false)
        }
      } else loaded

    if (resolve)
      resolveClass(found)

    found
  }
}

object S3ClassLoader {
  def main(args: Array[String]) {

    def getListOfFiles(dir: File): Array[URL] = {
      val these = dir.listFiles
      these.map(_.toURL) ++ these.filter(_.isDirectory).flatMap(getListOfFiles)
    }

    val files = getListOfFiles(new File("/Users/vitillo/sandbox/aws-lambda-parquet/lib_managed/jars/org.apache.parquet/")) ++
      getListOfFiles(new File("/Users/vitillo/sandbox/aws-lambda-parquet/lib_managed/jars/org.apache.parquet/"))

    //var classLoader = new URLClassLoader(
    var classLoader = new SelfFirstLoader(
      files,
      /*
       * need to specify parent, so we have all class instances
       * in current context
       */
      this.getClass.getClassLoader)

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

      val hadoopConf = new Configuration()
      val blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE
      val pageSize = ParquetWriter.DEFAULT_PAGE_SIZE
      val enableDict = ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED

      // TODO: delete, just to check if class loader loads the new class anyway
      val foobar = new AvroParquetWriter[GenericRecord](parquetFile, schema, CompressionCodecName.SNAPPY, blockSize, pageSize, enableDict, hadoopConf)
      stat.delete()

      // As Spark 1.5 is using an older version of AvroParquetWriter, we have to ensure we are loading one that supports getDataSize
      val codec = classLoader.loadClass("org.apache.parquet.hadoop.metadata.CompressionCodecName")
      var cls = classLoader.loadClass("org.apache.parquet.avro.AvroParquetWriter")
      val snappy  = codec.getEnumConstants().find(_.toString.equals("SNAPPY")).get


      val ctor = cls.getConstructor(classOf[Path], classOf[Schema], codec, classOf[Int], classOf[Int], classOf[Boolean], classOf[Configuration])
      val parquetWriter = ctor.newInstance(parquetFile, schema, snappy, Int.box(blockSize), Int.box(pageSize), Boolean.box(enableDict), hadoopConf)
      val getDataSize = cls.getMethod("getDataSize")
      val close = cls.getMethod("close")

      println("SIZE:")
      val size = getDataSize.invoke(parquetWriter).asInstanceOf[Long]
      println(size)
      close.invoke(parquetWriter)

      ""
    }

    serialize(Iterator[GenericRecord](), schema)

    //files.foreach(print(_))
    //var AvroParquetReader = classLoader.loadClass("org.apache.parquet.avro.AvroParquetReader")
    // var foo = classLoader.loadClass("telemetry.Foo")
    // val ctor = foo.getConstructor()
    // val gne = ctor.newInstance()

    // http://stackoverflow.com/questions/9691855/java-example-with-classloader
  }
}
