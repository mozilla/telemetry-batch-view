package telemetry

import awscala.s3._
import com.github.nscala_time.time.Imports._
import com.typesafe.config._
import heka.{HekaFrame, Message}
import java.io.File
import java.util.UUID
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.Days
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import scala.io.Source
import telemetry.parquet.ParquetFile
import telemetry.streams.ExecutiveStream

case class Partitioning(dimensions: List[Dimension]) {
  def partitionPrefix(prefix: String): String = {
    val path = prefix.split("/")
    assert(path.length - 1 == dimensions.length, "Invalid partitioning")

    path(0) + "/" + dimensions
      .zip(path.drop(1))
      .map(x => x._1.fieldName + "S3=" + x._2)
      .mkString("/")
  }
}
case class Dimension(fieldName: String, allowedValues: String)

object Partitioning{
  private implicit val formats = DefaultFormats

  def apply(rawSchema: String): Partitioning = {
    val schema = parse(rawSchema)
    schema.camelizeKeys.extract[Partitioning]
  }
}

trait BatchDerivedStream {
  private implicit val s3 = S3()
  private val conf = ConfigFactory.load()

  private val parquetBucket = conf.getString("app.parquetBucket")
  private val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")

  private val metaPrefix = {
    val Some(sourcesObj) = metadataBucket.get(s"sources.json")
    val sources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(metaPrefix) = sources \\ streamName \\ "metadata_prefix"
    metaPrefix
  }

  private val partitioning = {
    val Some(schemaObj) = metadataBucket.get(s"$metaPrefix/schema.json")
    val schema = Source.fromInputStream(schemaObj.getObjectContent()).getLines().mkString("\n")
    Partitioning(schema)
  }

  private val clsName = camelToDash(this.getClass.getSimpleName.replace("$", ""))  // Use classname as stream prefix on S3

  private def uploadLocalFileToS3(fileName: String, prefix: String) {
    val uuid = UUID.randomUUID.toString
    val key = s"$prefix/$uuid"
    val file = new File(fileName)
    println(s"Uploading Parquet file to $parquetBucket/$key")
    s3.putObject(parquetBucket, key, file)
  }

  private def camelToDash(name: String) = {
    val regexp = "(?<=([a-z]))([A-Z])|^([A-Z])".r
    regexp.replaceAllIn(name, _ match {
                          case regexp(_, _, x) if Option(x).getOrElse("") != "" => x.toLowerCase()
                          case regexp(_, x, _) if Option(x).getOrElse("") != "" => "-" + x.toLowerCase()
                        })
  }

  def buildSchema: Schema
  def buildRecord(message: Message, schema: Schema): Option[GenericRecord]
  def streamName: String

  def hasNotBeenProcessed(prefix: String): Boolean = {
    val bucket = Bucket(parquetBucket)
    val partitionedPrefix = partitioning.partitionPrefix(prefix)
    if (!s3.objectSummaries(bucket, s"$clsName/$partitionedPrefix").isEmpty) {
      println(s"Warning: can't process $prefix as data already exists!")
      false
    } else true
  }

  def transform(bucket: Bucket, keys: Iterator[S3ObjectSummary], prefix: String) {
    val schema = buildSchema
    val records = for {
      key <- keys
      hekaFile = bucket
      .getObject(key.getKey())
      .getOrElse(throw new Exception("File missing on S3"))
      message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey())
      record <- buildRecord(message, schema)
    } yield record

    val partitionedPrefix = partitioning.partitionPrefix(prefix)
    while(!records.isEmpty) {
      val localFile = ParquetFile.serialize(records, schema, chunked=true)
      uploadLocalFileToS3(localFile, s"$clsName/$partitionedPrefix")
      new File(localFile).delete()
    }
  }
}

object BatchConverter {
  type OptionMap = Map[Symbol, String]

  private val metadataBucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")
  private implicit val s3 = S3()

  private def parseOptions(args: Array[String]): OptionMap = {
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--from-date" :: value :: tail =>
          nextOption(map ++ Map('fromDate -> value), tail)
        case "--to-date" :: value :: tail =>
          nextOption(map ++ Map('toDate -> value), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('stream -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('stream -> string), list.tail)
        case option :: tail => Map()
      }
    }

    nextOption(Map(), args.toList)
  }

  private def S3Prefix(logical: String): String = {
    val Some(sourcesObj) = metadataBucket.get(s"sources.json")
    val sources = parse(Source.fromInputStream(sourcesObj.getObjectContent()).getLines().mkString("\n"))
    val JString(prefix) = sources \\ logical \\ "prefix"
    prefix
  }

  private def groupBySize(keys: Iterator[S3ObjectSummary]): List[List[S3ObjectSummary]] = {
    val threshold = 1L << 31
    keys.foldRight((0L, List[List[S3ObjectSummary]]()))(
      (x, acc) => {
        acc match {
          case (size, head :: tail) if size + x.getSize() < threshold =>
            (size + x.getSize(), (x :: head) :: tail)
          case (size, res) if size + x.getSize() < threshold =>
            (size + x.getSize(), List(x) :: res)
          case (_, res) =>
            (x.getSize(), List(x) :: res)
        }
      })._2
  }

  def main(args: Array[String]) {
    val usage = "converter --from-date YYYYMMDD --to-date YYYYMMDD stream_name"
    val options = parseOptions(args)

    if (!List('fromDate, 'toDate, 'stream).forall(options.contains)) {
      println(usage)
      return
    }

    val converter = options('stream) match {
      case "ExecutiveStream" => ExecutiveStream
      case _ => {
        println(usage)
        return
      }
    }

    val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-data")
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = DateTime.parse(options('fromDate), formatter)
    val toDate = DateTime.parse(options('toDate), formatter)
    val daysCount = Days.daysBetween(fromDate, toDate).getDays()
    val prefix = S3Prefix(converter.streamName)

    // FIXME: Parallel collections cause an exception after the first run...
    (0 until daysCount + 1)
      .map(fromDate.plusDays(_).toString("yyyyMMdd"))
      .par
      .foreach(date => {
                 println(s"Fetching data for $date")
                 s3.objectSummaries(bucket, s"$prefix/$date")
                   .groupBy((summary) => {
                              val Some(m) = "(.+)/.+".r.findFirstMatchIn(summary.getKey())
                              m.group(1)
                            })
                   .flatMap(x => groupBySize(x._2.toIterator).toIterator.zip(Iterator.continually{x._1}))
                   .filter(x => converter.hasNotBeenProcessed(x._2))
                   .par
                   .foreach(x => converter.transform(bucket, x._1.toIterator, x._2))
               });
  }
}
