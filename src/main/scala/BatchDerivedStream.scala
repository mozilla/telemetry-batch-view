package telemetry

import awscala.s3._
import com.github.nscala_time.time.Imports._
import com.typesafe.config._
import heka.{HekaFrame, Message}
import java.io.File
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.Days
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.parquet.ParquetFile
import telemetry.streams.ExecutiveStream

trait BatchDerivedStream {
  private val conf = ConfigFactory.load()
  private val parquetBucket = conf.getString("app.parquetBucket")
  private implicit val s3 = S3()

  private def uploadLocalFileToS3(fileName: String, key: String) {
    val file = new File(fileName)
    s3.putObject(parquetBucket, key, file)
  }

  def buildSchema: Schema
  def buildRecord(message: Message, schema: Schema): Option[GenericRecord]
  def streamName: String

  def transform(bucket: Bucket, keys: Iterator[S3ObjectSummary], prefix: String) = {
    val schema = buildSchema

    val records = for {
      key <- keys
      hekaFile = bucket
      .getObject(key.getKey())
      .getOrElse(throw new Exception("File missing on S3"))
      .getObjectContent()
      message <- HekaFrame.parse(hekaFile)
      record <- buildRecord(message, schema)
    } yield record

    val clsName = this.getClass.getSimpleName.replace("$", "")  // Use classname as stream prefix on S3
    var filesWritten = 0

    while(!records.isEmpty) {
      val destKey = s"$clsName/$prefix/$filesWritten"
      println("Uploading Parquet file to " + s"$parquetBucket/$destKey")

      val localFile = ParquetFile.serialize(records, schema, chunked=true)
      uploadLocalFileToS3(localFile, destKey)
      new File(localFile).delete()
      filesWritten += 1
    }
  }
}

object BatchConverter {
  type OptionMap = Map[Symbol, String]
  implicit val s3 = S3()

  def parseOptions(args: Array[String]): OptionMap = {
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

  def S3streamName(logical: String): String = {
    val bucket = Bucket("net-mozaws-prod-us-west-2-pipeline-metadata")
    val Some(schemaObj) = bucket.get(s"sources.json")
    val schema = parse(scala.io.Source.fromInputStream(schemaObj.getObjectContent()).getLines().mkString("\n"))
    val JString(prefix) = schema \\ logical \\ "prefix"
    prefix
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
    val prefix = S3streamName(converter.streamName)

    // FIXME: Parallel collections cause an exception after the first run...
    (0 until daysCount + 1)
      .map(fromDate.plusDays(_).toString("yyyyMMdd"))
      .par
      .foreach((date) => {
                 println(s"Fetching data for $date")
                 s3.objectSummaries(bucket, s"$prefix/$date")
                   .groupBy((summary) => {
                              val Some(m) = "(.+)/.+".r.findFirstMatchIn(summary.getKey())
                              m.group(1)
                            })
                   .par
                   .foreach((group) => {
                              converter.transform(bucket, group._2.toIterator, group._1)
                            })

               });
  }
}
