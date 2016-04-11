package telemetry.streams

import awscala.s3.Bucket
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import org.json4s.JsonAST.{JInt, JNothing, JObject, JString, JValue, JBool}
import org.json4s.jackson.JsonMethods.parse
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.heka.{HekaFrame, Message}
import telemetry.parquet.ParquetFile
import telemetry.streams.main_summary.Utils

case class Churn(prefix: String) extends DerivedStream{
  override def filterPrefix: String = prefix
  override def streamName: String = "telemetry"
  def streamVersion: String = "v1"

  // Convert the given Heka message to a map containing just the fields we're interested in.
  def messageToMap(message: Message): Option[Map[String,Any]] = {
    val fields = HekaFrame.fields(message)

    // Don't compute the expensive stuff until we need it. We may skip a record
    // due to missing simple fields.
    lazy val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
    lazy val partner = parse(fields.getOrElse("environment.partner", "{}").asInstanceOf[String])
    lazy val settings = parse(fields.getOrElse("environment.settings", "{}").asInstanceOf[String])
    lazy val info = parse(fields.getOrElse("payload.info", "{}").asInstanceOf[String])
    lazy val histograms = parse(fields.getOrElse("payload.histograms", "{}").asInstanceOf[String])

    lazy val weaveConfigured = Utils.booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
    lazy val weaveDesktop = Utils.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
    lazy val weaveMobile = Utils.enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")

    val map = Map[String, Any](
        "clientId" -> (fields.getOrElse("clientId", None) match {
          case x: String => x
          // clientId is required, and must be a string. If either
          // condition is not satisfied, we skip this record.
          case _ => return None
        }),
        "sampleId" -> (fields.getOrElse("sampleId", None) match {
          case x: Long => x
          case x: Double => x.toLong
          case _ => return None // required
        }),
        "submissionDate" -> (fields.getOrElse("submissionDate", None) match {
          case x: String => x
          case _ => return None // required
        }),
        "timestamp" -> message.timestamp, // required
        "channel" -> (fields.getOrElse("appUpdateChannel", None) match {
          case x: String => x
          case _ => ""
        }),
        "normalizedChannel" -> (fields.getOrElse("normalizedChannel", None) match {
          case x: String => x
          case _ => ""
        }),
        "country" -> (fields.getOrElse("geoCountry", None) match {
          case x: String => x
          case _ => ""
        }),
        "version" -> (fields.getOrElse("appVersion", None) match {
          case x: String => x
          case _ => ""
        }),
        "profileCreationDate" -> ((profile \ "creationDate") match {
          case x: JInt => x.num.toLong
          case _ => null
        }),
        "syncConfigured" -> weaveConfigured.getOrElse(null),
        "syncCountDesktop" -> weaveDesktop.getOrElse(null),
        "syncCountMobile" -> weaveMobile.getOrElse(null),
        "subsessionStartDate" -> ((info \ "subsessionStartDate") match {
          case JString(x) => x
          case _ => null
        }),
        "subsessionLength" -> ((info \ "subsessionLength") match {
          case x: JInt => x.num.toLong
          case _ => null
        }),
        "distributionId" -> ((partner \ "distributionId") match {
          case JString(x) => x
          case _ => null
        }),
        "e10sEnabled" -> ((settings \ "e10sEnabled") match {
          case JBool(x) => x
          case _ => null
        }),
        "e10sCohort" -> ((settings \ "e10sCohort") match {
          case JString(x) => x
          case _ => null
        })
      )
    Some(map)
  }

  override def transform(sc: SparkContext, bucket: Bucket, ignoreMe: RDD[ObjectSummary], from: String, to: String) {
    // Iterate by day, ignoring the passed-in s3objects
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val fromDate = formatter.parseDateTime(from)
    val toDate = formatter.parseDateTime(to)
    val daysCount = Days.daysBetween(fromDate, toDate).getDays()
    val bucket = {
      val JString(bucketName) = metaSources \\ streamName \\ "bucket"
      Bucket(bucketName)
    }
    val dataPrefix = {
      val JString(prefix) = metaSources \\ streamName \\ "prefix"
      prefix
    }

    // Process each day from fromDate to toDate (inclusive) separately.
    for (i <- 0 to daysCount) {
      val currentDay = fromDate.plusDays(i).toString("yyyyMMdd")
      println("Processing day: " + currentDay)
      val summaries = sc.parallelize(s3.objectSummaries(bucket, s"$dataPrefix/$currentDay/$filterPrefix")
                        .map(summary => ObjectSummary(summary.getKey(), summary.getSize())))

      val groups = DerivedStream.groupBySize(summaries.collect().toIterator)
      val churnMessages = sc.parallelize(groups, groups.size)
        .flatMap(x => x)
        .flatMap{ case obj =>
          val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3: " + obj.key))
          for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message }
        .flatMap{ case message => messageToMap(message) }
        .repartition(100) // TODO: partition by sampleId
        .foreachPartition{ case partitionIterator =>
          val schema = buildSchema
          val records = for {
            record <- partitionIterator
            saveable <- buildRecord(record, schema)
          } yield saveable

          while(!records.isEmpty) {
            val localFile = ParquetFile.serialize(records, schema)
            uploadLocalFileToS3(localFile, s"$streamVersion/submission_date_s3=$currentDay")
          }
        }
    }
  }

  private def buildSchema: Schema = {
    SchemaBuilder
      .record("System").fields
      .name("clientId").`type`().stringType().noDefault()
      .name("sampleId").`type`().intType().noDefault()
      .name("channel").`type`().stringType().noDefault() // appUpdateChannel
      .name("normalizedChannel").`type`().stringType().noDefault() // normalizedChannel
      .name("country").`type`().stringType().noDefault() // geoCountry
      // TODO: use proper 'date' type for date columns.
      .name("profileCreationDate").`type`().nullable().intType().noDefault() // environment/profile/creationDate
      .name("subsessionStartDate").`type`().nullable().stringType().noDefault() // info/subsessionStartDate
      .name("subsessionLength").`type`().nullable().intType().noDefault() // info/subsessionLength
      .name("distributionId").`type`().nullable().stringType().noDefault() // environment/partner/distributionId
      .name("submissionDate").`type`().stringType().noDefault()
      // See bug 1232050
      .name("syncConfigured").`type`().nullable().booleanType().noDefault() // WEAVE_CONFIGURED
      .name("syncCountDesktop").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_DESKTOP
      .name("syncCountMobile").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_MOBILE

      .name("version").`type`().stringType().noDefault() // appVersion
      .name("timestamp").`type`().longType().noDefault() // server-assigned timestamp when record was received

      // See bug 1251259
      .name("e10sEnabled").`type`().nullable.booleanType().noDefault() // environment/settings/e10sEnabled
      .name("e10sCohort").`type`().nullable.stringType().noDefault() // environment/settings/e10sCohort

      .endRecord
  }

  def buildRecord(fields: Map[String,Any], schema: Schema): Option[GenericRecord] ={
    val root = new GenericRecordBuilder(schema)
    for ((k, v) <- fields) root.set(k, v)
    Some(root.build)
  }
}
