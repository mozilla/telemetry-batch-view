package telemetry.streams

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import org.json4s.JsonAST.{JInt, JNothing, JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jvalue2monadic
import org.json4s.string2JsonInput

import awscala.s3.Bucket
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.heka.{HekaFrame, Message}
import telemetry.parquet.ParquetFile

case class Churn(prefix: String) extends DerivedStream{
  override def filterPrefix: String = prefix
  override def streamName: String = "telemetry"
  
  // Convert the given Heka message to a map containing just the fields we're interested in.
  def messageToMap(message: Message): Map[String,Any] = {
    val fields = HekaFrame.fields(message)
    // TODO: don't compute any of the expensive stuff if required fields are missing.
    val profile = parse(fields.getOrElse("environment.profile", "{}").asInstanceOf[String])
    val histograms = parse(fields.getOrElse("payload.histograms", "{}").asInstanceOf[String])
    
    val weaveConfigured = booleanHistogramToBoolean(histograms \ "WEAVE_CONFIGURED")
    val weaveDesktop = enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_DESKTOP")
    val weaveMobile = enumHistogramToCount(histograms \ "WEAVE_DEVICE_COUNT_MOBILE")
    
    val map = Map[String, Any](
        "clientId" -> (fields.getOrElse("clientId", None) match {
          case x: String => x
          case _ => None
        }),
        "sampleId" -> (fields.getOrElse("sampleId", None) match {
          case x: Long => x
          case x: Double => x.toLong
          case _ => None
        }),
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
        "submissionDate" -> (fields.getOrElse("submissionDate", None) match {
          case x: String => x
          case _ => None
        }),
        "profileCreationDate" -> ((profile \ "creationDate") match {
          case JNothing => null
          case x: JInt => x.num.toLong
          case _ => null
        }),
        "syncConfigured" -> weaveConfigured.getOrElse(null),
        "syncCountDesktop" -> weaveDesktop.getOrElse(null),
        "syncCountMobile" -> weaveMobile.getOrElse(null),
        "timestamp" -> message.timestamp
      )
    map
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
    val prefix = {
      val JString(prefix) = metaSources \\ streamName \\ "prefix"
      prefix
    }

    // Process each day from fromDate to toDate (inclusive) separately.
    for (i <- 0 to daysCount) {
      val currentDay = fromDate.plusDays(i).toString("yyyyMMdd")
      println("Processing day: " + currentDay)
      val summaries = sc.parallelize(s3.objectSummaries(bucket, s"$prefix/$currentDay/$filterPrefix")
                        .map(summary => ObjectSummary(summary.getKey(), summary.getSize())))

      val groups = DerivedStream.groupBySize(summaries.collect().toIterator)
      val churnMessages = sc.parallelize(groups, groups.size)
        .flatMap(x => x)
        .flatMap{ case obj =>
          val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3: " + obj.key))
          for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message }
        .map{ case message => messageToMap(message) }
        .repartition(100) // TODO: partition by sampleId
        .foreachPartition{ case partitionIterator =>
          val schema = buildSchema
          val records = for {
            record <- partitionIterator
            saveable <- buildRecord(record, schema)
          } yield saveable
  
          while(!records.isEmpty) {
            val localFile = ParquetFile.serialize(records, schema)
            uploadLocalFileToS3(localFile, s"$prefix/submissionDateS3=$currentDay")
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
      .name("profileCreationDate").`type`().nullable().intType().noDefault() // environment/profile/creationDate
      .name("submissionDate").`type`().stringType().noDefault()
      // See bug 1232050
      .name("syncConfigured").`type`().nullable().booleanType().noDefault() // WEAVE_CONFIGURED
      .name("syncCountDesktop").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_DESKTOP
      .name("syncCountMobile").`type`().nullable().intType().noDefault() // WEAVE_DEVICE_COUNT_MOBILE
      
      .name("version").`type`().stringType().noDefault() // appVersion
      .name("timestamp").`type`().longType().noDefault() // server-assigned timestamp when record was received
      .endRecord
  }

  // Check if a json value contains a number greater than zero.
  def gtZero(v: JValue): Boolean = {
    v match {
      case x: JInt => x.num.toInt > 0
      case _ => false
    }
  }

  // Given histogram h, return true if it has a value in the "true" bucket,
  // or false if it has a value in the "false" bucket, or None otherwise.
  def booleanHistogramToBoolean(h: JValue): Option[Boolean] = {
    (gtZero(h \ "values" \ "1"), gtZero(h \ "values" \ "0")) match {
      case (true, _) => Some(true)
      case (_, true) => Some(false)
      case _ => None
    }
  }
  
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }
  
  // Find the largest numeric bucket that contains a value greater than zero.
  def enumHistogramToCount(h: JValue): Option[Long] = {
    (h \ "values") match {
      case JNothing => None
      case JObject(x) => {
        var topBucket = -1
        for {
          (k, v) <- x
          b <- toInt(k) if b > topBucket && gtZero(v)
        } topBucket = b

        if (topBucket >= 0) {
          Some(topBucket)
        } else {
          None
        }
      }
      case _ => {
        None
      }
    }
  }
  
  def buildRecord(fields: Map[String,Any], schema: Schema): Option[GenericRecord] ={
//    println(fields)
    val root = new GenericRecordBuilder(schema)
    try {
      // Required fields, raise an exception if they're not present.
      for (field <- Array("clientId", "sampleId", "submissionDate", "timestamp")) {
        root.set(field, (fields(field) match {
          case None => throw new IllegalArgumentException()
          case x => x
        }))
      }
      
      // String fields that are not nullable. Default to "".
      for (field <- Array("channel", "normalizedChannel", "country", "version")) {
        root.set(field, (fields.get(field) match {
          case Some(x: String) => x
          case _ => ""
        }))
      }
      
      // Everything else is nullable, use 'null' if they're not present.
      for (field <- Array("profileCreationDate", "syncConfigured", "syncCountDesktop", "syncCountMobile")) {
        root.set(field, fields.getOrElse(field, null))
      }
      Some(root.build)
    }
    catch {
      case _: NoSuchElementException => None
      case _: IllegalArgumentException => None
    }
  }
}
