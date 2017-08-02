package com.mozilla.telemetry

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object utils{
  import java.rmi.dgc.VMID
  import org.apache.hadoop.fs.Path
  import org.joda.time._

  private val specialCases = Map(
    "submission_url" -> "submissionURL",
    "memory_mb" -> "memoryMB",
    "windows_ubr" -> "windowsUBR",
    "virtual_max_mb" -> "virtualMaxMB",
    "l2cache_kb" -> "l2cacheKB",
    "l3cache_kb" -> "l3cacheKB",
    "speed_mhz" -> "speedMHz",
    "d2d_enabled" -> "D2DEnabled",
    "d_write_enabled" -> "DWriteEnabled",
    "vendor_id" -> "vendorID",
    "device_id" -> "deviceID",
    "subsys_id" -> "subsysID",
    "ram" -> "RAM",
    "gpu_active" -> "GPUActive",
    "first_load_uri" -> "firstLoadURI",
    "" -> "")

  private val millisPerHour = 60 * 60 * 1000
  private val millisPerDay = millisPerHour * 24
  private val dateFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()
  private val uncamelPattern = java.util.regex.Pattern.compile("(^[^A-Z]+|[A-Z][^A-Z]+)")

  def camelize(name: String) = {
    specialCases.getOrElse(name, {
      val split = name.split("_")
      val rest = split.drop(1).map(_.capitalize).mkString
      split(0).mkString + rest
    })
  }

  def uncamelize(name: String) = {
    val matcher = uncamelPattern.matcher(name)
    val output = new StringBuilder

    while (matcher.find()) {
      if (output.nonEmpty) {
        output.append("_")
      }
      output.append(matcher.group().toLowerCase)
    }

    output.toString()
  }

  def getOrCreateSparkSession(jobName: String): SparkSession = {
    val conf = new SparkConf().setAppName(jobName)
    conf.setMaster(conf.get("spark.master", "local[*]"))

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("parquet.enable.summary-metadata", "false")
    spark
  }

  def normalizeISOTimestamp(timestamp: String) = {
    // certain date parsers, notably Presto's, have a hard time with certain date edge cases,
    // especially time zone offsets that are not between -12 and 14 hours inclusive (see bug 1250894)
    // for these time zones, we're going to use some hacky arithmetic to bring them into range;
    // they will still represent the same moment in time, just with a correct time zone
    // we're going to use the relatively lenient joda-time parser and output it in standard ISO format
    val date = dateFormatter.withOffsetParsed().parseDateTime(timestamp)
    val timezoneOffsetHours = date.getZone.getOffset(date).toDouble / millisPerHour
    def fixTimezone(i: Int) = org.joda.time.DateTimeZone.forOffsetMillis(((timezoneOffsetHours + (i * 12) * Math.floor(timezoneOffsetHours / (-i * 12)).toInt) * millisPerHour).toInt)
    val timezone = if (timezoneOffsetHours < -12.0) {
      fixTimezone(1)
    } else if (timezoneOffsetHours > 14.0) {
      fixTimezone(-1)
    } else {
      date.getZone
    }
    dateFormatter.withZone(timezone).print(date)
  }

  def normalizeYYYYMMDDTimestamp(YYYYMMDD: String) = {
    dateFormatter.withZone(org.joda.time.DateTimeZone.UTC).print(
      format.DateTimeFormat.forPattern("yyyyMMdd")
        .withZone(org.joda.time.DateTimeZone.UTC)
        .parseDateTime(YYYYMMDD.asInstanceOf[String]))
  }

  def normalizeEpochTimestamp(timestamp: BigInt) = {
    dateFormatter.withZone(org.joda.time.DateTimeZone.UTC).print(new DateTime(timestamp.toLong * millisPerDay))
  }

  def temporaryFileName(): Path = {
    val vmid = new VMID().toString.replaceAll(":|-", "")
    val fileURI = java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"), s"$vmid.tmp").toUri
    new Path(fileURI)
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block  // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time: ${(t1 - t0)/1000000000.0} s")
    result
  }

  def yesterdayAsYYYYMMDD: String = {
    DateTime.now.minusDays(1).toString("yyyyMMdd")
  }
}
