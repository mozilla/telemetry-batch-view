/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import java.net.URI
import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.zip.CRC32

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

package object utils{
  import java.rmi.dgc.VMID
  import org.apache.hadoop.fs.Path

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

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
  private val secondsPerHour = 60 * 60
  private val millisPerDay = millisPerHour * 24
  private val dateFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
  private val uncamelPattern = java.util.regex.Pattern.compile("(^[^A-Z]+|[A-Z][^A-Z]+)")

  def camelize(name: String): String = {
    specialCases.getOrElse(name, {
      val split = name.split("_")
      val rest = split.drop(1).map(_.capitalize).mkString
      split(0).mkString + rest
    })
  }

  def uncamelize(name: String): String = {
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

  def getOrCreateSparkSession(jobName: String, enableHiveSupport: Boolean = false): SparkSession = {
    val conf = new SparkConf().setAppName(jobName)
    conf.setMaster(conf.get("spark.master", "local[*]"))

    val spark = enableHiveSupport match {
      case true => {
        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }
      case false => {
        SparkSession
          .builder()
          .config(conf)
          .getOrCreate()
      }
    }

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("parquet.enable.summary-metadata", "false")
    spark
  }

  def normalizeISOTimestamp(timestamp: String): String = {
    // certain date parsers, notably Presto's, have a hard time with certain date edge cases,
    // especially time zone offsets that are not between -12 and 14 hours inclusive (see bug 1250894)
    // for these time zones, we're going to use some hacky arithmetic to bring them into range;
    // they will still represent the same moment in time, just with a correct time zone
    // we're going to output it in standard ISO format
    val date = OffsetDateTime.parse(timestamp, dateFormatter)
    val timezoneOffsetHours = date.getOffset().getTotalSeconds.toDouble / secondsPerHour
    def fixTimezone(i: Int) =
      ZoneOffset.ofTotalSeconds(
      ((timezoneOffsetHours + (i * 12) * Math.floor(timezoneOffsetHours / (-i * 12)).toInt) * secondsPerHour).toInt)

    val timezone = if (timezoneOffsetHours < -12.0) {
      fixTimezone(1)
    } else if (timezoneOffsetHours > 14.0) {
      fixTimezone(-1)
    } else {
      date.getOffset
    }
    date.format(dateFormatter.withZone(timezone))
  }

  def normalizeYYYYMMDDTimestamp(YYYYMMDD: String): String = {
    val format = DateTimeFormatter.ofPattern("yyyyMMdd")
    LocalDate.parse(YYYYMMDD, format).atStartOfDay(ZoneOffset.UTC).format(dateFormatter)
  }

  def normalizeEpochTimestamp(timestamp: BigInt): String = {
    Instant.ofEpochMilli(timestamp.toLong * millisPerDay).atOffset(ZoneOffset.UTC).format(dateFormatter)
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
    logger.info(s"Elapsed time: ${(t1 - t0)/1000000000.0} s")
    result
  }

  def yesterdayAsYYYYMMDD: String = {
    Instant.now().atOffset(ZoneOffset.UTC).minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"))
  }

  def deletePrefix(bucket: String, prefix: String): Unit = {
    if (!S3Store.isPrefixEmpty(bucket, prefix)) {
      S3Store.listKeys(bucket, prefix).foreach(o => S3Store.deleteKey(bucket, o.key))
    }
  }

  // Generate a deterministic block ID from a string -- e.g. sample_id from client_id
  def blockIdFromString(numBlocks: Long)(inString: String): Integer = {
    val crc = new CRC32
    crc.update(inString.getBytes)
    (crc.getValue % numBlocks).toInt
  }

  def writeTextFile(path: String, body: String): Unit = {
    val file = FileSystem
      .get(new URI(path), new Configuration())
      .create(new Path(path))
    file.write(body.getBytes)
    file.close()
  }

  /** Detect existence of a directory or file using hadoop
    *
    * @param pathString path to the directory or file to detect using hadoop
    */
  def hadoopExists(pathString: String): Boolean = {
    val path = new Path(pathString)
    FileSystem.get(path.toUri, new Configuration()).exists(path)
  }
}
