/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Clock, LocalDate}

import com.mozilla.telemetry.views.BatchJobBase._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

/***
  * Base class for batch processing jobs
  */
abstract class BatchJobBase extends Serializable {

  val clock: Clock = Clock.systemUTC()

  /**
    * Generates list of dates for querying `com.mozilla.telemetry.heka.Dataset`
    * If `to` is empty, uses yesterday as the upper bound.
    *
    * @param from start date, in "yyyyMMdd" format
    * @param to   (optional) end date, in "yyyyMMdd" format
    * @return sequence of dates formatted as "yyyyMMdd" strings
    */
  def datesBetween(from: String, to: Option[String]): Seq[String] = {
    val parsedFrom: LocalDate = LocalDate.parse(from, DateFormatter)
    val parsedTo: LocalDate = to match {
      case Some(t) => LocalDate.parse(t, DateFormatter)
      case _ => LocalDate.now(clock).minusDays(1)
    }
    (0L to ChronoUnit.DAYS.between(parsedFrom, parsedTo)).map { offset =>
      parsedFrom.plusDays(offset).format(DateFormatter)
    }
  }

  private[views] class BaseOpts(args: Array[String]) extends ScallopConf(args) {
    val from: ScallopOption[String] = opt[String](
      "from",
      descr = "Start submission date, defaults to yesterday. Format: YYYYMMDD",
      default=Some(LocalDate.now(clock).minusDays(1).format(DateFormatter)),
      required = false)
    val to: ScallopOption[String] = opt[String](
      "to",
      descr = "End submission date. Default: yesterday. Format: YYYYMMDD",
      required = false)
    val outputBucket = opt[String](
      "bucket",
      descr = "Destination bucket for parquet data",
      required = false)
  }

  protected def shouldStopContextAtEnd(spark: SparkSession): Boolean = {
    !spark.conf.get("spark.home", "").startsWith("/databricks")
  }
}

object BatchJobBase {
  /**
    * Date format for parsing input arguments and formatting partitioning columns
    */
  val DateFormat = "yyyyMMdd"
  val DateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(DateFormat)
}
