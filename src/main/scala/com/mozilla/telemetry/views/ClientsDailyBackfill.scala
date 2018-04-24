package com.mozilla.telemetry.views

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS

import com.mozilla.telemetry.utils.writeTextFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.AnalysisException
import org.rogach.scallop._

object ClientsDailyBackfill {
  class Conf(args: Array[String]) extends ScallopConf(args) {
    val start = opt[String](
      "from",
      descr = "First submission date to process",
      required = true)
    val end = opt[String](
      "to",
      descr = "Last submission date to process",
      required = true)
    verify()
  }

  private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val start = LocalDate.parse(conf.start(), fmt)
    val end = LocalDate.parse(conf.end(), fmt)

    val numDays = DAYS.between(start, end).toInt
    val step = if (numDays > 0) {1} else {-1}

    (0 to numDays by step).foreach {offset =>
      val date = start.plusDays(offset).format(fmt)
      try {
        val (dirNames, fileNames) = ls(s"s3://telemetry-backfill/clients_daily/v6/submission_date_s3=$date/")
        if (!fileNames.contains("_SUCCESS")) {
          ClientsDailyView.main(Array(
            "--date", date,
            "--output-bucket", "telemetry-backfill"
          ))
        }
      } catch {
        case e: AnalysisException => println(e)
          writeTextFile(s"s3://telemetry-backfill/clients_daily/v6_exception/submission_date_s3=$date", e.toString)
      }
    }
  }

  def ls(directory: String): (List[String], List[String]) = ls(new Path(directory))
  def ls(directory: Path): (List[String], List[String]) = {
    val fs = FileSystem.get(directory.toUri, new Configuration())
    if (fs.exists(directory)) {
      val listing = fs.listStatus(directory).toList
      val dirNames = listing.collect {
        case status if status.isDirectory => status.getPath.getName
      }
      val fileNames = listing.collect {
        case status if status.isFile => status.getPath.getName
      }
      (dirNames, fileNames)
    } else {
      (List(), List())
    }
  }
}
