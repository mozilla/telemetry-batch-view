/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.ndjson

import java.time.{LocalDate, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.{ISO_DATE, ISO_DATE_TIME}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JsonAST.{JObject, JString, JInt}
import org.json4s.jackson.JsonMethods.parseOpt

import scala.util.Try
import scalaj.http.Base64

class Dataset private (inputPath: String, clauses: Map[String, PartialFunction[String, Boolean]]) {
  def where(attribute: String)(clause: PartialFunction[String, Boolean]): Dataset = {
    if (clauses.contains(attribute)) {
      throw new Exception(s"There should be only one clause for $attribute")
    }

    if (attribute == "submissionDate") {
      throw new Exception("submissionDate must only be specified to the ndjson.Dataset constructor")
    }

    new Dataset(inputPath, clauses + (attribute -> clause))
  }

  def records()(implicit sc: SparkContext): RDD[Option[JValue]] = {
    // configure sc.textFile(path) to allow * in path
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val messages = sc.textFile(inputPath).flatMap(parseOpt(_))

    val filtered = clauses.foldLeft(messages)((rdd, attributeClause) => {
      val (attribute, clause) = attributeClause
      rdd.filter(_ \ "attributeMap" \ attribute match {
        case JString(value) => clause.isDefinedAt(value) && clause(value)
        case _ => false
      })
    })
    filtered.map(_ \ "payload" match {
      case JString(payload) => Try(Base64.decodeString(payload)).toOption.flatMap(parseOpt(_))
      case _ => None
    // provide doc \ "meta" to better match com.mozilla.telemetry.heka.Message.toJValue
    }).map {
      case Some(doc) =>
        implicit val formats: Formats = DefaultFormats
        val submissionTimestamp = ZonedDateTime.parse(
          // this is generated by the edge server and must be 'an ISO 8601 timestamp with microseconds and timezone "Z"'
          // https://github.com/mozilla/gcp-ingestion/blob/master/docs/edge.md#edge-server-pubsub-message-schema
          (doc \ "submission_timestamp").extract[String],
          ISO_DATE_TIME
        )
        Some(doc ++ JObject(List(
          ("meta", JObject(List(
            ("submissionDate", JString(submissionTimestamp.format(Dataset.DATE_NO_DASH))),
            ("Timestamp", JInt(
              submissionTimestamp.toEpochSecond * 1e9.toLong + submissionTimestamp.getNano.toLong
            )),
            ("documentId", doc \ "document_id"),
            ("clientId", doc \ "clientId"),
            ("sampleId", doc \ "sample_id"),
            ("appUpdateChannel", doc \ "metadata" \ "uri" \ "app_update_channel"),
            ("normalizedChannel", doc \ "normalized_channel"),
            ("normalizedOSVersion", doc \ "normalized_os_version"),
            ("Date", doc \ "metadata" \ "header" \ "date"),
            ("geoCountry", doc \ "metadata" \ "geo" \ "country"),
            ("geoCity", doc \ "metadata" \ "geo" \ "city"),
            ("geoSubdivision1", doc \ "metadata" \ "geo" \ "subdivision1"),
            ("geoSubdivision2", doc \ "metadata" \ "geo" \ "subdivision2")
          )))
        )))
      case _ => None
    }
  }
}

object Dataset {
  val DATE_NO_DASH: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  private var PROD_BUCKET: String = "gs://moz-fx-data-prod-data"
  private var STAGE_BUCKET: String = "gs://moz-fx-data-stage-data"

  private def useTestingPath(path: String): Unit = {
    PROD_BUCKET = s"$path/prod"
    STAGE_BUCKET = s"$path/stage"
  }

  def apply(dataset: String, submissionDate: Option[String] = None): Dataset = {
    // add dashes to submissionDate for backwards compatibility
    val isoSubmissionDate = submissionDate.map(LocalDate.parse(_, DATE_NO_DASH).format(ISO_DATE))

    // look up relevant sink output
    val prefix = dataset match {
      // TODO update telemetry to use PROD_BUCKET when it becomes available
      case "telemetry" => s"$STAGE_BUCKET/telemetry-decoded_gcs-sink/output"
    }

    // TODO add docType to telemetry-decoded output path
    val inputPath = s"$prefix/${isoSubmissionDate.getOrElse("*")}/*/*.ndjson.gz"

    new Dataset(inputPath, Map())
  }

  implicit def datasetToRDD(dataset: Dataset)(implicit sc: SparkContext): RDD[Option[JValue]] = {
    dataset.records()
  }
}
