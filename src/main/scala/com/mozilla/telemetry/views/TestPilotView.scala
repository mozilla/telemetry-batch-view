package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.Message
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JInt, JArray, JString, JValue}
import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods.parse

object TestPilotView extends TelemetryView{
  def schemaVersion: String = "v1"
  def jobName: String = "test_pilot_pings"
  def docType: String = "testpilot"

  private def eventsToLists(events: List[JValue]): Option[List[List[Any]]] = {
    val buf = scala.collection.mutable.ListBuffer.empty[List[Any]]
    for (event <- events) {
      buf.append(List[Any](
        event \ "timestamp" match {
          case JInt(x) => x.toLong
          case _ => null
        },
        event \ "event" match {
          case JString(x) => x
          case _ => null
        },
        event \ "object" match {
          case JString(x) => x
          case _ => null
        }
      ))
    }
    if (buf.isEmpty) None
    else Some(buf.toList)
  }

  def messageToRows(message: Message): Option[List[Row]] = {
    val fields = message.fieldsAsMap

    lazy val payload = parse(message.payload.getOrElse("{}").asInstanceOf[String])
    lazy val application = payload \ "application"
    lazy val inner_payload = payload \ "payload"
    lazy val system = parse(fields.getOrElse("environment.system", "{}").asInstanceOf[String])

    // If there aren't any events, short-circuit and return (probably old-style txp ping)
    val events = (inner_payload \ "events") match {
      case JArray(x) => eventsToLists(x)
      case _ => return None
    }


    val common = List[Any] (
      // Row fields must match the structure in 'buildSchema'
      fields.getOrElse("documentId", None) match {
        case x: String => x
        // documentId is required, and must be a string. If either
        // condition is not satisfied, we skip this record.
        case _ => null
      },
      fields.getOrElse("clientId", None) match {
        case x: String => x
        case _ => null
      },
      fields.getOrElse("normalizedChannel", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("geoCountry", None) match {
        case x: String => x
        case _ => ""
      },
      fields.getOrElse("geoCity", None) match {
        case x: String => x
        case _ => ""
      },
      system \ "os" \ "name" match {
        case JString(x) => x
        case _ => null
      },
      system \ "os" \ "version" match {
        case JString(x) => x
        case _ => null
      },
      fields.getOrElse("submissionDate", None) match {
        case x: String => x
        case _ => null // required
      },
      application \ "name" match {
        case JString(x) => x
        case _ => null
      },
      application \ "version" match {
        case JString(x) => x
        case _ => null
      },
      message.timestamp, // required
      fields.getOrElse("Date", None) match {
        case x: String => x
        case _ => null
      },
      inner_payload \ "version" match {
        case JString(x) => x
        case _ => null
      },
      inner_payload \ "test" match {
        case JString(x) => x
        case _ => null
      }
    )
    val rows = events match {
      case Some(x) => for (event <- x) yield Row.fromSeq(common ++ event)
      case _ => null
    }
    Some(rows)
  }

  def buildSchema: StructType = {
    StructType(List(
      StructField("document_id", StringType, nullable = false), // id
      StructField("client_id", StringType, nullable = true), // clientId
      StructField("normalized_channel", StringType, nullable = true), // normalizedChannel
      StructField("country", StringType, nullable = true), // geoCountry
      StructField("city", StringType, nullable = true), // geoCity
      StructField("os", StringType, nullable = true), // environment/system/os/name
      StructField("os_version", StringType, nullable = true), // environment/system/os/version
      StructField("submission_date", StringType, nullable = false), // YYYYMMDD version of 'timestamp'
      StructField("app_name", StringType, nullable = true), // application/name
      StructField("app_version", StringType, nullable = true), // application/version
      StructField("timestamp", LongType, nullable = false), // server-assigned timestamp when record was received
      StructField("client_submission_date", StringType, nullable = true), // Fields[Date], the HTTP Date header sent by the client

      // TxP payload fields
      StructField("txp_payload_version", StringType, nullable = true), // payload/version
      StructField("txp_test", StringType, nullable = true), // payload/test
      StructField("txp_event_timestamp", LongType, nullable = true), // payload/events/timestamp
      StructField("txp_event", StringType, nullable = true), // payload/events/event
      StructField("txp_event_object", StringType, nullable = true) // payload/events/object
    ))
  }
}

