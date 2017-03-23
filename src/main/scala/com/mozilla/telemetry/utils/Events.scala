package com.mozilla.telemetry.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.json4s.{DefaultFormats, JValue}

import scala.util.{Success, Try}

case class Event(timestamp: Long,
                 category:  String,
                 method:    String,
                 obj:       String,
                 strValue:  Option[String] = None,
                 mapValues: Option[Map[String, Any]] = None) {

  def toList: List[Any] = List(
    timestamp,
    category,
    method,
    obj,
    strValue.orNull,
    mapValues.orNull
  )

  def toRow: Row = Row.fromSeq(toList)
}

object Event {
  def fromList(event: List[Any]): Option[Event] = {
    // TODO: this is getting really ugly -- maybe try a per-param match?
    event match {
      case _ @ List(
        timestamp: BigInt,
        category: String,
        method: String,
        obj: String,
        strValue: String,
        mapValues: Map[String@unchecked, Any@unchecked])
        => Some(Event(timestamp.toLong, category, method, obj, Some(strValue), Some(mapValues.map {
          // Bug 1339130
          case (k: String, null) => (k, "null")
          case (k: String, v: Any) => (k, v.toString)
        })
      ))
      case _ @ List(
        timestamp: BigInt,
        category: String,
        method: String,
        obj: String,
        null,
        mapValues: Map[String@unchecked, Any@unchecked])
        => Some(Event(timestamp.toLong, category, method, obj, None, Some(mapValues.map {
          case (k: String, null) => (k, "null")
          case (k: String, v: Any) => (k, v.toString)
        })
      ))
      case _ @ List(
        timestamp: BigInt,
        category: String,
        method: String,
        obj: String,
        strValue: String)
        => Some(Event(timestamp.toLong, category, method, obj, Some(strValue)))
      case _ @ List(
        timestamp: BigInt,
        category: String,
        method: String,
        obj: String)
        => Some(Event(timestamp.toLong, category, method, obj))
      case _ => None
    }
  }
}

object Events {
  def getEvents(events: JValue): Option[List[Row]] = {
    extractEvents(events).flatMap(eventToRow) match {
      case Nil => None
      case x => Some(x)
    }
  }

  def extractEvents(events: JValue): List[List[Any]] = {
    implicit val formats = DefaultFormats
    Try(events.extract[List[List[Any]]]) match {
      case Success(eventList) => eventList
      case _ => List(List())
    }
  }

  def eventToRow(event: List[Any]): Option[Row] = {
    Event.fromList(event) match {
      case Some(e) => Some(e.toRow)
      case _ => None
    }
  }


  def buildEventSchema = StructType(List(
    StructField("timestamp",    LongType, nullable = false),
    StructField("category",     StringType, nullable = false),
    StructField("method",       StringType, nullable = false),
    StructField("object",       StringType, nullable = false),
    StructField("string_value", StringType, nullable = true),
    StructField("map_values",   MapType(StringType, StringType), nullable = true)
  ))
}

