/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.avro

import com.mozilla.telemetry.utils._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.json4s._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object JSON2Avro{
  def parseRecord(schema: Schema, json: JValue): Option[GenericData.Record] = json match {
      case JObject(o) =>
        val record = new GenericData.Record(schema)

        for (field <- schema.getFields) {
          val parsed = parse(field.schema, json \ camelize(field.name))
          parsed match {
            case Some(value) =>
              record.put(field.name, value)
            case _ =>
          }
        }

        Some(record)

      case _ =>
        None
  }

  // Note that java.util.Collection is used for compatibility with parquet-avro
  def parseArray(schema : Schema, json: JValue): Option[java.util.Collection[Any]] = json match {
    case JArray(elems) =>
      val parsed = for { elem <- elems } yield {
        parse(schema.getElementType, elem)
      }
      Some(parsed.flatten.asJava)
    case _ =>
      None
  }

  def parseMap(schema : Schema, json: JValue): Option[java.util.Map[String, Any]] = json match {
    case JObject(o) =>
      val parsed = for { (k, v) <- o } yield {
        val result = parse(schema.getValueType, v)
        result match {
          case Some(value) =>
            Some(k -> value)
          case _ =>
            None
        }
      }
      Some(mapAsJavaMap(parsed.flatten.toMap))
    case _ =>
      None
  }

  def parseString(schema: Schema, json: JValue): Option[String] = json match {
    case JString(value) =>
      Some(value)
    case JInt(value) =>
      Some(value.toString)
    case JDecimal(value) =>
      Some(value.toString)
    case JDouble(value) =>
      Some(value.toString)
    case JBool(value) =>
      Some(value.toString)
    case _ =>
      None
  }

  def parseInt(Schema: Schema, json: JValue): Option[Int] = json match {
    case JInt(value) =>
      Some(value.toInt)
    case _ =>
      None
  }

  def parseLong(Schema: Schema, json: JValue): Option[Long] = json match {
    case JInt(value) =>
      Some(value.toLong)
    case _ =>
      None
  }

  def parseDouble(Schema: Schema, json: JValue): Option[Double] = json match {
    case JDouble(value) =>
      Some(value.toDouble)
    case _ =>
      None
  }

  def parseBoolean(Schema: Schema, json: JValue): Option[Boolean] = json match {
    case JBool(value) =>
      Some(value)
    case _ =>
      None
  }

  def parse(schema: Schema, json: JValue): Option[Any] = schema.getType match{
    case Schema.Type.RECORD =>
      parseRecord(schema, json)

    case Schema.Type.ARRAY =>
      parseArray(schema, json)

    case Schema.Type.MAP =>
      parseMap(schema, json)

    case Schema.Type.STRING =>
      parseString(schema, json)

    case Schema.Type.UNION =>
      parse(schema.getTypes()(1), json)

    case Schema.Type.INT =>
      parseInt(schema, json)

    case Schema.Type.LONG =>
      parseLong(schema, json)

    case Schema.Type.DOUBLE =>
      parseDouble(schema, json)

    case Schema.Type.BOOLEAN =>
      parseBoolean(schema, json)

    case _ =>
      throw new Exception("Unhandled type")
  }
}

