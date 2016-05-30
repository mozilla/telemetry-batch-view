package telemetry.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import telemetry.utils.Utils

object JSON2Avro{
  def parseRecord(schema: Schema, json: JValue): Option[GenericData.Record] = {
    val record = new GenericData.Record(schema)

    for (field <- schema.getFields) {
      val parsed = parse(field.schema, json \ Utils.camelize(field.name))
      parsed match {
        case Some(value) =>
          record.put(field.name, value)
        case _ =>
      }
    }

    Some(record)
  }

  def parseArray(schema : Schema, json: JValue): Option[Array[Any]] = json match {
    case JArray(json) =>
      val parsed = for { elem <- json } yield {
        parse(schema.getElementType(), elem)
      }
      Some(parsed.flatten.toArray)
    case _ =>
      None
  }

  def parseMap(schema : Schema, json: JValue): Option[java.util.Map[String, Any]] = json match {
    case JObject(json) =>
      val parsed = for { (k, v) <- json } yield {
        val result = parse(schema.getValueType(), v)
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

  def parseFloat(Schema: Schema, json: JValue): Option[Double] = json match {
    case JDouble(value) =>
      Some(value.toFloat)
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

  def parse(schema: Schema, json: JValue): Option[Any] = schema.getType() match{
    case Schema.Type.NULL => None // the null type is not used, except as part of the optional type or as a part of unions

    case Schema.Type.RECORD =>
      parseRecord(schema, json)

    case Schema.Type.ARRAY =>
      parseArray(schema, json)

    case Schema.Type.MAP =>
      parseMap(schema, json)

    case Schema.Type.STRING =>
      parseString(schema, json)

    case Schema.Type.UNION =>
      // go through each possible option, taking the first one that successfully parses, or None if none parse successfully
      // this is done in the same order as the types are defined in the union
      schema.getTypes().flatMap(typePossibility => parse(typePossibility, json)).headOption

    case Schema.Type.INT =>
      parseInt(schema, json)

    case Schema.Type.LONG =>
      parseLong(schema, json)

    case Schema.Type.DOUBLE =>
      parseDouble(schema, json)

    case Schema.Type.FLOAT =>
      parseFloat(schema, json)

    case Schema.Type.BOOLEAN =>
      parseBoolean(schema, json)

    case _ =>
      throw new Exception("Unhandled type")
  }
}

