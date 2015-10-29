package telemetry.streams

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._
import telemetry.DerivedStream
import telemetry.heka.{Field, HekaFrame, Message}

object ExampleStream extends DerivedStream{
  def buildSchema: Schema = {
    SchemaBuilder
      .record("System").fields
      .name("memoryMB").`type`().nullable().intType().noDefault()
      .name("cpu").`type`().record("CPU").fields
      .name("count").`type`().nullable().intType().noDefault()
      .name("vendor").`type`().nullable().stringType().noDefault()
      .endRecord()
      .noDefault()
      .endRecord
  }

  def buildRecord(message: Message, schema: Schema): Option[GenericRecord] ={
    val fields = HekaFrame.fields(message)
    val parsed = fields("environment.system") match {
      case x: String => parse(x)
      case _ => return None
    }

    val cpuRecord = new GenericRecordBuilder(schema.getField("cpu").schema)
      .set("count", parsed \\ "cpu" \\ "count" match {
             case JInt(x) => x
             case _ => None})
      .set("vendor", parsed \\ "cpu" \\ "vendor" match {
             case JString(x) => x
             case _ => None})
      .build

    Some(new GenericRecordBuilder(schema)
      .set("memoryMB", parsed \\ "memoryMB" match {
             case JInt(x) => x
             case _ => None
           })
      .set("cpu", cpuRecord)
      .build)
  }
}
