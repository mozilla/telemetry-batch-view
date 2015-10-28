package telemetry

import java.io.BufferedInputStream
import java.io.FileInputStream
import heka.HekaFrame
import heka.Message
import heka.Field
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData

object DerivedStream {
  def buildSchema(): Schema = {
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

  def main(args: Array[String]){
    val input = new BufferedInputStream(new FileInputStream("/Users/vitillo/sandbox/aws-lambda-parquet/src/main/resources/sample.Heka"))
    val schema = buildSchema()
    val messages = HekaFrame.parse(input)
    val records = messages.map((m) => buildRecord(m, schema)).flatten

    println(records.take(5))
    println(records.size)
  }
}
