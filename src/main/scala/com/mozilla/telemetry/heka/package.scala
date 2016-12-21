package com.mozilla.telemetry

package object heka {
  class RichMessage(m: Message) {
    def fieldsAsMap(): Map[String, Any] = {
      val fields = m.fields
      Map(fields.map(_.name).zip(fields.map(field)): _*)
    }
  }

  private def field(f: Field): Any = {
    // I am assuming there is only one value
    f.valueType match {
      case Some(Field.ValueType.BYTES) => {
        val bytes = f.valueBytes(0)
        if (bytes.isValidUtf8) bytes.toStringUtf8 else ""
      }
      case Some(Field.ValueType.STRING) => f.valueString(0)
      case Some(Field.ValueType.BOOL) => f.valueBool(0)
      case Some(Field.ValueType.DOUBLE) => f.valueDouble(0)
      case Some(Field.ValueType.INTEGER) => f.valueInteger(0)
      case _ => ""
    }
  }

  implicit def messageToRichMessage(m: Message): RichMessage = new RichMessage(m)
}
