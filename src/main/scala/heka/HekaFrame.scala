package telemetry.heka

import java.io.DataInputStream
import java.io.InputStream
import org.xerial.snappy.Snappy

object HekaFrame{
  private def field(f: Field): Any = {
    // I am assuming there is only one value
    f.valueType match {
      case Some(Field.ValueType.STRING) => f.valueString(0)
      case Some(Field.ValueType.BOOL) => f.valueBool(0)
      case Some(Field.ValueType.DOUBLE) => f.valueDouble(0)
      case Some(Field.ValueType.INTEGER) => f.valueInteger(0)
      case _ => ""
    }
  }

  def fields(m: Message): Map[String, Any] = {
    val fields = m.fields
    Map(fields.map(_.name).zip(fields.map(field)): _*)
  }

  def payloads(l: List[Message]): List[String] = l.map(_.payload).flatten

  // See https://hekad.readthedocs.org/en/latest/message/index.html
  def parse(i: InputStream): Iterator[Message] = {
    val is = new DataInputStream(i)

    def next: Option[Message] = {
      val cursor = is.read()

      if (cursor == -1)
        return None

      // Parse record separator
      if (cursor != 0x1E)
        throw new Exception("Invalid Heka Frame: missing record separator")

      // Parse header
      val headerLength = is.read()
      val headerBuffer = new Array[Byte](headerLength)
      is.readFully(headerBuffer, 0, headerLength)
      val header = Header.parseFrom(headerBuffer)

      // Parse unit separator
      if (is.read() != 0x1F)
        throw new Exception("Invalid Heka Frame: missing unit separator")

      // Parse message which should be compressed with Snappy
      val messageBuffer = new Array[Byte](header.messageLength)
      is.readFully(messageBuffer, 0, header.messageLength)

      val message = try {
        val uncompressedLenght = Snappy.uncompressedLength(messageBuffer)
        val uncompressedMessage = new Array[Byte](uncompressedLenght)
        Snappy.uncompress(messageBuffer, 0, header.messageLength, uncompressedMessage, 0)
        Message.parseFrom(uncompressedMessage, 0, uncompressedLenght)
      } catch {
        case _: Throwable => Message.parseFrom(messageBuffer)
      }

      Some(message)
    }

    Iterator
      .continually(next)
      .takeWhile((x) => x match {
                   case Some(x) => true
                   case None => false
                 })
      .map(_.get)
  }
}

