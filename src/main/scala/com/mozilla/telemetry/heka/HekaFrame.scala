package com.mozilla.telemetry.heka

import java.io.DataInputStream
import java.io.InputStream
import org.xerial.snappy.Snappy

object HekaFrame{
  private object Logger extends Serializable {
    @transient lazy val log = org.apache.log4j.Logger.getLogger(HekaFrame.getClass.getName)
  }

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

  // See https://hekad.readthedocs.org/en/latest/message/index.html
  def parse(i: => InputStream, fail: Throwable => Unit = ex => throw ex): Iterator[Message] = {
    var is: DataInputStream = null
    var offset = 0L

    def next: Option[Message] = {
      if (is == null) {
        is = new DataInputStream(i)

        // See https://stackoverflow.com/questions/14057720/robust-skipping-of-data-in-a-java-io-inputstream-and-its-subtypes
        // for why I am not using skip or skipBytes. This is slow but acceptable as it's a rare operation.
        for (i <- 0L until offset) {
          if (is.read() == -1) {
            throw new Exception("Failure to skip bytes")
          }
        }
      }

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
        val uncompressedLength = Snappy.uncompressedLength(messageBuffer)
        val uncompressedMessage = new Array[Byte](uncompressedLength)
        Snappy.uncompress(messageBuffer, 0, header.messageLength, uncompressedMessage, 0)
        Message.parseFrom(uncompressedMessage, 0, uncompressedLength)
      } catch {
        case ex: Throwable =>
          Message.parseFrom(messageBuffer)
      }

      // 3 -> one byte for the record separator, one for the header length and one for the unit separator
      offset += 3 + headerLength + header.messageLength
      Some(message)
    }

    @annotation.tailrec
    def retry[T](n: Int)(fn: => T): T = {
      import scala.util.{Failure, Success, Try}

      Try { fn } match {
        case Success(x) =>
          x
        case _ if n > 1 =>
          is = null
          retry(n - 1)(fn)
        case Failure(e) =>
          throw e
      }
    }

    Iterator
      .continually {
        try retry(3){
          next
        } catch {
          case ex: Throwable =>
            fail(ex)
            None
        }
      }.takeWhile {
        case Some(x) => true
        case _ => false
      }.flatten
  }
}
