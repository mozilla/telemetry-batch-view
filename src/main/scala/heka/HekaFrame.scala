package telemetry.heka

import java.io.InputStream

object HekaFrame{
  def jsonBlobs(l: List[Message]): List[String] = l.map(_.payload).flatten

  // See https://hekad.readthedocs.org/en/latest/message/index.html
  def parse(is: InputStream): List[Message] = {
    val data = Iterator.continually(is.read).takeWhile(-1 !=).map(_.toByte).toArray

    def loop(i: Int, acc: List[Message]): List[Message] = {
      if (i >= data.length) acc
      else if (data(i) == 0x1E) {
        // Parse header
        val headerLength = data(i + 1)
        val header = Header.parseFrom(data, i + 2, headerLength)
        if (data(i + 2 + headerLength) != 0x1F) throw new Exception("Missing unit separator")

        // Parse compressed message which is almost always compressed with Snappy
        val messageOffset = i + 3 + headerLength
        val message = try {
          val uncompressedLenght = Snappy.uncompressedLength(data, messageOffset, header.messageLength)
          val uncompressedMessage = new Array[Byte](uncompressedLenght)
          Snappy.uncompress(data, messageOffset, header.messageLength, uncompressedMessage, 0)
          Message.parseFrom(uncompressedMessage, 0, uncompressedLenght)
        } catch {
          case _: Throwable => Message.parseFrom(data, messageOffset, header.messageLength)
        }

        loop(messageOffset + header.messageLength, message :: acc)
      } else throw new Exception("Invalid data format")
    }

    loop(0, List[Message]())
  }
}

