package telemetry.utils

import org.joda.time._
import org.json4s.JsonAST._

object Utils{
  val dict = Map("submission_url" -> "submissionURL",
                 "memory_mb" -> "memoryMB",
                 "virtual_max_mb" -> "virtualMaxMB",
                 "l2cache_kb" -> "l2cacheKB",
                 "l3cache_kb" -> "l3cacheKB",
                 "speed_mhz" -> "speedMHz",
                 "d2d_enabled" -> "D2DEnabled",
                 "d_write_enabled" -> "DWriteEnabled",
                 "vendor_id" -> "vendorID",
                 "device_id" -> "deviceID",
                 "subsys_id" -> "subsysID",
                 "ram" -> "RAM",
                 "gpu_active" -> "GPUActive",
                 "first_load_uri" -> "firstLoadURI",
                 "" -> "")

  def camelize(name: String) = {
    dict.getOrElse(name, {
                     val split = name.split("_")
                     val rest = split.drop(1).map(_.capitalize).mkString
                     split(0).mkString + rest
                   }
    )
  }

  def uncamelize(name: String) = {
    val pattern = java.util.regex.Pattern.compile("(^[^A-Z]+|[A-Z][^A-Z]+)")
    val matcher = pattern.matcher(name);
    val output = new StringBuilder

    while (matcher.find()) {
      if (output.length > 0)
        output.append("_");
      output.append(matcher.group().toLowerCase);
    }

    output.toString()
  }

  def normalizeISOTimestamp(timestamp: String) = {
    // certain date parsers, notably Presto's, have a hard time with certain date edge cases,
    // especially time zone offsets that are not between -12 and 14 hours inclusive (see bug 1250894)
    // for these time zones, we're going to use some hacky arithmetic to bring them into range;
    // they will still represent the same moment in time, just with a correct time zone
    // we're going to use the relatively lenient joda-time parser and output it in standard ISO format
    val dateFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()
    val date = dateFormatter.withOffsetParsed().parseDateTime(timestamp)
    val millisPerHour = 60 * 60 * 1000;
    val timezoneOffsetHours = date.getZone().getOffset(date).toDouble / millisPerHour
    val timezone = if (timezoneOffsetHours < -12.0) {
      org.joda.time.DateTimeZone.forOffsetMillis(((timezoneOffsetHours + 12 * Math.floor(timezoneOffsetHours / -12).toInt) * millisPerHour).toInt)
    } else if (timezoneOffsetHours > 14.0) (
      org.joda.time.DateTimeZone.forOffsetMillis(((timezoneOffsetHours - 12 * Math.floor(timezoneOffsetHours / 12).toInt) * millisPerHour).toInt)
    ) else {
      date.getZone()
    }
    dateFormatter.withZone(timezone).print(date)
  }

  def normalizeYYYYMMDDTimestamp(YYYYMMDD: String) = {
    val formatISO = org.joda.time.format.ISODateTimeFormat.dateTime()
    formatISO.withZone(org.joda.time.DateTimeZone.UTC).print(
      format.DateTimeFormat.forPattern("yyyyMMdd")
                                .withZone(org.joda.time.DateTimeZone.UTC)
                                .parseDateTime(YYYYMMDD.asInstanceOf[String])
    )
  }

  def normalizeEpochTimestamp(timestamp: BigInt) = {
    val dateFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()
    val millisecondsPerDay = 1000 * 60 * 60 * 24
    dateFormatter.withZone(org.joda.time.DateTimeZone.UTC).print(new DateTime(timestamp.toLong * millisecondsPerDay))
  }

  // Find the largest numeric bucket that contains a value greater than zero.
  def enumHistogramToCount(h: JValue): Option[Long] = {
    (h \ "values") match {
      case JNothing => None
      case JObject(x) => {
        var topBucket = -1
        for {
          (k, v) <- x
          b <- toInt(k) if b > topBucket && gtZero(v)
        } topBucket = b

        if (topBucket >= 0) {
          Some(topBucket)
        } else {
          None
        }
      }
      case _ => {
        None
      }
    }
  }


  // Given histogram h, return true if it has a value in the "true" bucket,
  // or false if it has a value in the "false" bucket, or None otherwise.
  def booleanHistogramToBoolean(h: JValue): Option[Boolean] = {
    (gtZero(h \ "values" \ "1"), gtZero(h \ "values" \ "0")) match {
      case (true, _) => Some(true)
      case (_, true) => Some(false)
      case _ => None
    }
  }

  def getHistogramSum(h: JValue, default: Int): Int = {
    (h \ "sum") match {
      case x: JInt => x.num.toInt
      case _ => default
    }
  }

  // Count the number of keys inside a JSON Object
  def countKeys(o: JValue): Option[Long] = {
    o match {
      case JObject(x) => Some(x.length)
      case _ => {
        None
      }
    }
  }

  // Check if a json value contains a number greater than zero.
  def gtZero(v: JValue): Boolean = {
    v match {
      case x: JInt => x.num.toInt > 0
      case _ => false
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def compareFlashVersions(a: Option[String], b: Option[String]): Option[Int] = {
    (a, b) match {
      case (Some(a), None) => Some(1)
      case (None, Some(b)) => Some(-1)
      case (Some(a), Some(b)) => {
        // Shortcut if they're the same string
        if (a == b) return Some(0)

        // Otherwise split them up and compare components numerically.
        val ac = a.split('.')
        val bc = b.split('.')
        var l = ac.length
        if (bc.length < l) l = bc.length

        var abad = true
        var bbad = true
        var aci: Array[Int] = null
        var bci: Array[Int] = null
        try {
          aci = ac.map(n => n.toInt)
          abad = false
        } catch {
          case _: NumberFormatException => abad = true
        }
        try {
          bci = bc.map(n => n.toInt)
          bbad = false
        } catch {
          case _: NumberFormatException => bbad = true
        }

        // Both bad... can't compare.
        if (abad && bbad) return None

        // Good > Bad
        if (abad) return Some(-1)
        if (bbad) return Some(1)

        for (versionPiece <- aci.zipAll(bci, 0, 0)) {
          if (versionPiece._1 < versionPiece._2) return Some(-1)
          if (versionPiece._1 > versionPiece._2) return Some(1)
        }

        // They're the same.
        return Some(0)
      }
      case _ => None
    }
  }

  def maxFlashVersion(a: String, b: String): String = {
    val c = compareFlashVersions(Some(a), Some(b)).getOrElse(1)
    if (c < 0)
      b
    else
      a
  }

  // See also:
  //  https://github.com/mozilla-services/data-pipeline/blob/master/hindsight/modules/fx/ping.lua#L82
  def getFlashVersion(addons: JValue): Option[String] = {
    val flashVersions = ((addons \ "activePlugins") match {
      case JArray(x) => x
      case _ => return None
    }).filter((p) => {
      p match {
        case (attrs: JValue) => {
          (attrs \ "name") match {
            case JString(a) => a == "Shockwave Flash"
            case _ => false
          }
        }
        case _ => false
      }
    }).flatMap((p) => {
      p match {
        case (attrs: JValue) => {
          (attrs \ "version") match {
            case JString(x) => Some(x)
            case _ => None
          }
        }
        case _ => None
      }
    })

    if (flashVersions.nonEmpty)
      Some(flashVersions.reduceLeft(maxFlashVersion(_, _)))
    else
      None
  }

  val searchKeyPattern = "^(.+)\\.(.+)$".r
  def searchHistogramToMap(name: String, hist: JValue): Option[Map[String, Any]] = {
    // Split name into engine and source, then insert count from histogram.
    try {
      val searchKeyPattern(engine, source) = name
      val count = (hist \ "sum") match {
        case x: JInt => x.num.toInt
        case _ => -1
      }
      Some(Map(
        "engine" -> engine,
        "source" -> source,
        "count" -> count
      ))
    } catch {
      case e: scala.MatchError => None
    }
  }

  def getSearchCounts(searchCounts: JValue): Option[List[Map[String,Any]]] = {
    searchCounts match {
      case JObject(x) => {
        val buf = scala.collection.mutable.ListBuffer.empty[Map[String,Any]]
        for ((k, v) <- x) {
          for (c <- searchHistogramToMap(k, v)) {
            buf.append(c)
          }
        }
        if (buf.isEmpty) None
        else Some(buf.toList)
      }
      case _ => None
    }
  }
}
