package com.mozilla.telemetry.utils

import org.apache.spark.sql.Row
import org.json4s.JsonAST._

case class Addon(id: Option[String],
                 blocklisted: Option[Boolean],
                 description: Option[String],
                 name: Option[String],
                 userDisabled: Option[Boolean],
                 appDisabled: Option[Boolean],
                 version: Option[String],
                 scope: Option[Integer],
                 `type`: Option[String],
                 foreignInstall: Option[Boolean],
                 hasBinaryComponents: Option[Boolean],
                 installDay: Option[Integer],
                 updateDay: Option[Integer],
                 signedState: Option[Integer],
                 isSystem: Option[Boolean])

case class Attribution(source: Option[String],
                       medium: Option[String],
                       campaign: Option[String],
                       content: Option[String])

object MainPing{
  // Count the number of keys inside a JSON Object
  def countKeys(o: JValue): Option[Long] = {
    o match {
      case JObject(x) => Some(x.length)
      case _ => None
    }
  }

  def compareFlashVersions(x: Option[String], y: Option[String]): Option[Int] = {
    (x, y) match {
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
        Some(0)
      }
      case _ => None
    }
  }

  private def maxFlashVersion(a: String, b: String): String = {
    val c = compareFlashVersions(Some(a), Some(b)).getOrElse(1)
    if (c < 0)
      b
    else
      a
  }

  // See also:
  //  https://github.com/mozilla-services/data-pipeline/blob/master/hindsight/modules/fx/ping.lua#L82
  def getFlashVersion(addons: JValue): Option[String] = {
    val flashVersions: List[String] = for {
      JObject(addon) <- addons \ "activePlugins"
      JField("name", JString(addonName)) <- addon
      JField("version", JString(addonVersion)) <- addon
      if addonName == "Shockwave Flash"
    } yield addonVersion

    if (flashVersions.nonEmpty)
      Some(flashVersions.reduceLeft(maxFlashVersion(_, _)))
    else
      None
  }

  private val searchKeyPattern = "^(.+)\\.(.+)$".r

  def searchHistogramToRow(name: String, hist: JValue): Row = {
    // Split name into engine and source, then insert count from histogram.
    // If the name does not match the expected pattern, use 'null' for engine
    // and source. If the histogram sum is not a number, use 'null' for count.
    val count = hist \ "sum" match {
      case JInt(x) => x.toLong
      case _ => null
    }
    try {
      val searchKeyPattern(engine, source) = name
      Row(engine, source, count)
    } catch {
      case e: scala.MatchError => Row(null, null, count)
    }
  }

  def getSearchCounts(searchCounts: JValue): Option[List[Row]] = searchCounts match {
    case JObject(x) =>
      val buf = scala.collection.mutable.ListBuffer.empty[Row]
      for ((k, v) <- x) {
        buf.append(searchHistogramToRow(k, v))
      }
      if (buf.isEmpty) None
      else Some(buf.toList)
    case _ => None
  }

  // Return a row with the bucket values for the given set of keys as fields.
  def enumHistogramToRow(histogram: JValue, keys: IndexedSeq[String]): Row = histogram \ "values" match {
    case JNothing => null
    case v =>
      val values = keys.map(key => v \ key match {
        case JInt(n) => n.toInt
        case _ => 0
      })
      Row.fromSeq(values)
  }

  // Return a map of (keys -> counts), where keys are
  def enumHistogramToMap(histogram: JValue, keys: IndexedSeq[String]): Map[String, Int] = {
    histogram \ "values" match {
      case JNothing => null
      case v => keys.flatMap(key => v \ key match {
        case JInt(count) if count > 0 => Some(key -> count.toInt)
        case _ => None
      }).toMap
    }
  }

  // Return a map of histogram keys to rows with the bucket values for the given set of keys as fields.
  def keyedEnumHistogramToMap(histogram: JValue, keys: IndexedSeq[String]): Option[Map[String,Row]] = {
    val enums = Map[String, Row]() ++ (for {
      JObject(x) <- histogram
      (enumKey, enumHistogram) <- x
      enumRow = enumHistogramToRow(enumHistogram, keys)
      if enumRow != null
    } yield (enumKey, enumRow))

    if (enums.isEmpty)
      None
    else
      Some(enums)
  }

  // Find the largest numeric bucket that contains a value greater than zero.
  def enumHistogramToCount(h: JValue): Option[Int] = {
    val buckets = for {
      JObject(x) <- h \ "values"
      (bucket, JInt(count)) <- x
      if count > 0
      b <- toInt(bucket)
    } yield b

    buckets match {
      case x if x.nonEmpty => Some(x.max)
      case _ => None
    }
  }

  // Get the count of a specific histogram bucket
  def enumHistogramBucketCount(h: JValue, bucket: String): Option[Int] = {
    h \ "values" match {
      case JNothing => None
      case v => v \ bucket match {
        case JInt(count) => Some(count.toInt)
        case _ => None
      }
      case _=> None
    }
  }

  // Sum all counts in a histogram with the given keys
  def enumHistogramSumCounts(histogram: JValue, keys: IndexedSeq[String]): Int = {
    histogram \ "values" match {
      case JNothing => 0
      case v =>
        keys.map(key => v \ key match {
          case JInt(n) => n.toInt
          case _ => 0
        }).sum
    }
  }

  // Given histogram h, return floor(mean) of the measurements in the bucket.
  // That is, the histogram sum divided by the number of measurements taken.
  def histogramToMean(h: JValue): Option[Int] = {
    h \ "sum" match {
      case JInt(sum) if sum < 0 => None
      case JInt(sum) if sum == 0 => Some(0)
      case JInt(sum) => {
        val totalCount = (for {
          JObject(values) <- h \ "values"
          JField(bucket, JInt(count)) <- values
        } yield count).sum
        if (totalCount == 0) None
        else Some((sum / totalCount).toInt)
      }
      case _ => None
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

  // Check if a json value contains a number greater than zero.
  private def gtZero(v: JValue): Boolean = v match {
    case JInt(x) => x > 0
    case _ => false
  }

  private def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }
}
