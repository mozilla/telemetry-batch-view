package com.mozilla.telemetry.histograms

import com.mozilla.telemetry.utils.MetricDefinition
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.{Map => MMap}
import scala.io.Source

case class RawHistogram(values: Map[String, Int], sum: Long)

sealed abstract class HistogramDefinition extends MetricDefinition
case class FlagHistogram(keyed: Boolean) extends HistogramDefinition
case class BooleanHistogram(keyed: Boolean) extends HistogramDefinition
case class CountHistogram(keyed: Boolean) extends HistogramDefinition
case class EnumeratedHistogram(keyed: Boolean, nValues: Int) extends HistogramDefinition
case class LinearHistogram(keyed: Boolean, low: Int, high: Int, nBuckets: Int) extends HistogramDefinition
case class ExponentialHistogram(keyed: Boolean, low: Int, high: Int, nBuckets: Int) extends HistogramDefinition

class HistogramsClass {
  private val processTypes = List("content", "gpu")
  private case class HistogramLocation(path: List[String], suffix: String)
  private def generateLocations(histogramKey: String)(processType: String): HistogramLocation = {
    HistogramLocation(
      List("payload", "processes", processType, histogramKey),
      "_" + processType.toUpperCase
    )
  }

  // Locations for non-keyed histograms
  private val histogramLocations =
    HistogramLocation(List("payload.histograms"), "") ::
    processTypes.map(generateLocations("histograms"))

  private val keyedHistogramLocations =
    HistogramLocation(List("payload.keyedHistograms"), "") ::
    processTypes.map(generateLocations("keyedHistograms"))

  private val suffixes =
    (histogramLocations ++ keyedHistogramLocations)
    .map( (hl: HistogramLocation) => hl.suffix )
    .distinct

  private def parseHistogramLocation[HistogramFormat : Manifest](
    payload: Map[String, Any],
    location: HistogramLocation
  ): Option[Map[String, HistogramFormat]] = {
    implicit val formats = DefaultFormats
    for {
      // Extract the json from the payload as a JValue
      json <- payload.get(location.path.head)
      root = parse(json.asInstanceOf[String])
      // Traverse the rest of the histogram path
      leaf = location.path.tail.foldLeft(root)((acc, x) => acc \ x)
      histograms <- leaf.extractOpt[Map[String, HistogramFormat]]
    } yield (
      histograms.map(pair => (pair._1 + location.suffix, pair._2))
    )
  }

  private def stripPayload[HistogramFormat: Manifest](
    locations: List[HistogramLocation]
  )(
    payload: Map[String, Any]
  ): Map[String, HistogramFormat] = {
    locations.map(parseHistogramLocation[HistogramFormat](payload, _))
      .flatten
      .foldLeft(Map[String, HistogramFormat]())((acc, map) => acc ++ map)
  }

  val stripHistograms = stripPayload[RawHistogram](histogramLocations) _
  val stripKeyedHistograms = stripPayload[Map[String, RawHistogram]](keyedHistogramLocations) _

  // mock[io.Source] wasn't working with scalamock, so we'll just use the function
  protected val getURL: (String, String) => scala.io.BufferedSource = Source.fromURL

  def definitions(includeOptin: Boolean = false): Map[String, HistogramDefinition] = {
    implicit val formats = DefaultFormats

    val uris = Map("release" -> "https://hg.mozilla.org/releases/mozilla-release/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "beta" -> "https://hg.mozilla.org/releases/mozilla-beta/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "aurora" -> "https://hg.mozilla.org/releases/mozilla-aurora/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "nightly" -> "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/Histograms.json")

    val parsed = uris.map{ case (key, value) =>
      val json = parse(getURL(value, "UTF8").mkString)
      val result = MMap[String, MMap[String, Option[Any]]]()

      /* Unfortunately the histogram definition file does not respect a proper schema and
         as such it's rather unpleasant to parse it in a statically typed langauge, see
         https://bugzilla.mozilla.org/show_bug.cgi?id=1245514 */

      for {
        JObject(root) <- json
        JField(name, JObject(histogram)) <- root
        JField(k, v) <- histogram
      } yield {
        val value = try {
          (k, v) match {
            case ("low", JString(x)) => Some(x.toInt)
            case ("low", JInt(x)) => Some(x.toInt)
            case ("high", JString(x)) => Some(x.toInt)
            case ("high", JInt(x)) => Some(x.toInt)
            case ("n_buckets", JString(x)) => Some(x.toInt)
            case ("n_buckets", JInt(x)) => Some(x.toInt)
            case ("n_values", JString(x)) => Some(x.toInt)
            case ("n_values", JInt(x)) => Some(x.toInt)
            case ("kind", JString(x)) => Some(x)
            case ("keyed", JBool(x)) => Some(x)
            case ("keyed", JString(x)) => x match {
              case "true" => Some(true)
              case _ => Some(false)
            }
            case ("releaseChannelCollection", JString(x)) => Some(x)
            case _ => None
          }
        } catch {
          case e: NumberFormatException =>
            None
        }

        if (value.isDefined) {
          val definition = result.getOrElse(name, MMap[String, Option[Any]]())
          result(name) = definition
          definition(k) = value
        }
      }

      def includeHistogram(definition: MMap[String, Option[Any]]) = {
        includeOptin || (definition.getOrElse("releaseChannelCollection", "opt-in") == Some("opt-out"))
      }

      val pretty = for {
        (k, v) <- result
        if includeHistogram(v)
      } yield {
        val kind = v("kind").get.asInstanceOf[String]
        val keyed = v.getOrElse("keyed", Some(false)).get.asInstanceOf[Boolean]
        val nValues = v.getOrElse("n_values", None).asInstanceOf[Option[Int]]
        val low = v.getOrElse("low", Some(1)).get.asInstanceOf[Int]
        val high = v.getOrElse("high", None).asInstanceOf[Option[Int]]
        val nBuckets = v.getOrElse("n_buckets", None).asInstanceOf[Option[Int]]

        def addSuffixes(key: String, definition: HistogramDefinition): List[(String, HistogramDefinition)] = {
            suffixes.map(suffixType => (key + suffixType, definition))
        }

        (kind, nValues, high, nBuckets) match {
          case ("flag", _, _, _) =>
            Some(addSuffixes(k, FlagHistogram(keyed)))
          case ("boolean", _, _ , _) =>
            Some(addSuffixes(k, BooleanHistogram(keyed)))
          case ("count", _, _, _) =>
            Some(addSuffixes(k, CountHistogram(keyed)))
          case ("enumerated", Some(x), _, _) =>
            Some(addSuffixes(k, EnumeratedHistogram(keyed, x)))
          case ("linear", _, Some(h), Some(n)) =>
            Some(addSuffixes(k, LinearHistogram(keyed, low, h, n)))
          case ("exponential", _, Some(h), Some(n)) =>
            Some(addSuffixes(k, ExponentialHistogram(keyed, low, h, n)))
          case _ =>
            None
        }
      }

      (key, pretty.flatten.flatten.toMap)
    }

    // Histograms are considered to be immutable so it's OK to merge their definitions
    parsed.flatMap(_._2)
  }

  def linearBuckets(min: Float, max: Float, nBuckets: Int): Array[Int] = {
    lazy val buckets = {
      val values = Array.fill(nBuckets){0}

      for(i <- 1 until nBuckets) {
        val linearRange = (min * (nBuckets - 1 - i) + max * (i - 1)) / (nBuckets - 2)
        values(i) = (linearRange + 0.5).toInt
      }

      values
    }

    memoLinearBuckets.getOrElseUpdate((min, max, nBuckets), buckets)
  }

  def exponentialBuckets(min: Float, max: Float, nBuckets: Int): Array[Int] = {
    lazy val buckets = {
      val logMax = math.log(max)
      val retArray = Array.fill(nBuckets){0}
      var current = min.toInt

      retArray(1) = current
      for (bucketIndex <- 2 until nBuckets) {
        val logCurrent = math.log(current)
        val logRatio = (logMax - logCurrent) / (nBuckets - bucketIndex)
        val logNext = logCurrent + logRatio
        val nextValue = math.floor(math.exp(logNext) + 0.5).toInt

        if (nextValue > current)
          current = nextValue
        else
          current = current + 1
        retArray(bucketIndex) = current
      }

      retArray
    }

    memoExponentialBuckets.getOrElseUpdate((min, max, nBuckets), buckets)
  }

  private val memoLinearBuckets = MMap[(Float, Float, Int), Array[Int]]()
  private val memoExponentialBuckets = MMap[(Float, Float, Int), Array[Int]]()
}

object Histograms extends HistogramsClass
