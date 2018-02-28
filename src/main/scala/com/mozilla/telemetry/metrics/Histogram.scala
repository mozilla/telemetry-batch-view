package com.mozilla.telemetry.metrics

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.{Map => MMap}
import scala.io.Source
import com.mozilla.telemetry.utils.MainPing

case class RawHistogram(values: Map[String, Int], sum: Long)

sealed abstract class HistogramDefinition extends MetricDefinition {
  def getBuckets: Array[Int]
}
case class FlagHistogram(keyed: Boolean, originalName: String, process: Option[String] = None) extends HistogramDefinition {
  def getBuckets: Array[Int] = Array(0, 1)
  val isCategoricalMetric = true
}
case class BooleanHistogram(keyed: Boolean, originalName: String, process: Option[String] = None) extends HistogramDefinition {
  def getBuckets: Array[Int] = Array(0, 1)
  val isCategoricalMetric = true
}
case class CountHistogram(keyed: Boolean, originalName: String, process: Option[String] = None) extends HistogramDefinition {
  def getBuckets: Array[Int] = Array(0, 1)
  val isCategoricalMetric = false
}
case class EnumeratedHistogram(keyed: Boolean, originalName: String, nValues: Int, process: Option[String] = None) extends HistogramDefinition {
  def getBuckets: Array[Int] = (0 to nValues).toArray
  val isCategoricalMetric = true
}
case class LinearHistogram(keyed: Boolean, originalName: String, low: Int, high: Int, nBuckets: Int, process: Option[String] = None) extends HistogramDefinition {
  def getBuckets: Array[Int] = Histograms.linearBuckets(low, high, nBuckets)
  val isCategoricalMetric = false
}
case class ExponentialHistogram(keyed: Boolean, originalName: String, low: Int, high: Int, nBuckets: Int, process: Option[String] = None) extends HistogramDefinition {
  def getBuckets: Array[Int] = Histograms.exponentialBuckets(low, high, nBuckets)
  val isCategoricalMetric = false
}

/**
 * All non-labeled values will be aggregated into the spill bucket,
 * and their label is SpillBucketName
 */
case class CategoricalHistogram(keyed: Boolean, originalName: String, labels: Seq[String], process: Option[String] = None) extends HistogramDefinition {
  def getLabel(i: Int): String = labels.lift(i).getOrElse(CategoricalHistogram.SpillBucketName)
  def getBuckets: Array[Int] = (0 to labels.length).toArray
  val isCategoricalMetric = true
}
object CategoricalHistogram{ val SpillBucketName = "spill" }

class HistogramsClass extends MetricsClass {
  def HistogramPrefix = "histogram"

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

  def prefixProcessJoiner(name: String, process: String) = s"${HistogramPrefix}_${process}_${name.toLowerCase}"

  def suffixProcessJoiner(name: String, process: String) = (name + ( if(process != "parent") "_" + process else "" )).toUpperCase

  def definitions(includeOptin: Boolean = false, nameJoiner: (String, String) => String = suffixProcessJoiner, includeCategorical: Boolean = false): Map[String, HistogramDefinition] = {
    implicit val formats = DefaultFormats

    val uris = Map("release" -> "https://hg.mozilla.org/releases/mozilla-release/raw-file/tip/toolkit/components/telemetry/Histograms.json",
                   "beta" -> "https://hg.mozilla.org/releases/mozilla-beta/raw-file/tip/toolkit/components/telemetry/Histograms.json",
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
            case ("record_in_processes", JArray(x)) => Some(x.map{ p =>
              p match {
                case JString(p) => Some(p)
                case _ => None
              }
            }.toList.flatten)
            case ("labels", JArray(x)) => Some(x.map{ l =>
              l match {
                case JString(l) => Some(l)
                case _ => None
              }
            })
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
        (includeOptin || (definition.getOrElse("releaseChannelCollection", "opt-in") == Some("opt-out"))) &&
        (includeCategorical || (definition.getOrElse("kind", "categorical") != Some("categorical")))
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
        val labels = v.getOrElse("labels", None).asInstanceOf[Option[Seq[Option[String]]]]

        val processes = getProcesses(
          v.getOrElse("record_in_processes", Some(MainPing.ProcessTypes)).get
          .asInstanceOf[List[String]]
        )

        def addProcesses(key: String, definition: HistogramDefinition): List[(String, HistogramDefinition)] = {
          processes.map(process => (nameJoiner(key, process), definition match {
            case d: FlagHistogram => d.copy(process=Some(process))
            case d: BooleanHistogram => d.copy(process=Some(process))
            case d: CountHistogram => d.copy(process=Some(process))
            case d: EnumeratedHistogram => d.copy(process=Some(process))
            case d: LinearHistogram => d.copy(process=Some(process))
            case d: ExponentialHistogram => d.copy(process=Some(process))
            case d: CategoricalHistogram => d.copy(process=Some(process))
          }))
        }

        (kind, nValues, high, nBuckets, labels) match {
          case ("flag", _, _, _, _) =>
            Some(addProcesses(k, FlagHistogram(keyed, k)))
          case ("boolean", _, _, _, _) =>
            Some(addProcesses(k, BooleanHistogram(keyed, k)))
          case ("count", _, _, _, _) =>
            Some(addProcesses(k, CountHistogram(keyed, k)))
          case ("enumerated", Some(x), _, _, _) =>
            Some(addProcesses(k, EnumeratedHistogram(keyed, k, x)))
          case ("linear", _, Some(h), Some(n), _) =>
            Some(addProcesses(k, LinearHistogram(keyed, k, low, h, n)))
          case ("exponential", _, Some(h), Some(n), _) =>
            Some(addProcesses(k, ExponentialHistogram(keyed, k, low, h, n)))
          case ("categorical", _, _, _, Some(l)) =>
            Some(addProcesses(k, CategoricalHistogram(keyed, k, l.map(_.get))))
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
