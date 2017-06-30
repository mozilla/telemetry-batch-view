package com.mozilla.telemetry.metrics

import java.util

import org.yaml.snakeyaml.Yaml
import collection.JavaConversions._
import scala.io.Source
import com.mozilla.telemetry.utils.MainPing

abstract class ScalarDefinition extends MetricDefinition
case class UintScalar(keyed: Boolean, processes: List[String]) extends ScalarDefinition
case class BooleanScalar(keyed: Boolean, processes: List[String]) extends ScalarDefinition
case class StringScalar(keyed: Boolean, processes: List[String]) extends ScalarDefinition

class ScalarsClass extends MetricsClass {
  val ScalarColumnNamePrefix = "scalar"

  // mock[io.Source] wasn't working with scalamock, so now we'll just use the function
  protected val getURL: (String, String) => scala.io.BufferedSource = Source.fromURL

  def getParquetFriendlyScalarName(scalarName: String, processName: String): String = {
    // Scalar group and probe names can contain dots ('.'). But we don't
    // want them to get into the Parquet column names, so replace them with
    // underscores ('_'). Additionally, to prevent name clashing, we prefix
    // all scalars.
    ScalarColumnNamePrefix + '_' + (processName + '_' + scalarName).replace('.', '_')
  }

  def definitions(includeOptin: Boolean = false): Map[String, ScalarDefinition] = {
    val uris = Map("release" -> "https://hg.mozilla.org/releases/mozilla-release/raw-file/tip/toolkit/components/telemetry/Scalars.yaml",
                   "beta" -> "https://hg.mozilla.org/releases/mozilla-beta/raw-file/tip/toolkit/components/telemetry/Scalars.yaml",
                   "aurora" -> "https://hg.mozilla.org/releases/mozilla-aurora/raw-file/tip/toolkit/components/telemetry/Scalars.yaml",
                   "nightly" -> "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/Scalars.yaml")

    // Scalars are considered to be immutable so it's OK to merge their definitions.
    uris.flatMap{ case (key, value) =>
      val yaml = new Yaml().load(getURL(value, "UTF8").mkString)
      val result = yaml.asInstanceOf[util.LinkedHashMap[String, util.LinkedHashMap[String, Any]]]

      // The probes in the definition file are represented in a fixed-depth, two-level structure.
      // Flatten the structure and build the full scalar names.
      val flattenedScalars = result.flatMap{
        case (scalarGroup, probes) =>
          scalarGroup match {
            case "telemetry.test" => None
            case _ =>
              for {
                (scalarName, v) <- probes
              } yield {
                (scalarGroup + "." + scalarName, v)
              }
          }
      }

      def includeScalar(v: Any) = {
        val definition = v.asInstanceOf[util.LinkedHashMap[String, Any]]
        includeOptin || (definition.getOrElse("release_channel_collection", "opt-in") == "opt-out")
      }

      // Emit a scalar definition for each scalar.
      val scalarDefinitions = for {
        (scalarName, v) <- flattenedScalars
        if includeScalar(v)
      } yield {
        val props = v.asInstanceOf[util.LinkedHashMap[String, Any]]
        val kind = props.get("kind").asInstanceOf[String]
        val keyed = props.getOrElse("keyed", false).asInstanceOf[Boolean]
        val processes = getProcesses(
          props
          .getOrElse("record_in_processes", new util.ArrayList(MainPing.ProcessTypes))
          .asInstanceOf[util.ArrayList[String]].toSet.toList
        )

        kind match {
          case ("uint") =>
            Some((scalarName, UintScalar(keyed, processes)))
          case ("boolean") =>
            Some((scalarName, BooleanScalar(keyed, processes)))
          case ("string") =>
            Some((scalarName, StringScalar(keyed, processes)))
          case _ =>
            None
        }
      }

      scalarDefinitions.flatten.toMap
    }
  }
}

package object Scalars extends ScalarsClass
