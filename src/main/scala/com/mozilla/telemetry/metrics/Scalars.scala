package com.mozilla.telemetry.metrics

import java.util

import org.yaml.snakeyaml.Yaml
import collection.JavaConversions._
import scala.io.Source
import com.mozilla.telemetry.utils.MainPing
import org.apache.spark.sql.types._

import java.io.InputStream


trait ScalarType {
  val name: String
  val keyed: Boolean
  val dataType: DataType

  def getName: String = keyed match {
    case true => List("keyed", name).mkString("_")
    case false => name
  }

  def getParquetType = keyed match {
    case true => MapType(StringType, dataType, valueContainsNull = true)
    case false => dataType
  }
}

case class BooleanScalarType(keyed: Boolean = false) extends ScalarType {
  val name = "boolean"
  val dataType = BooleanType
}

case class UintScalarType(keyed: Boolean = false) extends ScalarType {
  val name = "uint"
  val dataType = IntegerType
}

case class StringScalarType(keyed: Boolean = false) extends ScalarType {
  val name = "string"
  val dataType = StringType
}


abstract class ScalarDefinition extends MetricDefinition {
  val scalarType: ScalarType
}

case class UintScalar(keyed: Boolean, originalName: String, process: Option[String] = None)
  extends ScalarDefinition {
  val isCategoricalMetric = false
  val scalarType = UintScalarType(keyed)
}

case class BooleanScalar(keyed: Boolean, originalName: String, process: Option[String] = None)
  extends ScalarDefinition {
  val isCategoricalMetric = true
  val scalarType = BooleanScalarType(keyed)
}

case class StringScalar(keyed: Boolean, originalName: String, process: Option[String] = None)
  extends ScalarDefinition {
  val isCategoricalMetric = true
  val scalarType = StringScalarType(keyed)
}

class ScalarsClass extends MetricsClass {
  val DynamicScalarDefinitionsFile = "/addon/Scalars.yaml"
  val ScalarColumnNamePrefix = "scalar"

  // mock[io.Source] wasn't working with scalamock, so now we'll just use the function
  protected val getURL: (String, String) => scala.io.BufferedSource = Source.fromURL

  def getParquetFriendlyScalarName(scalarName: String, processName: String): String = {
    // Scalar group and probe names can contain dots ('.'). But we don't
    // want them to get into the Parquet column names, so replace them with
    // underscores ('_'). Additionally, to prevent name clashing, we prefix
    // all scalars.
    //
    // Dynamic scalars do not have the scalars_$PROCESS_ prefix,
    // since they are already separated.
    (processName match {
      case MainPing.DynamicProcess => scalarName
      case p => ScalarColumnNamePrefix + '_' + (p + '_' + scalarName)
    }).replace(".", "_")
  }

  def definitions(includeOptin: Boolean = false, nameJoiner: (String, String) => String = getParquetFriendlyScalarName): Map[String, ScalarDefinition] = {
    val uris = Map("release" -> "https://hg.mozilla.org/releases/mozilla-release/raw-file/tip/toolkit/components/telemetry/Scalars.yaml",
                   "beta" -> "https://hg.mozilla.org/releases/mozilla-beta/raw-file/tip/toolkit/components/telemetry/Scalars.yaml",
                   "nightly" -> "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/Scalars.yaml")

    // Addon scalars will show during the tests
    val addonStream : InputStream = getClass.getResourceAsStream(DynamicScalarDefinitionsFile)

    val sources = uris.map{ case(key, value) => key -> getURL(value, "UTF8") } +
                  ("addon" -> Source.fromInputStream(addonStream))

    // Scalars are considered to be immutable so it's OK to merge their definitions.
    sources.flatMap{ case (key, source) =>
      val result = new Yaml().load[util.LinkedHashMap[String, util.LinkedHashMap[String, Any]]](source.mkString)

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
          .getOrElse("record_in_processes", new util.ArrayList(MainPing.DefaultProcessTypes))
          .asInstanceOf[util.ArrayList[String]].toSet.toList
        )

        def addProcesses(key: String, definition: ScalarDefinition): List[(String, ScalarDefinition)] = {
          processes.map(process => (nameJoiner(key, process), definition match {
            case d: UintScalar => d.copy(process=Some(process))
            case d: BooleanScalar => d.copy(process=Some(process))
            case d: StringScalar => d.copy(process=Some(process))
          }))
        }

        kind match {
          case ("uint") =>
            Some(addProcesses(scalarName, UintScalar(keyed, scalarName)))
          case ("boolean") =>
            Some(addProcesses(scalarName, BooleanScalar(keyed, scalarName)))
          case ("string") =>
            Some(addProcesses(scalarName, StringScalar(keyed, scalarName)))
          case _ =>
            None
        }
      }

      scalarDefinitions.flatten.flatten.toMap
    }
  }
}

package object Scalars extends ScalarsClass
