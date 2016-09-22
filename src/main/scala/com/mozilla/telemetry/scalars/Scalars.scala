package com.mozilla.telemetry.scalars

import java.util

import org.yaml.snakeyaml.Yaml
import collection.JavaConversions._
import scala.io.Source

sealed abstract class ScalarDefinition
case class UintScalar(keyed: Boolean) extends ScalarDefinition
case class BooleanScalar(keyed: Boolean) extends ScalarDefinition
case class StringScalar(keyed: Boolean) extends ScalarDefinition

trait ScalarsDefinitionApi {
  val definitions: Map[String, ScalarDefinition]
}

object Scalars extends ScalarsDefinitionApi {
  val definitions = {
    // TODO: Scalars are not on release yet! Uncomment the line below once they hit Release.
    val uris = Map(// "https://hg.mozilla.org/releases/mozilla-release/raw-file/tip/toolkit/components/telemetry/Scalars.yaml",
                   "beta" -> "https://hg.mozilla.org/releases/mozilla-beta/raw-file/tip/toolkit/components/telemetry/Scalars.yaml",
                   "aurora" -> "https://hg.mozilla.org/releases/mozilla-aurora/raw-file/tip/toolkit/components/telemetry/Scalars.yaml",
                   "nightly" -> "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/Scalars.yaml")

    // Scalars are considered to be immutable so it's OK to merge their definitions.
    uris.flatMap{ case (key, value) =>
      val yaml = new Yaml().load(Source.fromURL(value, "UTF8").mkString)
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

      // Emit a scalar definition for each scalar.
      val scalarDefinitions = for {
        (scalarName, v) <- flattenedScalars
      } yield {
        val props = v.asInstanceOf[util.LinkedHashMap[String, Any]]
        val kind = props.get("kind").asInstanceOf[String]
        val keyed = props.getOrElse("keyed", false).asInstanceOf[Boolean]

        kind match {
          case ("uint") =>
            Some((scalarName, UintScalar(keyed)))
          case ("boolean") =>
            Some((scalarName, BooleanScalar(keyed)))
          case ("string") =>
            Some((scalarName, StringScalar(keyed)))
          case _ =>
            None
        }
      }

      scalarDefinitions.flatten.toMap
    }
  }
}
