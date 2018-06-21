/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import org.json4s._
import org.json4s.jackson.JsonMethods._

case class Partitioning(dimensions: List[Dimension]) {
  def partitionPrefix(prefix: String, version: String): String = {
    val path = prefix.split("/")

    version + "/" + dimensions
      .zip(path.drop(1))
      .map(x => snakify(x._1.fieldName + "S3=") + x._2)
      .mkString("/")
  }

  private def snakify(name : String) =
    name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase
}

case class Dimension(fieldName: String)

object Partitioning{
  private implicit val formats = DefaultFormats

  def apply(rawSchema: String): Partitioning = {
    // Can't use extract method with old json4s version used by Spark, see https://github.com/json4s/json4s/pull/126
    val schema = parse(rawSchema)
    val dimensions = for {
      JObject(root) <- schema
      JField("dimensions", JArray(dimensions)) <- root
      JObject(dimension) <- dimensions
      JField("field_name", JString(field)) <- dimension
    } yield Dimension(field)
    Partitioning(dimensions)
  }
}
