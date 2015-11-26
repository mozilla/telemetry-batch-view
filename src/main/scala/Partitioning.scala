package telemetry

import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.JavaConverters._

case class Partitioning(dimensions: List[Dimension]) {
  def partitionPrefix(prefix: String): String = {
    val path = prefix.split("/")

    path(0) + "/" + dimensions
      .zip(path.drop(1))
      .map(x => x._1.fieldName + "S3=" + x._2)
      .mkString("/")
  }
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
