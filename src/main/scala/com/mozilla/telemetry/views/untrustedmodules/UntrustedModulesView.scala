/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.untrustedmodules

import com.mozilla.telemetry.pings.CombinedStacks
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import com.mozilla.telemetry.views.BatchJobBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scalaj.http.Http


object UntrustedModulesView extends BatchJobBase {

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

  def schemaVersion: String = "v1"

  private class Conf(args: Array[String]) extends BaseOpts(args) {
    val inputTable = opt[String]("inputTable", descr = "Source table containing untrustedModules pings with unsymbolicated stacktraces", required = false,
      default = Some("telemetry_untrusted_modules_parquet"))
    val outputPath = opt[String]("outputPath", descr = "Output path", required = true)
    val symbolicationServiceUrl = opt[String]("symbolicationServiceUrl", required = false,
      default = Some("https://symbols.mozilla.org//symbolicate/v5"))
    val partitionLimit = opt[Int]("partitionLimit", required = false, default = Some(5),
      descr = "Number of partitions to coalesce to before symbolicating (can be used to throttle requests to symbolication server)")
    val sampleFraction = opt[Double]("sampleFraction", required = false, default = Some(1),
      descr = "Fraction of records to sample from original dataset (for testing)")


    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark = getOrCreateSparkSession(jobName = "UntrustedModulesView", enableHiveSupport = true,
      additionalConfig = Seq(("spark.sql.sources.partitionOverwriteMode", "dynamic")))
    require(spark.version >= "2.3", "Spark 2.3 is required due to dynamic partition overwrite mode")
    import spark.implicits._

    for (currentDateString <- datesBetween(conf.from(), conf.to.toOption)) {
      logger.info(s"Processing day: $currentDateString")

      val rawPings = spark.sql(s"select * from ${conf.inputTable()}")
        .where($"submission_date_s3" === currentDateString)
        .where($"metadata.normalized_channel" === "nightly") // filter out beta and release so we don't kill symbolication server

      val sampled = conf.sampleFraction.toOption match {
        case Some(ratio) => rawPings.sample(ratio)
        case None => rawPings
      }
      val coalesced = conf.partitionLimit.toOption match {
        case Some(p) => sampled.coalesce(p)
        case None => sampled
      }

      val newSchema = StructType(coalesced.schema.fields ++ Array(StructField("symbolicated_stacks", StringType, false)))
      val symbolicated = coalesced.rdd.mapPartitions { rawPings =>
        rawPings.map { rawPing =>

          val combinedStacks: Row = rawPing.getAs[Row]("payload").getAs[Row]("combined_stacks")
          val stacksJson = combinedStackToJson(combinedStacks)
          //          val symbolicatedStacks = Symbolicator.symbolicate(conf.symbolicationServiceUrl())(stacksJson)
          val symbolicatedStacks = stacksJson.map(identity).getOrElse("Empty combinedStacks")

          val withSymbolicatedStacks: Row = Row.fromSeq(rawPing.toSeq ++ Array(symbolicatedStacks))

          withSymbolicatedStacks
        }
      }

      val symbolicatedDf = spark.createDataFrame(symbolicated, newSchema)

      symbolicatedDf.write
        .mode("overwrite")
        .partitionBy("submission_date_s3")
        .parquet(conf.outputPath() + "/" + schemaVersion)
    }

    stopSessionSafely(spark)
  }

  /**
    * Converts combined stacks to json compatible with Symbolication Server
    */
  def combinedStackToJson(combinedStacks: CombinedStacks): String = {
    val memoryMap = combinedStacks.memory_map.toList.map { mapping => List(mapping.module_name, mapping.debug_id) }
    val stacks = combinedStacks.stacks.toList.map { stack =>
      stack.toList.map { stackFrame => List(stackFrame.module_index, stackFrame.module_offset) }
    }
    compact(render(("memoryMap" -> memoryMap) ~ ("stacks" -> stacks)))
  }

  def combinedStackToJson(combinedStacks: Row): Option[String] = {
    val memoryMap = combinedStacks.getAs[Seq[Row]]("memory_map").map { r => List(r.getAs[String]("module_name"), r.getAs[String]("debug_id")) }
    val stacks = combinedStacks.getAs[Seq[Seq[Row]]]("stacks").map { stack =>
      stack.map { stackFrame => List(stackFrame.getAs[Long]("module_index"), stackFrame.getAs[Long]("module_offset")) }
    }

    if (memoryMap.nonEmpty && memoryMap.head.nonEmpty) {
      Some(compact(render(("memoryMap" -> memoryMap) ~ ("stacks" -> stacks))))
    } else {
      None
    }
  }
}


/**
  * Symbolicates stack traces using Mozilla Symbolication Server
  * (https://tecken.readthedocs.io/en/latest/symbolication.html)
  */
object Symbolicator {
  //  val url = "https://symbols.mozilla.org//symbolicate/v5"

  def symbolicate(url: String)(unsymbolicatedStacks: String): String = {
    Http(url).postData(unsymbolicatedStacks).asString.body
  }
}

object Test extends App {
  // scalastyle:off
  val testInput =
    """{"memoryMap": [["mozglue.pdb", "4F26C626A59BD87CFE4D05A43D48A2601"], ["kernelbase.pdb", "F4EEB437F634B4B76EE59F40C5E7FEFB1"], ["firefox.pdb", "8A265F2F4F95C74B92DC10568A6437091"], ["kernel32.pdb", "63816243EC704DC091BC31470BAC48A31"], ["ntdll.pdb", "38A5841BD353770D9C800BF1AF6B17EB1"]], "stacks": [[[0, 20955], [1, 207483], [2, 265500], [2, 264903], [2, 5814], [2, 4985], [2, 4382], [2, 270656], [3, 77876], [4, 463985]]]}"""

  val symbolicated = Symbolicator.symbolicate("https://symbols.mozilla.org//symbolicate/v5")(testInput)
  println("test input:")
  println(testInput)
  println()
  println("symbolicated:")
  println(symbolicated)
  // scalastyle:on
}