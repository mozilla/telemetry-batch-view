/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.Dataset
import com.mozilla.telemetry.utils.{Experiment, getOrCreateSparkSession}
import org.apache.spark.sql.SaveMode
import org.joda.time.{DateTime, Days, format}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.rogach.scallop.ScallopConf

case class Application(
    architecture: String,
    buildId: String,
    channel: String,
    name: String,
    platformVersion: String,
    vendor: String,
    version: String,
    xpcomAbi: String)

case class Build(
    applicationId: Option[String],
    applicationName: Option[String],
    architecture: Option[String],
    buildId: String,
    platformVersion: String,
    vendor: String,
    version: String,
    xpcomAbi: String)

case class SystemOs(name: String, version: String)

case class SystemGfxFeatures(compositor: Option[String])

case class SystemGfx(
    D2DEnabled: Option[Boolean],
    DWriteEnabled: Option[Boolean],
    features: Option[SystemGfxFeatures])


case class System(os: SystemOs, gfx: Option[SystemGfx])

case class ActiveExperiment(id: String, branch: String)

case class Addons(activeExperiment: Option[ActiveExperiment])

case class Settings(
    blocklistEnabled: Option[Boolean],
    isDefaultBrowser: Option[Boolean],
    e10sEnabled: Option[Boolean],
    locale: String,
    telemetryEnabled: Boolean)

case class Profile(
    creationDate: Option[Int],
    resetDate: Option[Int])

case class Meta(
    Host: Option[String],
    Hostname: Option[String],
    Size: Option[Double],
    Timestamp: Option[Int],
    Type: Option[String],
    appBuildId: Option[String],
    appName: Option[String],
    appUpdateChannel: Option[String],
    appVendor: Option[String],
    appVersion: Option[Double],
    clientId: Option[String],
    creationTimestamp: Option[Float],
    docType: Option[String],
    documentId: Option[String],
    geoCity: Option[String],
    geoCountry: String,
    normalizedChannel: String,
    os: Option[String],
    sampleId: Option[Double],
    sourceName: Option[String],
    sourceVersion: Option[Int],
    submissionDate: Option[String],
    telemetryEnabled: Option[Boolean],
    `environment.build`: Build,
    `environment.settings`: Settings,
    `environment.system`: System,
    `environment.profile`: Profile,
    `environment.addons`: Addons,
    `environment.experiments`: Option[Map[String, Experiment]]) {

  def getExperiments: Map[String, Option[String]] =
    this.`environment.experiments`.getOrElse(Map.empty).mapValues(_.branch)

}

case class Payload(
    crashDate: String,
    processType: Option[String],
    hasCrashEnvironment: Boolean,
    metadata: Map[String, String],
    version: Int)

case class CrashPing(
    application: Application,
    clientId: Option[String],
    creationDate: String,
    meta: Meta,
    id: String,
    `type`: String,
    version: Int,
    payload: Payload)

case class CrashSummary (
    client_id: Option[String],
    normalized_channel: String,
    build_version: String,
    build_id: String,
    channel: String,
    application: String,
    os_name: String,
    os_version: String,
    architecture: String,
    country: String,
    experiment_id: Option[String],
    experiment_branch: Option[String],
    experiments: Map[String, Option[String]],
    e10s_enabled: Option[Boolean],
    gfx_compositor: Option[String],
    profile_created: Option[Int],
    payload: Payload) {

  def this(ping: CrashPing) = {
    this(
      client_id = ping.clientId,
      normalized_channel = ping.meta.normalizedChannel,
      build_version = ping.meta.`environment.build`.version,
      build_id = ping.meta.`environment.build`.buildId,
      channel = ping.application.channel,
      application = ping.application.name,
      os_name = ping.meta.`environment.system`.os.name,
      os_version = ping.meta.`environment.system`.os.version,
      architecture = ping.application.architecture,
      country = ping.meta.geoCountry,
      experiment_id = for {
        x <- ping.meta.`environment.addons`.activeExperiment
      } yield x.id,
      experiment_branch = for {
        x <- ping.meta.`environment.addons`.activeExperiment
      } yield x.branch,
      experiments = ping.meta.getExperiments,
      e10s_enabled = ping.meta.`environment.settings`.e10sEnabled,
      gfx_compositor = for {
        x <- ping.meta.`environment.system`.gfx
        y <-  x.features
        z <- y.compositor
      } yield z,
      profile_created = ping.meta.`environment.profile`.creationDate,
      payload = ping.payload
    )
  }
}

object CrashSummaryView {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val outputBucket = opt[String](
      "outputBucket",
      descr = "Bucket in which to save data",
      required = false,
      default = Some("telemetry-test-bucket"))
    val from = opt[String](
      "from",
      descr = "From submission date",
      required = false)
    val to = opt[String](
      "to",
      descr = "To submission date",
      required = false)
    val dryRun = opt[Boolean](
      "dryRun",
      descr = "Calculate the dataset, but do not write to S3",
      required = false,
      default=Some(false))
    verify()
  }

  def transformPayload(fields: Map[String, Any], payload: Option[String]): Option[CrashPing] = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List(
    "environment.build",
    "environment.settings",
    "environment.system",
    "environment.profile",
    "environment.addons",
    "environment.experiments"
    )
    val jsonObj = Extraction.decompose(fields)
    // Transform json fields into JValues
    val meta = jsonObj transformField {
      case JField(key, JString(s)) if jsonFieldNames contains key => (key, parse(s))
    }
    val submission = if(payload.isDefined) payload else fields.get("submission")
    val jsonPayload = submission match {
      case Some(value: String) => parse(value) ++ JObject(List(JField("meta", meta)))
      case _ => JObject()
    }
    jsonPayload.extractOpt[CrashPing]
  }

  def main(args: Array[String]): Unit = {
    // Setup spark contexts
    val spark = getOrCreateSparkSession(this.getClass.getName)
    implicit val sc = spark.sparkContext
    import spark.implicits._
    implicit val formats = Serialization.formats(NoTypeHints)

    // Parse command line options
    val opts = new Opts(args)
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val to = opts.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }
    val from = opts.from.get match {
      case Some(f) => fmt.parseDateTime(f)
      case _ => DateTime.now.minusDays(1)
    }

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyy-MM-dd")

      val messages = Dataset("telemetry")
        .where("sourceName") { case "telemetry" => true }
        .where("sourceVersion") { case "4" => true }
        .where("docType") { case "crash" => true }
        .where("submissionDate") { case date if date == currentDate.toString("yyyyMMdd") => true }

      val processedPings = spark.sparkContext.longAccumulator("processedPings")
      val discardedPings = spark.sparkContext.longAccumulator("discardedPings")
      val crashPings = messages.records()
        .map(x => this.transformPayload(x.fieldsAsMap, x.payload))
      crashPings.foreach(x => {
        if (!x.isDefined) {
          discardedPings.add(1)
        } else {
          processedPings.add(1)
        }
      })
      val crashSummary = crashPings.flatMap(identity[Option[CrashPing]]).map(new CrashSummary(_))
      val dataset = spark.createDataset(crashSummary)

      // Save to S3
      if (!opts.dryRun()) {
        val prefix = s"crash_summary/v1"
        val outputBucket = opts.outputBucket()
        val path = s"s3://${outputBucket}/${prefix}/submission_date=${currentDateString}"
        dataset.write.mode(SaveMode.Overwrite).parquet(path)
      }
      logger.info("************************************")
      logger.info(s"Total pings: ${dataset.count()}")
      logger.info(s"Processed pings: ${processedPings.value}")
      logger.info(s"Discarded pings: ${discardedPings.value}")
      logger.info("************************************")
    }
    sc.stop()
  }
}
