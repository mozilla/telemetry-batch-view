package com.mozilla.telemetry.views

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.parquet.ParquetFile
import com.mozilla.telemetry.utils._
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop._
import scala.util.Random

/*
Useful links re: e10sCohort experiment design and structure:

* An overview of the rationale: https://bugzilla.mozilla.org/show_bug.cgi?id=1251259
* Original bug for e10sCohort: https://bugzilla.mozilla.org/show_bug.cgi?id=1249845
* A brief description of e10sCohort's schema: https://github.com/mozilla-services/mozilla-pipeline-schemas/issues/4
* A later addition to the schema: https://bugzilla.mozilla.org/show_bug.cgi?id=1255013
*/

object E10sExperimentView {
  private class SampleIdPartitioner extends Partitioner {
    def numPartitions: Int = 100

    def getPartition(key: Any): Int = key match {
      case (_, sampleId: Int) => sampleId % numPartitions
      case _ => throw new Exception("Invalid key")
    }
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = true)
    val to = opt[String]("to", descr = "To submission date", required = true)
    val channel = opt[String]("channel", descr = "channel", required = true)
    val version = opt[String]("version", descr = "version", required = true)
    val experimentId = opt[String]("experiment", descr = "experiment", required = true)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val from = opts.from()
    val to = opts.to()
    val channel = opts.channel()
    val version = opts.version()

    val sparkConf = new SparkConf().setAppName("E10sExperiment")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    implicit val sc = new SparkContext(sparkConf)

    val messages = Dataset("telemetry")
      .where("sourceName") {
        case "telemetry" => true
      }.where("sourceVersion") {
        case "4" => true
      }.where("docType") {
        case "saved_session" => true
      }.where("appName") {
        case "Firefox" => true
      }.where("submissionDate") {
        case date if date <= to && date >= from => true
      }.where("appUpdateChannel") {
        case x if x == channel => true
      }.where("appVersion") {
        case x if x == version => true
      }

    run(opts, messages)
  }

  private def run(opts: Opts, messages: RDD[Message]) {
    val experimentId = opts.experimentId().replaceAll("-", "_")
    val clsName = uncamelize(this.getClass.getSimpleName.replace("$", ""))
    val prefix = s"${clsName}/${experimentId}/v${opts.from()}_${opts.to()}"
    val outputBucket = opts.outputBucket()

    require(S3Store.isPrefixEmpty(outputBucket, prefix), s"s3://${outputBucket}/${prefix} already exists!")

    messages
      .flatMap { message =>
        val fields = message.fieldsAsMap
        val clientId = fields.get("clientId")
        val sampleId = fields.get("sampleId")
        val settings = parse(fields.getOrElse("environment.settings", "{}").asInstanceOf[String])

        // the e10s cohort determines which group a user belongs to in the e10s experiment
        // (as of beta46 experiment 2; previously, we used the Telemetry Experiments framework)
        val JString(cohort) = settings \ "e10sCohort"

        // the first time the system addon is run, it is possible that the addon determines the
        // user should have e10s enabled for the experiment, and sets the e10sCohort to "test",
        // yet it is too late in the session to actually enable e10s
        // the suggested solution is to check the value of environment/settings/e10sEnabled, and exclude those pings from consideration
        val JBool(e10sEnabled) = settings \ "e10sEnabled"
        val pingShouldBeExcluded = (cohort == "test" && !e10sEnabled) || // should have e10s enabled, but doesn't
          (cohort == "control" && e10sEnabled) // shouldn't have e10s enabled, but does

        (clientId, sampleId) match {
          case (Some(client: String), Some(sample: Double)) if !pingShouldBeExcluded =>
            Some(((client, sample.toInt), fields))
          case _ => None
        }
      }
      .mapValues(x => (x, 1))
      .reduceByKey{ case ((p1, n1), (p2, n2)) =>
        // Every submission should have the same probability of being chosen
        val n = n1 + n2
        val rnd = new Random().nextFloat()
        if (n * rnd < n1) (p1, n) else (p2, n)
      }
      .mapValues(x => x._1)
      .partitionBy(new SampleIdPartitioner())
      .values
      .foreachPartition{ fieldsIterator =>
        val schema = buildSchema
        val records = for {
          fields <- fieldsIterator
          record <- buildRecord(fields, schema)
        } yield record

        while(records.nonEmpty) {
          val localFile = new java.io.File(ParquetFile.serialize(records, schema).toUri)
          S3Store.uploadFile(localFile, outputBucket, prefix)
          localFile.delete()
        }
      }
  }

  private def buildSchema: Schema = {
    SchemaBuilder
      .record("Submission").fields
      .name("clientId").`type`().stringType().noDefault()
      .name("e10sCohort").`type`().stringType().noDefault()
      .name("creationTimestamp").`type`().stringType().noDefault()
      .name("submissionDate").`type`().stringType().noDefault()
      .name("documentId").`type`().stringType().noDefault()
      .name("sampleId").`type`().intType().noDefault()
      .name("buildId").`type`().stringType().noDefault()
      .name("simpleMeasurements").`type`().stringType().noDefault()
      .name("settings").`type`().stringType().noDefault()
      .name("addons").`type`().stringType().noDefault()
      .name("system").`type`().stringType().noDefault()
      .name("build").`type`().stringType().noDefault()
      .name("threadHangStats").`type`().stringType().noDefault()
      .name("histograms").`type`().stringType().noDefault()
      .name("keyedHistograms").`type`().stringType().noDefault()
      .name("childPayloads").`type`().stringType().noDefault()
      .endRecord
  }

  private def buildRecord(fields: Map[String, Any], schema: Schema): Option[GenericRecord] = {
    val addons = fields.getOrElse("environment.addons", "{}").asInstanceOf[String]
    val system = fields.getOrElse("environment.system", "{}").asInstanceOf[String]
    val build = fields.getOrElse("environment.build", "{}").asInstanceOf[String]
    val settings = fields.getOrElse("environment.settings", "").asInstanceOf[String]
    val JString(cohort) = parse(settings) \ "e10sCohort"

    val root = new GenericRecordBuilder(schema)
      .set("clientId", fields.getOrElse("clientId", ""))
      .set("e10sCohort", cohort)
      .set("creationTimestamp", fields.getOrElse("creationTimestamp", ""))
      .set("submissionDate", fields.getOrElse("submissionDate", ""))
      .set("documentId", fields.getOrElse("documentId", ""))
      .set("sampleId", fields.getOrElse("sampleId", -1))
      .set("buildId", fields.getOrElse("appBuildId", ""))
      .set("simpleMeasurements", fields.getOrElse("payload.simpleMeasurements", ""))
      .set("settings", fields.getOrElse("environment.settings", ""))
      .set("addons", addons)
      .set("system", system)
      .set("build", build)
      .set("threadHangStats", fields.getOrElse("payload.threadHangStats", ""))
      .set("histograms", fields.getOrElse("payload.histograms", ""))
      .set("keyedHistograms", fields.getOrElse("payload.keyedHistograms", ""))
      .set("childPayloads", fields.getOrElse("payload.childPayloads", "{}"))
      .build

    Some(root)
  }
}
