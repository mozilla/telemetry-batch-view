package telemetry.streams

import awscala._
import awscala.s3._
import java.io.File
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.spark.{SparkContext, Partitioner}
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import scala.util.Random
import telemetry.{DerivedStream, ObjectSummary}
import telemetry.DerivedStream.s3
import telemetry.heka.{HekaFrame, Message}
import telemetry.parquet.ParquetFile

private class SampleIdPartitioner extends Partitioner{
  def numPartitions: Int = 100
  def getPartition(key: Any): Int = key match {
    case (_, sampleId: Int) => sampleId
    case _ => throw new Exception("Invalid key")
  }
}

/*
Useful links re: e10sCohort experiment design and structure:

* An overview of the rationale: https://bugzilla.mozilla.org/show_bug.cgi?id=1251259
* Original bug for e10sCohort: https://bugzilla.mozilla.org/show_bug.cgi?id=1249845
* A brief description of e10sCohort's schema: https://github.com/mozilla-services/mozilla-pipeline-schemas/issues/4
* A later addition to the schema: https://bugzilla.mozilla.org/show_bug.cgi?id=1255013
*/

case class E10sExperiment(experimentId: String, prefix: String) extends DerivedStream {
  override def streamName: String = "telemetry"

  override def filterPrefix: String = prefix

  override def transform(sc: SparkContext, bucket: Bucket, summaries: RDD[ObjectSummary], from: String, to: String) {
    val tmp = experimentId.replaceAll("-", "_")
    val prefix = s"$tmp/v$to"

    if (!isS3PrefixEmpty(prefix)) {
      println(s"Warning: prefix $prefix already exists on S3!")
      return
    }

    val groups = DerivedStream.groupBySize(summaries.collect().toIterator)
    val clientMessages = sc.parallelize(groups, groups.size)
      .flatMap(x => x)
      .flatMap{ case obj =>
        val hekaFile = bucket.getObject(obj.key).getOrElse(throw new Exception("File missing on S3"))
        for (message <- HekaFrame.parse(hekaFile.getObjectContent(), hekaFile.getKey()))  yield message }
      .flatMap{ case message =>
        val fields = HekaFrame.fields(message)
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
            List(((client, sample.toInt), fields))
          case _ => Nil
        }
      }

    val representativeSubmissions = clientMessages
      .mapValues{case x => (x, 1)}
      .reduceByKey{ case ((p1, n1), (p2, n2)) =>
        // Every submission should have the same probability of being chosen
        val n = n1 + n2
        val rnd = new Random().nextFloat()
        if (n * rnd < n1) (p1, n) else (p2, n)
      }
      .mapValues{case x => x._1}
      .partitionBy(new SampleIdPartitioner())
      .values
      .foreachPartition{ case fieldsIterator =>
        val schema = buildSchema
        val records = for {
          fields <- fieldsIterator
          record <- buildRecord(fields, schema)
          } yield record

        while(!records.isEmpty) {
          val localFile = ParquetFile.serialize(records, schema)
          uploadLocalFileToS3(localFile, prefix)
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
