/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.ml

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import breeze.linalg.{DenseMatrix, DenseVector}
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.mozilla.telemetry.utils.{S3Store, getOrCreateSparkSession}
import org.apache.spark.ml.evaluation.NaNRegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, format}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.rogach.scallop._

import scala.collection.Map
import scala.io.Source
import scala.sys.process._

private case class Rating(clientId: Int, addonId: Int, rating: Float)
private case class Addons(client_id: Option[String], active_addons: Option[Map[String, Addon]])
private case class Addon(blocklisted: Option[Boolean],
                         description: Option[String],
                         name: Option[String],
                         user_disabled: Option[Boolean],
                         app_disabled: Option[Boolean],
                         version: Option[String],
                         scope: Option[Int],
                         `type`: Option[String],
                         foreign_install: Option[Boolean],
                         has_binary_components: Option[Boolean],
                         install_day: Option[Long],
                         update_day: Option[Long],
                         signed_state: Option[Int],
                         is_system: Option[Boolean])

private case class ItemFactors(id: Int, features: Array[Float])

object AddonRecommender {
  implicit val formats = Serialization.formats(NoTypeHints)
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val train = new Subcommand("train") {
      val output = opt[String]("output", descr = "Output path", required = true, default = Some("."))
      val runDate = opt[String]("runDate", descr = "The execution date", required = false)
      val privateBucket = opt[String]("privateBucket", descr = "Destination bucket for archiving the model data", required = true)
      val publicBucket = opt[String]("publicBucket", descr = "Destination bucket for the latest public model", required = true)
    }

    val recommend = new Subcommand("recommend") {
      val input = opt[String]("input", descr = "Input path", required = true, default = Some("."))
      val addons = opt[String]("addons", descr = "Comma separated list of add-on names", required = true)
      val top = opt[Int]("top", descr = "Number of recommendations to show", required = false, default = Some(10))
    }

    addSubcommand(train)
    addSubcommand(recommend)
    verify()
  }

  private class ItemFactorSerializer extends CustomSerializer[ItemFactors](format => (
    {
      case JObject(JField("jsonClass", JString("ItemFactors")) :: JField("id", JInt(id)) :: JField("factors", data_) :: Nil) =>
        val data = data_.extract[Array[Float]]
        ItemFactors(id.toInt, data)
    },
    {
      case f: ItemFactors =>
        JObject(JField("jsonClass", JString("ItemFactors")) ::
          JField("id", JInt(f.id)) ::
          JField("items", Extraction.decompose(f.features)) ::
          Nil)
    }
    ))

  // Positive hash function
  private def hash(x: String): Int = x.hashCode & 0x7FFFFF

  private def itemFactorsToMatrix(itemFactors: Array[ItemFactors]): DenseMatrix[Double] = {
    require(itemFactors.length > 0)

    val itemMatrixValues = itemFactors.flatMap {
      case ItemFactors(id: Int, features: Array[Float]) =>
        features.map(_.toDouble)
    }

    new DenseMatrix(itemFactors(0).features.length, itemFactors.length, itemMatrixValues)
  }

  /**
    * Get a new dataset from the Longitudinal dataset containing only the relevant addon data.
    * @param sparkSession The SparkSession used for loading the Longitudinal dataset.
    * @param addonBlacklist The a list of addon GUIDs to exclude.
    * @param amoDB The AMO database fetched by AMODatabase.
    * @return A 4 columns Dataset with each row having the client id, the addon GUID
    *         and the hashed client id and GUID.
    */
  private def getAddonData(sparkSession: SparkSession, addonBlacklist: List[String], amoDB: Map[String, Any]) = {
    import sparkSession.sqlContext.implicits._
    sparkSession.sqlContext.sql("select * from longitudinal")
      .where("active_addons is not null")
      .where("build is not null and build[0].application_name = 'Firefox'")
      .selectExpr("client_id", "active_addons[0] as active_addons")
      .as[Addons]
      .flatMap { case Addons(Some(clientId), Some(addons)) =>
        for {
          (addonId, meta) <- addons
          if !addonBlacklist.contains(addonId) && amoDB.contains(addonId)
          addonName <- meta.name
          blocklisted <- meta.blocklisted
          signedState <- meta.signed_state
          userDisabled <- meta.user_disabled
          appDisabled <- meta.app_disabled
          addonType <- meta.`type`
          if !blocklisted && (addonType != "extension" || signedState == 2) && !userDisabled && !appDisabled
        } yield {
          (clientId, addonId, hash(clientId), hash(addonId))
        }
      }
  }

  private def recommend(inputDir: String, addons: Set[String]) = {
    val mapping: List[(Int, String)] = for {
      JObject(a) <- parse(Source.fromFile(s"$inputDir/addon_mapping.json").mkString)
      JField(k, JObject(v)) <- a
    } yield {
      (k.toInt, v.toMap.get("name") match {
        case Some(s) => s.extract[String]
        case None => "Unknown"
      })
    }
    val addonMapping = mapping.toMap

    val itemFactors = parse(Source.fromFile(s"$inputDir/item_matrix.json").mkString).extract[Array[ItemFactors]]
    val itemMatrix = itemFactorsToMatrix(itemFactors)

    val addonVector = {
      val hashed = addons.map(hash)
      val binary = itemFactors.map {
        case ItemFactors(addonId: Int, _) =>
          if (hashed.contains(addonId)) 1.0 else 0.0
      }
      new DenseVector(binary)
    }

    // Approximate representation of the user in latent space
    val userFactors = (addonVector.t * itemMatrix.t).t

    // Compute distance between the user and all the add-ons in latent space
    itemFactors.map { case ItemFactors(addonId: Int, features: Array[Float]) =>
      val dp = blas.ddot(userFactors.length, userFactors.toArray, 1, features.map(_.toDouble), 1)
      (addonMapping(addonId), dp)
    }.sortWith((x, y) => x._2 > y._2)
  }

  // scalastyle:off methodLength
  private def train(localOutputDir: String, runDate: String, privateBucket: String, publicBucket: String) = {
    // The AMODatabase init needs to happen before we get the SparkContext,
    // otherwise the job will fail due to all the workers being idle.
    val amoDbMap = AMODatabase.getAddonMap()

    val spark = getOrCreateSparkSession("AddonRecommenderTest", enableHiveSupport = true)
    val sc = spark.sparkContext
    import spark.implicits._

    val blacklist = List("loop@mozilla.org", "firefox@getpocket.com", "e10srollout@mozilla.org", "firefox-hotfix@mozilla.org")
    val clientAddons = getAddonData(spark, blacklist, amoDbMap)

    val ratings = clientAddons
      .map{ case (_, _, hashedClientId, hashedAddonId) => Rating(hashedClientId, hashedAddonId, 1.0f)}
      .repartition(sc.defaultParallelism)
      .toDF
      .cache

    val als = new ALS()
      .setSeed(42)
      .setMaxIter(20)
      .setImplicitPrefs(true)
      .setUserCol("clientId")
      .setItemCol("addonId")
      .setRatingCol("rating")

    val evaluator = new NaNRegressionEvaluator()
      .setMetricName("rmse")
      .setDropNaN(true)
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val paramGrid = new ParamGridBuilder()
      .addGrid(als.rank, Array(15, 25, 35))
      .addGrid(als.regParam, Array(0.01, 0.1))
      .addGrid(als.alpha, Array(1.0, 10, 20))
      .build

    val cv = new CrossValidator()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val model = cv.fit(ratings)

    // Serialize add-on mapping
    val addonMapping = clientAddons
      .map{ case (_, addonId, _, hashedAddonId) => (hashedAddonId, addonId)}
      .distinct
      .cache()
      .collect()
      .toMap

    val serializedMapping = pretty(render(addonMapping.map {
      case (k, addonId) =>
        (k.toString,
          Map[String, JValue]("name" -> JString(AMODatabase.getAddonNameById(addonId, amoDbMap).getOrElse("Unknown")),
                              "id" -> addonId,
                              "isWebextension" -> JBool(AMODatabase.isWebextension(addonId, amoDbMap).getOrElse(false))).toMap)
    }))
    val addonMappingPath = Paths.get(s"$localOutputDir/addon_mapping.json")
    val privateS3prefix = s"telemetry-ml/addon_recommender/${runDate}"
    val latestS3prefix = "telemetry-ml/addon_recommender"
    Files.write(addonMappingPath, serializedMapping.getBytes(StandardCharsets.UTF_8))
    // We upload the generated data in a private bucket to have an history of all the
    // generated models. Also push the latest version on the public bucket, so that
    // AMO can access it over HTTP.
    S3Store.uploadFile(addonMappingPath.toFile, privateBucket, privateS3prefix, addonMappingPath.getFileName.toString)
    S3Store.uploadFile(addonMappingPath.toFile, publicBucket, latestS3prefix, addonMappingPath.getFileName.toString)

    // Serialize item matrix
    val itemFactors = model.bestModel.asInstanceOf[ALSModel].itemFactors.as[ItemFactors].collect()
    val serializedItemFactors = write(itemFactors)
    val itemMatrixPath = Paths.get(s"$localOutputDir/item_matrix.json")
    Files.write(itemMatrixPath, serializedItemFactors.getBytes(StandardCharsets.UTF_8))
    S3Store.uploadFile(itemMatrixPath.toFile, privateBucket, privateS3prefix, itemMatrixPath.getFileName.toString)
    S3Store.uploadFile(itemMatrixPath.toFile, publicBucket, latestS3prefix, itemMatrixPath.getFileName.toString)

    // Upload the generated AMO cache to a S3 bucket.
    val addonCachePath = AMODatabase.getLocalCachePath()
    S3Store.uploadFile(addonCachePath.toFile, privateBucket, privateS3prefix, addonCachePath.getFileName.toString)
    S3Store.uploadFile(addonCachePath.toFile, privateBucket, latestS3prefix, addonCachePath.getFileName.toString)

    // Serialize model to HDFS and then copy it to the local machine. We need to do this
    // instead of simply saving to file:// due to permission issues.
    try {
      model.write.overwrite().save(s"$localOutputDir/als.model")
      // Run the copy as a shell command: unfortunately, FileSystem.copyToLocalFile
      // triggers the same permission issues that we experience when saving to file://.
      val copyCmdOutput = s"hdfs dfs -get $localOutputDir/als.model $localOutputDir/".!!
      logger.debug("Command output " + copyCmdOutput)
      // Save the model to S3 as well. We don't need to upload that in the public bucket.
      model.write.overwrite().save(s"s3://$privateBucket/$privateS3prefix/als.model")
    } catch {
      // We failed to write the model to HDFS or there was a permission issue with the
      // copy command.
      case e: Exception => logger.error("Failed to write the model: " + e.getMessage)
    }

    logger.info("Cross validation statistics:")
    model.getEstimatorParamMaps
      .zip(model.avgMetrics)
      .foreach(logger.info)

    sc.stop()
  }
  // scalastyle:on methodLength

  def main(args: Array[String]) {
    val conf = new Conf(args)

    conf.subcommand match {
      case Some(command) if command == conf.recommend =>
        val addons = conf.recommend.addons().split(",")
        val top = conf.recommend.top()
        val input = conf.recommend.input()
        // scalastyle:off print-ln
        recommend(input, addons.toSet).take(top).foreach(println)
        // scalastyle:on print-ln

      case Some(command) if command == conf.train =>
        val output = conf.train.output()
        val cwd = new java.io.File(".").getCanonicalPath
        val outputDir = new java.io.File(cwd, output)

        val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
        val date = conf.train.runDate.get match {
          case Some(f) => f
          case _ => fmt.print(DateTime.now)
        }

        train(outputDir.getCanonicalPath, date, conf.train.privateBucket(), conf.train.publicBucket())

      case None =>
        conf.printHelp()
    }
  }
}
