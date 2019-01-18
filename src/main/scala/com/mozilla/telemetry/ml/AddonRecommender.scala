/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.ml

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneOffset}

import breeze.linalg.{DenseMatrix, DenseVector}
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.mozilla.telemetry.utils.{S3Store, getOrCreateSparkSession}
import com.mozilla.telemetry.views.DatabricksSupport
import org.apache.spark.ml.evaluation.NaNRegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods.{parse, _}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.rogach.scallop._

import scala.collection.Map
import scala.io.Source
import scala.language.reflectiveCalls

object AddonRecommender extends DatabricksSupport {
  implicit val formats = Serialization.formats(NoTypeHints)
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)

  private class Conf(args: Array[String]) extends ScallopConf(args) {
    private def dateOffset(minusDays: Int) = {
      val date = LocalDate.now().minusDays(minusDays)
      val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      date.format(formatter)
    }

    val train = new Subcommand("train") {
      val output = opt[String]("output", descr = "Output path", required = true, default = Some("."))
      val runDate = opt[String]("runDate", descr = "The execution date", required = false)
      val privateBucket = opt[String]("privateBucket", descr = "Destination bucket for archiving the model data", required = true)
      val publicBucket = opt[String]("publicBucket", descr = "Destination bucket for the latest public model", required = true)
      val inputTable = opt[String]("inputTable", default = Some("clients_daily"),
        descr = "Input table, by default `clients_daily` is used", required = false)
      val clientsSampleDateFrom = opt[String]("clientsSampleDateFrom", default = Some(dateOffset(minusDays = 90)),
        descr = "Lower bound date for sampling `clients_daily`, formatted as `YYYYMMDD`, by default current date minus 90 days is used", required = false)
      val clientsSamplingFraction = opt[Int]("clientsSamplingFraction", default = Some(100),
        descr = "Fraction of `clients_daily` used for training - [1,100]", required = false,
        validate = f => f >= 1 && f <= 100)
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
  private[ml] def hash(x: String): Int = x.hashCode & 0x7FFFFF

  private def itemFactorsToMatrix(itemFactors: Array[ItemFactors]): DenseMatrix[Double] = {
    require(itemFactors.length > 0)

    val itemMatrixValues = itemFactors.flatMap {
      case ItemFactors(id: Int, features: Array[Float]) =>
        features.map(_.toDouble)
    }

    new DenseMatrix(itemFactors(0).features.length, itemFactors.length, itemMatrixValues)
  }

  /**
    * Get a new dataset from the clients_daily dataset containing only the relevant addon data.
    *
    * @param spark          The SparkSession used for loading clients_daily dataset.
    * @param addonWhitelist The a list of addon GUIDs that are valid
    * @param amoDB          The AMO database fetched by AMODatabase.
    * @param inputTable     Name of the input table (can be used to override clients_daily)
    * @param dateFrom       Lower threshold date for clients' sample
    * @param sampling       Percent of clients to include in sample, based on `sample_id` column
    * @return A 4 columns Dataset with each row having the client id, the addon GUID
    *         and the hashed client id and GUID.
    */
  private[ml] def getAddonData(spark: SparkSession, addonWhitelist: List[String], amoDB: Map[String, Any],
                               inputTable: String, dateFrom: String, sampling: Int): Dataset[(String, String, Int, Int)] = {
    import spark.implicits._
    spark.sql(s"SELECT * FROM $inputTable")
      .where("client_id IS NOT null")
      .where("active_addons IS NOT null")
      .where("channel = 'release'")
      .where("app_name = 'Firefox'")
      .where(s"submission_date_s3 >= '$dateFrom'")
      .where(s"sample_id <= '$sampling'")
      .selectExpr(
        "client_id",
        "active_addons",
        "submission_date_s3",
        "row_number() OVER (PARTITION BY client_id ORDER BY submission_date_s3 desc) as rn"
      )
      .where("rn = 1")
      .drop("rn")
      .as[ClientWithAddons]
      .flatMap { case ClientWithAddons(clientId, activeAddons) =>
        for {
          addon <- activeAddons
          addonId <- addon.addon_id
          if addonWhitelist.contains(addonId) && amoDB.contains(addonId)
          addonName <- addon.name
          blocklisted <- addon.blocklisted
          signedState <- addon.signed_state
          userDisabled <- addon.user_disabled
          appDisabled <- addon.app_disabled
          addonType <- addon.`type`
          isSystem <- addon.is_system
          if !blocklisted && (addonType != "extension" || signedState == 2) && !userDisabled && !appDisabled && !isSystem
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
  private def train(localOutputDir: String, runDate: String, privateBucket: String, publicBucket: String,
                    inputTable: String, dateFrom: String, sampling: Int) = {
    logger.info(s"Training - using clients_daily from $dateFrom")
    // The AMODatabase init needs to happen before we get the SparkContext,
    // otherwise the job will fail due to all the workers being idle.
    val amoDbMap = AMODatabase.getAddonMap()

    val spark = getOrCreateSparkSession("AddonRecommenderTest", enableHiveSupport = true)
    val sc = spark.sparkContext

    import spark.implicits._
    implicit val formats = DefaultFormats
    val istream = S3Store.getKey("telemetry-parquet", "telemetry-ml/addon_recommender/only_guids_top_200.json")
    val json_str = scala.io.Source.fromInputStream(istream).mkString
    val whitelist = parse(json_str).extract[List[String]]
    val clientAddons = getAddonData(spark, whitelist, amoDbMap, inputTable, dateFrom, sampling)

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
      .setParallelism(20)

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

    val parentDir = Paths.get(localOutputDir)
    if (!Files.exists(parentDir)) Files.createDirectories(parentDir)

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

    model.write.overwrite().save(s"s3://$privateBucket/$privateS3prefix/als.model")

    logger.info("Cross validation statistics:")
    model.getEstimatorParamMaps
      .zip(model.avgMetrics)
      .foreach(logger.info)

    if (shouldStopContextAtEnd(spark)) {
      spark.stop()
    }
  }
  // scalastyle:on methodLength

  def main(args: Array[String]) {
    val conf = new Conf(args)

    conf.subcommand match {
      case Some(command) if command == conf.recommend =>
        val addons = conf.recommend.addons().split(",")
        val top = conf.recommend.top()
        val input = conf.recommend.input()
        recommend(input, addons.toSet).take(top).foreach(logger.info)

      case Some(command) if command == conf.train =>
        val output = conf.train.output()
        val cwd = new java.io.File(".").getCanonicalPath
        val outputDir = new java.io.File(cwd, output)

        val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
        val date = conf.train.runDate.toOption match {
          case Some(f) => f
          case _ => Instant.now().atOffset(ZoneOffset.UTC).format(fmt)
        }

        train(outputDir.getCanonicalPath, date, conf.train.privateBucket(), conf.train.publicBucket(),
          conf.train.inputTable(), conf.train.clientsSampleDateFrom(), conf.train.clientsSamplingFraction())

      case None =>
        conf.printHelp()
    }
  }
}

private case class Rating(clientId: Int, addonId: Int, rating: Float)

private case class ItemFactors(id: Int, features: Array[Float])

case class ClientWithAddons(client_id: String,
                            active_addons: Array[ActiveAddon])

case class ClientsDailyRow(client_id: String,
                           app_name: String,
                           active_addons: Array[ActiveAddon],
                           channel: String,
                           sample_id: String,
                           submission_date_s3: String)

case class ActiveAddon(addon_id: Option[String],
                       app_disabled: Option[Boolean],
                       blocklisted: Option[Boolean],
                       foreign_install: Option[Boolean],
                       has_binary_components: Option[Boolean],
                       install_day: Option[Long],
                       is_system: Option[Boolean],
                       is_web_extension: Option[Boolean],
                       multiprocess_compatible: Option[Boolean],
                       name: Option[String],
                       scope: Option[Long],
                       signed_state: Option[Long],
                       `type`: Option[String],
                       update_day: Option[Long],
                       user_disabled: Option[Boolean],
                       version: Option[String])
