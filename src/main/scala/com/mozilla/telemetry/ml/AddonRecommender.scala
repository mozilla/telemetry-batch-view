package com.mozilla.telemetry.ml

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import breeze.linalg.{DenseMatrix, DenseVector}
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.mozilla.telemetry.utils.S3Store
import org.apache.spark.ml.evaluation.NaNRegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
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
      val outputBucket = opt[String]("bucket", descr = "Destination bucket for the model data", required = true)
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

  private def recommend(inputDir: String, addons: Set[String]) = {
    val mapping: List[(Int, String)] = for {
      JObject(a) <- parse(Source.fromFile(s"$inputDir/addon_mapping.json").mkString)
      JField(k, JString(v)) <- a
    } yield {
      (k.toInt, v)
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

  private def train(localOutputDir: String, runDate: Option[String], outputBucket: String) = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val date = runDate match {
      case Some(f) => f
      case _ => fmt.print(DateTime.now)
    }

    val s3prefix = s"telemetry-ml/addon_recommender/${date}"
    require(S3Store.isPrefixEmpty(outputBucket, s3prefix), s"s3://${outputBucket}/${s3prefix} already exists!")

    val clientAddons = hiveContext.sql("select * from longitudinal")
      .where("active_addons is not null")
      .selectExpr("client_id", "active_addons[0] as active_addons")
      .as[Addons]
      .flatMap{ case Addons(Some(clientId), Some(addons)) =>
        for {
          (addonId, meta) <- addons
          if !List("loop@mozilla.org","firefox@getpocket.com", "e10srollout@mozilla.org", "firefox-hotfix@mozilla.org").contains(addonId) &&
             AMODatabase.contains(addonId)
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
      .map{ case (_, addonId, _, hashedAddonId) => (hashedAddonId, AMODatabase.getAddonNameById(addonId))}
      .distinct
      .cache()
      .collect()
      .toMap

    val serializedMapping = pretty(render(addonMapping.map {case (k, v) => (k.toString, v)}))
    val addonMappingPath = Paths.get(s"$localOutputDir/addon_mapping.json")
    Files.write(addonMappingPath, serializedMapping.getBytes(StandardCharsets.UTF_8))
    S3Store.uploadFile(addonMappingPath.toFile, outputBucket, s3prefix, addonMappingPath.getFileName.toString)

    // Serialize item matrix
    val itemFactors = model.bestModel.asInstanceOf[ALSModel].itemFactors.as[ItemFactors].collect()
    val serializedItemFactors = write(itemFactors)
    val itemMatrixPath = Paths.get(s"$localOutputDir/item_matrix.json")
    Files.write(itemMatrixPath, serializedItemFactors.getBytes(StandardCharsets.UTF_8))
    S3Store.uploadFile(itemMatrixPath.toFile, outputBucket, s3prefix, itemMatrixPath.getFileName.toString)

    // Serialize model to HDFS and then copy it to the local machine. We need to do this
    // instead of simply saving to file:// due to permission issues.
    try {
      model.write.overwrite().save(s"$localOutputDir/als.model")
      // Run the copy as a shell command: unfortunately, FileSystem.copyToLocalFile
      // triggers the same permission issues that we experience when saving to file://.
      val copyCmdOutput = s"hdfs dfs -get $localOutputDir/als.model $localOutputDir/".!!
      logger.debug("Command output " + copyCmdOutput)
      // Save the model to S3 as well.
      model.write.overwrite().save(s"s3://$outputBucket/$s3prefix/als.model")
    } catch {
      // We failed to write the model to HDFS or there was a permission issue with the
      // copy command.
      case e: Exception => logger.error("Failed to write the model: " + e.getMessage)
    }

    logger.info("Cross validation statistics:")
    model.getEstimatorParamMaps
      .zip(model.avgMetrics)
      .foreach(logger.info)
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)

    conf.subcommand match {
      case Some(command) if command == conf.recommend =>
        val addons = conf.recommend.addons().split(",")
        val top = conf.recommend.top()
        val input = conf.recommend.input()
        recommend(input, addons.toSet).take(top).foreach(println)

      case Some(command) if command == conf.train =>
        val output = conf.train.output()
        val cwd = new java.io.File(".").getCanonicalPath
        val outputDir = new java.io.File(cwd, output)
        train(outputDir.getCanonicalPath, conf.train.runDate.get, conf.train.outputBucket())

      case None =>
        conf.printHelp()
    }
  }
}
