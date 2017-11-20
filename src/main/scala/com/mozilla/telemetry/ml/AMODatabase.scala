package com.mozilla.telemetry.ml

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import scala.annotation.tailrec
import scala.collection.Map
import scala.io.Source
import scalaj.http.Http

private case class AMOAddonPage(previous: String, next: String, results: List[AMOAddonInfo])
case class AMOAddonFile(id: Long, platform: String, status: String, is_webextension: Boolean)
case class AMOAddonVersion(files: List[AMOAddonFile])
case class AMOAddonInfo(guid: String,
                        categories: Map[String, List[String]],
                        default_locale: String,
                        description: Option[Map[String, String]],
                        name: Map[String,String],
                        current_version: AMOAddonVersion,
                        ratings: Map[String, Double],
                        summary: Option[Map[String, String]],
                        tags: List[String],
                        weekly_downloads: Long)

/**
  * The AMODatabase singleton encapsulates a local version of the addons.mozilla.org (AMO)
  * database by using the provided public API.
  *
  * There are non-publicly available addons on AMO. This could happen because, the add-on:
  * - has been disabled by the developer;
  * - has been rejected;
  * - it's unlisted. The developer has submitted it to AMO for it to be signed but does not
  *   want us to distribute it, they'll handle that themselves;
  * - its developer started the submission process but didn't finish it, so the AMO entry has
  *   incomplete information.
  *
  * In all these cases, the public unauthenticated a.m.o API won't return the addon information.
  */
final object AMODatabase {
  implicit val formats = Serialization.formats(NoTypeHints)
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)
  // This API URI will fetch all the public addons for Firefox, sorting them by creation date.
  private val defaultAMORequestURI = "https://addons.mozilla.org/api/v3/addons/search/"
  private val queryParams = "?app=firefox&sort=created&type=extension"

  /**
    * Fetch the remote AMO database by using the /search API endpoint.
    * @param requestUri The AMO URI to query.
    * @return A map of addon GUIDs to their info.
    */
  private def fetchAddonsDatabase(requestUri: Option[String]): Map[String, AMOAddonInfo] = {
    @tailrec
    def fetchAMOPage(requestUri: Option[String], addonMap: Map[String, AMOAddonInfo]): Map[String, AMOAddonInfo] = {
      requestUri match {
        case Some(pageUri) => {
          logger.info(s"Fetching $pageUri")

          // Fetch the JSON results for this "page" and parse it using json4s.
          val responseBody = Http(pageUri).asString.body
          val parsedObj = parse(responseBody)

          // We're only interested in a few fields for each addon (e.g. locale).
          // De-serialize the JSON data into classes and then build the GUID -> Addon map.
          val data = parsedObj.extract[AMOAddonPage]
          val partialMap = data.results.map(addon => addon.guid -> addon).toMap

          // Merge the map containing the addon info from this page to the complete map.
          val nextPageURI = (parsedObj \ "next").extractOpt[String]
          fetchAMOPage(nextPageURI, addonMap ++ partialMap)
        }
        case None => addonMap
      }
    }
    fetchAMOPage(requestUri, Map())
  }

  /**
    * Fetch a copy of the AMO addons database or return a cached local copy if
    * available.
    * @param apiURI The AMO api URI to use to fetch the data.
    * @return A map of addon GUIDs to their info.
    */
  private def getDatabase(apiURI: String): Map[String, AMOAddonInfo] = synchronized {
    // If we have a cached copy of the request handy, use that. Please note that
    // the "read-from-disk" functionality is only needed for testing purposes, so
    // cache invalidation isn't a real issue.
    val dbPath = getLocalCachePath()
    if (Files.exists(dbPath)) {
      logger.info(s"Hitting addon database cache at $dbPath")
      parse(Source.fromFile(dbPath.toString()).mkString).extract[Map[String, AMOAddonInfo]]
    } else {
      // Otherwise fetch it from addons.mozilla.org (might take some time..) and cache it.
      val fetchedAddonsMap = fetchAddonsDatabase(Some(s"$apiURI$queryParams"))
      val serializedFetchedAddons = write(fetchedAddonsMap)
      Files.write(dbPath, serializedFetchedAddons.getBytes(StandardCharsets.UTF_8))
      fetchedAddonsMap
    }
  }

  /**
    * Initialize this object by downloading the AMO addons.
    * @param apiURI The AMO api URI to use to fetch the data.
    */
  def getAddonMap(apiURI: String = defaultAMORequestURI): Map[String, AMOAddonInfo] = {
    logger.info(s"Downloading AMO data from $apiURI")
    getDatabase(apiURI)
  }

  /**
    * Get the name of the desired addon, in the default locale.
    * @param addonId The GUID for the desired addon.
    * @return The addon name, if available, or an empty Option. The name should always be available,
    *         unless the database is corrupted.
    */
  def getAddonNameById(addonId: String, addonDB: Map[String, AMOAddonInfo]): Option[String] = {
    // The addon info contains the default_locale for the addon as a mandatory field, which is a string
    // containing the name of the default locale (e.g. "it_IT"). This is the locale used as a reference
    // for translations and, as such, the "preferred" one.
    addonDB.get(addonId) match {
      case Some(addonInfo) => addonInfo.name.get(addonInfo.default_locale)
      case None =>
        logger.warn(s"Addon GUID not in AMO DB: $addonId")
        None
    }
  }

  /**
    * Check if the provided addon is a webextension or a legacy addon.
    * @param addonId The GUID for the desired addon.
    * @return True if the latest version of the addon uses Webextensions, False otherwise.
    */
  def isWebextension(addonId: String, addonDB: Map[String, AMOAddonInfo]): Option[Boolean] = {
    // Each adddon can contain multiple files for each version. To ensure the addon is not
    // a legacy addon, we verify that the latest version contains at least a Webextension
    // compatible file which is publicly visible (status = "public").
    addonDB.get(addonId) match {
      case Some(addonInfo) => {
        Some(addonInfo.current_version.files.exists(
          fileInfo => fileInfo.is_webextension && fileInfo.status.equalsIgnoreCase("public")))
      }
      case None =>
        logger.warn(s"Addon GUID not in AMO DB: $addonId")
        None
    }
  }

  /**
    * Get the path to the local cache file.
    * @return A Path object representing the location of the addons cache file.
    */
  def getLocalCachePath() : Path = {
    Paths.get("./addons_database.json")
  }
}
