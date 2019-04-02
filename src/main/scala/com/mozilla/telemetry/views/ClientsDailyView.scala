/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views

import com.mozilla.telemetry.utils.getOrCreateSparkSession
import com.mozilla.telemetry.utils.udfs.{AggMapFirst, AggMapSum, AggRowFirst, AggSearchCounts}
import com.mozilla.telemetry.views.MainSummaryView.buildAddonSchema
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.rogach.scallop._


object ClientsDailyView {
  // allow visibility within "views" for testing
  private[views] val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)
  private val jobName: String = "clients_daily"
  private val schemaVersion: String = "v6"
  private val fieldAggregators = List(
    aggSum("aborts_content"),
    aggSum("aborts_gmplugin"),
    aggSum("aborts_plugin"),
    aggAddonsFirst("active_addons"),
    aggMean("active_addons_count"),
    // active_hours_sum has to be coerced from decimal to double for backwards compatibility
    aggSum(expr("DOUBLE(active_ticks/(3600.0/5))"), "active_hours_sum"),
    aggFirst("addon_compatibility_check_enabled"),
    aggFirst("app_build_id"),
    aggFirst("app_display_version"),
    aggFirst("app_name"),
    aggFirst("app_version"),
    aggFirst("attribution"),
    aggFirst("blocklist_enabled"),
    aggFirst("channel"),
    aggFirst(expr("IF(country IS NOT NULL AND country != '??', IF(city IS NOT NULL, city, '??'), NULL)"), "city"),
    aggMean("client_clock_skew"),
    aggMean("client_submission_latency"),
    aggFirst(expr("IF(country IS NOT NULL AND country != '??', country, NULL)"), "country"),
    aggFirst("cpu_cores"),
    aggFirst("cpu_count"),
    aggFirst("cpu_family"),
    aggFirst("cpu_l2_cache_kb"),
    aggFirst("cpu_l3_cache_kb"),
    aggFirst("cpu_model"),
    aggFirst("cpu_speed_mhz"),
    aggFirst("cpu_stepping"),
    aggFirst("cpu_vendor"),
    aggSum("crashes_detected_content"),
    aggSum("crashes_detected_gmplugin"),
    aggSum("crashes_detected_plugin"),
    aggSum("crash_submit_attempt_content"),
    aggSum("crash_submit_attempt_main"),
    aggSum("crash_submit_attempt_plugin"),
    aggSum("crash_submit_success_content"),
    aggSum("crash_submit_success_main"),
    aggSum("crash_submit_success_plugin"),
    aggFirst("default_search_engine"),
    aggFirst("default_search_engine_data_load_path"),
    aggFirst("default_search_engine_data_name"),
    aggFirst("default_search_engine_data_origin"),
    aggFirst("default_search_engine_data_submission_url"),
    aggSum("devtools_toolbox_opened_count"),
    aggFirst("distribution_id"),
    aggFirst("e10s_enabled"),
    aggFirst("env_build_arch"),
    aggFirst("env_build_id"),
    aggFirst("env_build_version"),
    aggMapFirst("experiments"),
    aggMean("first_paint"),
    aggFirst("flash_version"),
    aggFirst(expr("IF(country IS NOT NULL AND country != '??', IF(geo_subdivision1 IS NOT NULL, geo_subdivision1, '??'), NULL)"), "geo_subdivision1"),
    aggFirst(expr("IF(country IS NOT NULL AND country != '??', IF(geo_subdivision2 IS NOT NULL, geo_subdivision2, '??'), NULL)"), "geo_subdivision2"),
    aggFirst("gfx_features_advanced_layers_status"),
    aggFirst("gfx_features_d2d_status"),
    aggFirst("gfx_features_d3d11_status"),
    aggFirst("gfx_features_gpu_process_status"),
    aggSum("histogram_parent_devtools_aboutdebugging_opened_count"),
    aggSum("histogram_parent_devtools_animationinspector_opened_count"),
    aggSum("histogram_parent_devtools_browserconsole_opened_count"),
    aggSum("histogram_parent_devtools_canvasdebugger_opened_count"),
    aggSum("histogram_parent_devtools_computedview_opened_count"),
    aggSum("histogram_parent_devtools_custom_opened_count"),
    aggSum("histogram_parent_devtools_dom_opened_count"),
    aggSum("histogram_parent_devtools_eyedropper_opened_count"),
    aggSum("histogram_parent_devtools_fontinspector_opened_count"),
    aggSum("histogram_parent_devtools_inspector_opened_count"),
    aggSum("histogram_parent_devtools_jsbrowserdebugger_opened_count"),
    aggSum("histogram_parent_devtools_jsdebugger_opened_count"),
    aggSum("histogram_parent_devtools_jsprofiler_opened_count"),
    aggSum("histogram_parent_devtools_layoutview_opened_count"),
    aggSum("histogram_parent_devtools_memory_opened_count"),
    aggSum("histogram_parent_devtools_menu_eyedropper_opened_count"),
    aggSum("histogram_parent_devtools_netmonitor_opened_count"),
    aggSum("histogram_parent_devtools_options_opened_count"),
    aggSum("histogram_parent_devtools_paintflashing_opened_count"),
    aggSum("histogram_parent_devtools_picker_eyedropper_opened_count"),
    aggSum("histogram_parent_devtools_responsive_opened_count"),
    aggSum("histogram_parent_devtools_ruleview_opened_count"),
    aggSum("histogram_parent_devtools_scratchpad_opened_count"),
    aggSum("histogram_parent_devtools_scratchpad_window_opened_count"),
    aggSum("histogram_parent_devtools_shadereditor_opened_count"),
    aggSum("histogram_parent_devtools_storage_opened_count"),
    aggSum("histogram_parent_devtools_styleeditor_opened_count"),
    aggSum("histogram_parent_devtools_webaudioeditor_opened_count"),
    aggSum("histogram_parent_devtools_webconsole_opened_count"),
    aggSum("histogram_parent_devtools_webide_opened_count"),
    aggFirst("install_year"),
    aggFirst("is_default_browser"),
    aggFirst("is_wow64"),
    aggFirst("locale"),
    aggFirst("memory_mb"),
    aggFirst("normalized_channel"),
    aggFirst("normalized_os_version"),
    aggFirst("os"),
    aggFirst("os_service_pack_major"),
    aggFirst("os_service_pack_minor"),
    aggFirst("os_version"),
    countDistinct("document_id").alias("pings_aggregated_by_this_row"),
    aggMean("places_bookmarks_count"),
    aggMean("places_pages_count"),
    aggSum("plugin_hangs"),
    aggSum("plugins_infobar_allow"),
    aggSum("plugins_infobar_block"),
    aggSum("plugins_infobar_shown"),
    aggSum("plugins_notification_shown"),
    aggFirst("previous_build_id"),
    aggFirst(
      expr(
        "datediff(subsession_start_date, from_unixtime(profile_creation_date*24*60*60))"
      ),
      "profile_age_in_days"),
    aggFirst(
      expr("from_unixtime(profile_creation_date*24*60*60)"),
      "profile_creation_date"),
    aggSum("push_api_notify"),
    aggFirst("sample_id"),
    aggFirst("sandbox_effective_content_process_level"),
    aggSum(
      col("scalar_parent_webrtc_nicer_stun_retransmits") + col("scalar_content_webrtc_nicer_stun_retransmits"),
      "scalar_combined_webrtc_nicer_stun_retransmits_sum"
    ),
    aggSum(col("scalar_parent_webrtc_nicer_turn_401s") + col("scalar_content_webrtc_nicer_turn_401s"), "scalar_combined_webrtc_nicer_turn_401s_sum"),
    aggSum(col("scalar_parent_webrtc_nicer_turn_403s") + col("scalar_content_webrtc_nicer_turn_403s"), "scalar_combined_webrtc_nicer_turn_403s_sum"),
    aggSum(col("scalar_parent_webrtc_nicer_turn_438s") + col("scalar_content_webrtc_nicer_turn_438s"), "scalar_combined_webrtc_nicer_turn_438s_sum"),
    aggSum("scalar_content_navigator_storage_estimate_count"),
    aggSum("scalar_content_navigator_storage_persist_count"),
    aggFirst("scalar_parent_aushelper_websense_reg_version"),
    aggMax("scalar_parent_browser_engagement_max_concurrent_tab_count"),
    aggMax("scalar_parent_browser_engagement_max_concurrent_window_count"),
    aggSum("scalar_parent_browser_engagement_tab_open_event_count"),
    aggSum("scalar_parent_browser_engagement_total_uri_count"),
    aggSum("scalar_parent_browser_engagement_unfiltered_uri_count"),
    aggMax("scalar_parent_browser_engagement_unique_domains_count"),
    aggMean("scalar_parent_browser_engagement_unique_domains_count"),
    aggSum("scalar_parent_browser_engagement_window_open_event_count"),
    aggSum("scalar_parent_devtools_accessibility_node_inspected_count"),
    aggSum("scalar_parent_devtools_accessibility_opened_count"),
    aggSum("scalar_parent_devtools_accessibility_picker_used_count"),
    aggMapSum("scalar_parent_devtools_accessibility_select_accessible_for_node"),
    aggSum("scalar_parent_devtools_accessibility_service_enabled_count"),
    aggSum("scalar_parent_devtools_copy_full_css_selector_opened"),
    aggSum("scalar_parent_devtools_copy_unique_css_selector_opened"),
    aggSum("scalar_parent_devtools_toolbar_eyedropper_opened"),
    aggSum("scalar_parent_navigator_storage_estimate_count"),
    aggSum("scalar_parent_navigator_storage_persist_count"),
    aggSum("scalar_parent_storage_sync_api_usage_extensions_using"),
    aggFirst("search_cohort"),
    aggSearchCounts("search_counts"),
    aggMean("session_restored"),
    aggSum(expr("IF(subsession_counter = 1, 1, 0)"), "sessions_started_on_this_day"),
    aggSum("shutdown_kill"),
    aggSum(expr("subsession_length/3600.0").cast(DecimalType(35,6)), "subsession_hours_sum"),
    aggSum("ssl_handshake_result_failure"),
    aggSum("ssl_handshake_result_success"),
    aggFirst("sync_configured"),
    aggSum("sync_count_desktop"),
    aggSum("sync_count_mobile"),
    aggFirst("telemetry_enabled"),
    aggFirst("timezone_offset"),
    aggFirst("update_auto_download"),
    aggFirst("update_channel"),
    aggFirst("update_enabled"),
    aggFirst("vendor"),
    aggSum("web_notification_shown"),
    aggFirst("windows_build_number"),
    aggFirst("windows_ubr"),
    aggFirstNonemptyList("environment_settings_intl_accept_languages"),
    aggFirstNonemptyList("environment_settings_intl_app_locales"),
    aggFirstNonemptyList("environment_settings_intl_available_locales"),
    aggFirstNonemptyList("environment_settings_intl_regional_prefs_locales"),
    aggFirstNonemptyList("environment_settings_intl_requested_locales"),
    aggFirstNonemptyList("environment_settings_intl_system_locales")
  )

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val date = conf.date()
    val maxRecordsPerFile = conf.maxRecordsPerFile()

    val inputPath = s"s3://${conf.inputBucket()}/main_summary/" +
      s"${MainSummaryView.schemaVersion}/submission_date_s3=$date"
    val outputPath = s"s3://${conf.outputBucket()}/clients_daily/" +
      s"$schemaVersion/submission_date_s3=$date"

    val spark = getOrCreateSparkSession(jobName)

    val df = spark.read.parquet(inputPath)
    val input = conf.sampleId.get match {
      case Some(sampleId) => df.where(s"sample_id = '$sampleId'")
      case _ => df
    }

    val results = extractDayAggregates(input)

    results
      .write
      .mode("overwrite")
      .option("maxRecordsPerFile", maxRecordsPerFile)
      .parquet(outputPath)
  }

  def extractDayAggregates(df: DataFrame): DataFrame = {
    // determine fieldAggregators that can be used against this DataFrame
    val agg = Some(fieldAggregators
      // unresolvedAttributeNames gets the names of columns used as inputs to an expression
      .flatMap(unresolvedAttributeNames)
      // filter out the column names that are present in df
      .toSet -- df.columns)
      // filter Some(missingColumns) to None unless nonEmpty
      .filter(_.nonEmpty)
      // filter expressions using missingColumns out of fieldAggregators
      .map { missingColumns =>
        logger.warn(s"JOB $jobName $schemaVersion MISSING INPUT COLUMNS: ${missingColumns.mkString(", ")}")
        fieldAggregators
          .filter(unresolvedAttributeNames(_)
            .forall(!missingColumns.contains(_)))
      }
      // let spark throw AnalysisException if we filtered all aggregates
      .filter(_.nonEmpty)
      // default to fieldAggregators if there were no missing columns
      .getOrElse(fieldAggregators)

    // perform aggregation
    val aggregates = df
      .groupBy("client_id")
      .agg(agg.head, agg.tail: _*)

    /* expand search_counts with "search_counts.*". fields in search_counts
     * are prefixed with "search_count_" so this is safe
     */
    aggregates
      .selectExpr(
        aggregates.schema.map { c => if (c.name == "search_counts") "search_counts.*" else c.name }: _*
      )
  }

  private def unresolvedAttributeNames(c: Column): Seq[String] = c
    .expr
    .collectLeaves
    .flatMap {
      case attr: UnresolvedAttribute => Some(attr.name)
      case _ => None
    }

  private def aggFirst(field: String): Column = first(field, ignoreNulls = true).alias(field)

  private def aggFirst(expression: Column, alias: String): Column = first(expression, ignoreNulls = true).alias(alias)

  private def aggFirstNonemptyList(field: String): Column = first(expr(s"IF(size($field) == 0, NULL, $field)"), ignoreNulls = true).alias(field)

  private def aggMapFirst(field: String): Column = {
    val mapFirst = new AggMapFirst()
    mapFirst(col(field)).alias(field)
  }

  private def aggMapSum(field: String): Column = {
    val mapSum = new AggMapSum()
    mapSum(col(field)).alias(s"${field}_sum")
  }

  private def aggMax(field: String): Column = max(field).alias(s"${field}_max")

  private def aggMean(field: String): Column = mean(field).alias(s"${field}_mean")

  private def aggAddonsFirst(field: String): Column = {
    val addonsFirst = new AggRowFirst[String](buildAddonSchema, 0, StringType)
    addonsFirst(col(field)).alias(field)
  }

  private def aggSearchCounts(field: String): Column = {
    val searchSources = List(
      "abouthome",
      "contextmenu",
      "newtab",
      "searchbar",
      "system",
      "urlbar"
    )
    val searchCounts = new AggSearchCounts(searchSources)
    searchCounts(col(field)).alias(field)
  }

  private def aggSum(field: String): Column = sum(field).alias(s"${field}_sum")

  private def aggSum(expression: Column, alias: String): Column = sum(expression).alias(alias)

  class Conf(args: Array[String]) extends ScallopConf(args) {
    val date = opt[String](
      "date",
      descr = "Submission date to process",
      required = true)
    val inputBucket = opt[String](
      "input-bucket",
      descr = "Source bucket for main_summary data",
      required = false,
      default = Some("telemetry-parquet"))
    val outputBucket = opt[String](
      "output-bucket",
      descr = "Destination bucket for parquet data",
      required = true)
    val sampleId = opt[String](
      "sample-id",
      descr = "Sample_id to restrict results to",
      required = false)
    // 2,000,000 rows yields ~ 200MB files in snappy+parquet
    val maxRecordsPerFile = opt[Int](
      "max-records-per-file",
      descr = "Max number of rows to write to output files before splitting",
      required = false,
      default = Some(2000000))
    verify()
  }

}
