package com.mozilla.telemetry.views

object ClientsDailyViewTestPayloads {
  case class SearchCount(
    engine: Option[String] = None,
    source: Option[String] = None,
    count: Option[Long] = None
  )

  case class Attribution(
    source: Option[String] = None,
    medium: Option[String] = None,
    campaign: Option[String] = None,
    content: Option[String] = None
  )

  case class MainSummaryRow(
    aborts_content: Option[Int] = None,
    aborts_gmplugin: Option[Int] = None,
    aborts_plugin: Option[Int] = None,
    active_addons_count: Option[Long] = None,
    active_experiment_branch: Option[String] = None,
    active_experiment_id: Option[String] = None,
    active_ticks: Option[Int] = None,
    addon_compatibility_check_enabled: Option[Boolean] = None,
    app_build_id: Option[String] = None,
    app_display_version: Option[String] = None,
    app_name: Option[String] = None,
    app_version: Option[String] = None,
    attribution: Option[Attribution] = None,
    blocklist_enabled: Option[Boolean] = None,
    channel: Option[String] = None,
    city: Option[String] = None,
    client_clock_skew: Option[Long] = None,
    client_submission_latency: Option[Long] = None,
    client_id: Option[String] = Some("test"),
    country: Option[String] = None,
    cpu_cores: Option[Int] = None,
    cpu_count: Option[Int] = None,
    cpu_family: Option[Int] = None,
    cpu_l2_cache_kb: Option[Int] = None,
    cpu_l3_cache_kb: Option[Int] = None,
    cpu_model: Option[Int] = None,
    cpu_speed_mhz: Option[Int] = None,
    cpu_stepping: Option[Int] = None,
    cpu_vendor: Option[String] = None,
    crash_submit_attempt_content: Option[Int] = None,
    crash_submit_attempt_main: Option[Int] = None,
    crash_submit_attempt_plugin: Option[Int] = None,
    crash_submit_success_content: Option[Int] = None,
    crash_submit_success_main: Option[Int] = None,
    crash_submit_success_plugin: Option[Int] = None,
    crashes_detected_content: Option[Int] = None,
    crashes_detected_gmplugin: Option[Int] = None,
    crashes_detected_plugin: Option[Int] = None,
    default_search_engine: Option[String] = None,
    default_search_engine_data_load_path: Option[String] = None,
    default_search_engine_data_name: Option[String] = None,
    default_search_engine_data_origin: Option[String] = None,
    default_search_engine_data_submission_url: Option[String] = None,
    devtools_toolbox_opened_count: Option[Int] = None,
    distribution_id: Option[String] = None,
    document_id: Option[String] = None,
    e10s_enabled: Option[Boolean] = None,
    env_build_arch: Option[String] = None,
    env_build_id: Option[String] = None,
    env_build_version: Option[String] = None,
    experiments: Option[scala.collection.Map[String,Option[String]]] = None,
    first_paint: Option[Int] = None,
    flash_version: Option[String] = None,
    geo_subdivision1: Option[String] = None,
    geo_subdivision2: Option[String] = None,
    gfx_features_advanced_layers_status: Option[String] = None,
    gfx_features_d2d_status: Option[String] = None,
    gfx_features_d3d11_status: Option[String] = None,
    gfx_features_gpu_process_status: Option[String] = None,
    install_year: Option[Long] = None,
    is_default_browser: Option[Boolean] = None,
    is_wow64: Option[Boolean] = None,
    locale: Option[String] = None,
    memory_mb: Option[Int] = None,
    normalized_channel: Option[String] = None,
    normalized_os_version: Option[String] = None,
    os: Option[String] = None,
    os_service_pack_major: Option[Long] = None,
    os_service_pack_minor: Option[Long] = None,
    os_version: Option[String] = None,
    places_bookmarks_count: Option[Int] = None,
    places_pages_count: Option[Int] = None,
    plugin_hangs: Option[Int] = None,
    plugins_infobar_allow: Option[Int] = None,
    plugins_infobar_block: Option[Int] = None,
    plugins_infobar_shown: Option[Int] = None,
    plugins_notification_shown: Option[Int] = None,
    previous_build_id: Option[String] = None,
    profile_creation_date: Option[Long] = None,
    push_api_notify: Option[Int] = None,
    sample_id: Option[String] = None,
    sandbox_effective_content_process_level: Option[Int] = None,
    scalar_content_navigator_storage_estimate_count: Option[Int] = None,
    scalar_content_navigator_storage_persist_count: Option[Int] = None,
    scalar_content_webrtc_nicer_stun_retransmits: Option[Int] = None,
    scalar_content_webrtc_nicer_turn_401s: Option[Int] = None,
    scalar_content_webrtc_nicer_turn_403s: Option[Int] = None,
    scalar_content_webrtc_nicer_turn_438s: Option[Int] = None,
    scalar_parent_aushelper_websense_reg_version: Option[String] = None,
    scalar_parent_browser_engagement_max_concurrent_tab_count: Option[Int] = None,
    scalar_parent_browser_engagement_max_concurrent_window_count: Option[Int] = None,
    scalar_parent_browser_engagement_tab_open_event_count: Option[Int] = None,
    scalar_parent_browser_engagement_total_uri_count: Option[Int] = None,
    scalar_parent_browser_engagement_unfiltered_uri_count: Option[Int] = None,
    scalar_parent_browser_engagement_unique_domains_count: Option[Int] = None,
    scalar_parent_browser_engagement_window_open_event_count: Option[Int] = None,
    scalar_parent_devtools_copy_full_css_selector_opened: Option[Int] = None,
    scalar_parent_devtools_copy_unique_css_selector_opened: Option[Int] = None,
    scalar_parent_devtools_toolbar_eyedropper_opened: Option[Int] = None,
    scalar_parent_dom_contentprocess_troubled_due_to_memory: Option[Int] = None,
    scalar_parent_navigator_storage_estimate_count: Option[Int] = None,
    scalar_parent_navigator_storage_persist_count: Option[Int] = None,
    scalar_parent_storage_sync_api_usage_extensions_using: Option[Int] = None,
    scalar_parent_webrtc_nicer_stun_retransmits: Option[Int] = None,
    scalar_parent_webrtc_nicer_turn_401s: Option[Int] = None,
    scalar_parent_webrtc_nicer_turn_403s: Option[Int] = None,
    scalar_parent_webrtc_nicer_turn_438s: Option[Int] = None,
    search_cohort: Option[String] = None,
    search_counts: Option[scala.collection.Seq[SearchCount]] = None,
    session_restored: Option[Int] = None,
    shutdown_kill: Option[Int] = None,
    ssl_handshake_result_failure: Option[Int] = None,
    ssl_handshake_result_success: Option[Int] = None,
    submission_date_s3: Option[Int] = None,
    subsession_counter: Option[Int] = None,
    subsession_length: Option[Long] = None,
    subsession_start_date: Option[String] = None,
    sync_configured: Option[Boolean] = None,
    sync_count_desktop: Option[Int] = None,
    sync_count_mobile: Option[Int] = None,
    telemetry_enabled: Option[Boolean] = None,
    timezone_offset: Option[Int] = None,
    total_time: Option[Int] = None,
    update_auto_download: Option[Boolean] = None,
    update_channel: Option[String] = None,
    update_enabled: Option[Boolean] = None,
    vendor: Option[String] = None,
    web_notification_shown: Option[Int] = None,
    windows_build_number: Option[Long] = None,
    windows_ubr: Option[Long] = None
  )

  def getRowAggFirst(sValue: Option[String], bValue: Option[Boolean], iValue: Option[Int]) = MainSummaryRow(
    active_experiment_branch = sValue,
    active_experiment_id = sValue,
    addon_compatibility_check_enabled = bValue,
    app_build_id = sValue,
    app_display_version = sValue,
    app_name = sValue,
    app_version = sValue,
    attribution = Some(Attribution(
      source = sValue,
      medium = sValue,
      campaign = sValue,
      content = sValue
    )),
    blocklist_enabled = bValue,
    channel = sValue,
    cpu_cores = iValue,
    cpu_count = iValue,
    cpu_family = iValue,
    cpu_l2_cache_kb = iValue,
    cpu_l3_cache_kb = iValue,
    cpu_model = iValue,
    cpu_speed_mhz = iValue,
    cpu_stepping = iValue,
    cpu_vendor = sValue,
    default_search_engine = sValue,
    default_search_engine_data_load_path = sValue,
    default_search_engine_data_name = sValue,
    default_search_engine_data_origin = sValue,
    default_search_engine_data_submission_url = sValue,
    distribution_id = sValue,
    e10s_enabled = bValue,
    env_build_arch = sValue,
    env_build_id = sValue,
    env_build_version = sValue,
    flash_version = sValue,
    gfx_features_advanced_layers_status = sValue,
    gfx_features_d2d_status = sValue,
    gfx_features_d3d11_status = sValue,
    gfx_features_gpu_process_status = sValue,
    install_year = Some(iValue.get),
    is_default_browser = bValue,
    is_wow64 = bValue,
    locale = sValue,
    memory_mb = iValue,
    normalized_channel = sValue,
    normalized_os_version = sValue,
    os = sValue,
    os_service_pack_major = Some(iValue.get),
    os_service_pack_minor = Some(iValue.get),
    os_version = sValue,
    previous_build_id = sValue,
    sample_id = sValue,
    sandbox_effective_content_process_level = iValue,
    scalar_parent_aushelper_websense_reg_version = sValue,
    search_cohort = sValue,
    sync_configured = bValue,
    telemetry_enabled = bValue,
    timezone_offset = iValue,
    update_auto_download = bValue,
    update_channel = sValue,
    update_enabled = bValue,
    vendor = sValue,
    windows_build_number = Some(iValue.get),
    windows_ubr = Some(iValue.get)
  )

  def getExpectAggFirst(sValue: String, bValue: Boolean, iValue: Int) = Map(
    "active_experiment_branch" -> sValue,
    "active_experiment_id" -> sValue,
    "addon_compatibility_check_enabled" -> bValue,
    "app_build_id" -> sValue,
    "app_display_version" -> sValue,
    "app_name" -> sValue,
    "app_version" -> sValue,
    "attribution.source" -> sValue,
    "attribution.medium" -> sValue,
    "attribution.campaign" -> sValue,
    "attribution.content" -> sValue,
    "blocklist_enabled" -> bValue,
    "channel" -> sValue,
    "cpu_cores" -> iValue,
    "cpu_count" -> iValue,
    "cpu_family" -> iValue,
    "cpu_l2_cache_kb" -> iValue,
    "cpu_l3_cache_kb" -> iValue,
    "cpu_model" -> iValue,
    "cpu_speed_mhz" -> iValue,
    "cpu_stepping" -> iValue,
    "cpu_vendor" -> sValue,
    "default_search_engine" -> sValue,
    "default_search_engine_data_load_path" -> sValue,
    "default_search_engine_data_name" -> sValue,
    "default_search_engine_data_origin" -> sValue,
    "default_search_engine_data_submission_url" -> sValue,
    "distribution_id" -> sValue,
    "e10s_enabled" -> bValue,
    "env_build_arch" -> sValue,
    "env_build_id" -> sValue,
    "env_build_version" -> sValue,
    "flash_version" -> sValue,
    "gfx_features_advanced_layers_status" -> sValue,
    "gfx_features_d2d_status" -> sValue,
    "gfx_features_d3d11_status" -> sValue,
    "gfx_features_gpu_process_status" -> sValue,
    "install_year" -> iValue,
    "is_default_browser" -> bValue,
    "is_wow64" -> bValue,
    "locale" -> sValue,
    "memory_mb" -> iValue,
    "normalized_channel" -> sValue,
    "normalized_os_version" -> sValue,
    "os" -> sValue,
    "os_service_pack_major" -> iValue,
    "os_service_pack_minor" -> iValue,
    "os_version" -> sValue,
    "previous_build_id" -> sValue,
    "sample_id" -> sValue,
    "sandbox_effective_content_process_level" -> iValue,
    "scalar_parent_aushelper_websense_reg_version" -> sValue,
    "search_cohort" -> sValue,
    "sync_configured" -> bValue,
    "telemetry_enabled" -> bValue,
    "timezone_offset" -> iValue,
    "update_auto_download" -> bValue,
    "update_channel" -> sValue,
    "update_enabled" -> bValue,
    "vendor" -> sValue,
    "windows_build_number" -> iValue,
    "windows_ubr" -> iValue
  )

  def getRowAggMax(value: Option[Int]) = MainSummaryRow(
    scalar_parent_browser_engagement_max_concurrent_tab_count = value,
    scalar_parent_browser_engagement_max_concurrent_window_count = value,
    scalar_parent_browser_engagement_unique_domains_count = value
  )

  def getExpectAggMax(value: Int) = Map(
    "scalar_parent_browser_engagement_max_concurrent_tab_count_max" -> value,
    "scalar_parent_browser_engagement_max_concurrent_window_count_max" -> value,
    "scalar_parent_browser_engagement_unique_domains_count_max" -> value
  )

  def getRowAggMean(value: Option[Int]) = MainSummaryRow(
    active_addons_count = Some(value.get),
    client_clock_skew = Some(value.get),
    client_submission_latency = Some(value.get),
    first_paint = value,
    places_bookmarks_count = value,
    places_pages_count = value,
    scalar_parent_browser_engagement_unique_domains_count = value,
    session_restored = value
  )

  def getExpectAggMean(value: Int) = Map(
    "active_addons_count_mean" -> value,
    "client_clock_skew_mean" -> value,
    "client_submission_latency_mean" -> value,
    "first_paint_mean" -> value,
    "places_bookmarks_count_mean" -> value,
    "places_pages_count_mean" -> value,
    "scalar_parent_browser_engagement_unique_domains_count_mean" -> value,
    "session_restored_mean" -> value
  )

  def getRowAggSum(value: Option[Int]) = MainSummaryRow(
    aborts_content = value,
    aborts_gmplugin = value,
    aborts_plugin = value,
    active_ticks = Some(value.get * (3600/5)), // convert value to hours
    crash_submit_attempt_content = value,
    crash_submit_attempt_main = value,
    crash_submit_attempt_plugin = value,
    crash_submit_success_content = value,
    crash_submit_success_main = value,
    crash_submit_success_plugin = value,
    crashes_detected_content = value,
    crashes_detected_gmplugin = value,
    crashes_detected_plugin = value,
    devtools_toolbox_opened_count = value,
    plugin_hangs = value,
    plugins_infobar_allow = value,
    plugins_infobar_block = value,
    plugins_infobar_shown = value,
    plugins_notification_shown = value,
    push_api_notify = value,
    scalar_content_navigator_storage_estimate_count = value,
    scalar_content_navigator_storage_persist_count = value,
    scalar_content_webrtc_nicer_stun_retransmits = value,
    scalar_content_webrtc_nicer_turn_401s = value,
    scalar_content_webrtc_nicer_turn_403s = value,
    scalar_content_webrtc_nicer_turn_438s = value,
    scalar_parent_browser_engagement_tab_open_event_count = value,
    scalar_parent_browser_engagement_total_uri_count = value,
    scalar_parent_browser_engagement_unfiltered_uri_count = value,
    scalar_parent_browser_engagement_window_open_event_count = value,
    scalar_parent_devtools_copy_full_css_selector_opened = value,
    scalar_parent_devtools_copy_unique_css_selector_opened = value,
    scalar_parent_devtools_toolbar_eyedropper_opened = value,
    scalar_parent_dom_contentprocess_troubled_due_to_memory = value,
    scalar_parent_navigator_storage_estimate_count = value,
    scalar_parent_navigator_storage_persist_count = value,
    scalar_parent_storage_sync_api_usage_extensions_using = value,
    scalar_parent_webrtc_nicer_stun_retransmits = value,
    scalar_parent_webrtc_nicer_turn_401s = value,
    scalar_parent_webrtc_nicer_turn_403s = value,
    scalar_parent_webrtc_nicer_turn_438s = value,
    shutdown_kill = value,
    ssl_handshake_result_failure = value,
    ssl_handshake_result_success = value,
    subsession_length = Some(value.get * 3600), // convert value to hours
    sync_count_desktop = value,
    sync_count_mobile = value,
    total_time = Some(value.get * 3600), // convert value to hours
    web_notification_shown = value
  )

  def getExpectAggSum(value: Int) = Map(
    "aborts_content_sum" -> value,
    "aborts_gmplugin_sum" -> value,
    "aborts_plugin_sum" -> value,
    "int(round(active_hours_sum))" -> value,
    "crash_submit_attempt_content_sum" -> value,
    "crash_submit_attempt_main_sum" -> value,
    "crash_submit_attempt_plugin_sum" -> value,
    "crash_submit_success_content_sum" -> value,
    "crash_submit_success_main_sum" -> value,
    "crash_submit_success_plugin_sum" -> value,
    "crashes_detected_content_sum" -> value,
    "crashes_detected_gmplugin_sum" -> value,
    "crashes_detected_plugin_sum" -> value,
    "devtools_toolbox_opened_count_sum" -> value,
    "plugin_hangs_sum" -> value,
    "plugins_infobar_allow_sum" -> value,
    "plugins_infobar_block_sum" -> value,
    "plugins_infobar_shown_sum" -> value,
    "plugins_notification_shown_sum" -> value,
    "push_api_notify_sum" -> value,
    "scalar_combined_webrtc_nicer_stun_retransmits_sum" -> 2*value,
    "scalar_combined_webrtc_nicer_turn_401s_sum" -> 2*value,
    "scalar_combined_webrtc_nicer_turn_403s_sum" -> 2*value,
    "scalar_combined_webrtc_nicer_turn_438s_sum" -> 2*value,
    "scalar_content_navigator_storage_estimate_count_sum" -> value,
    "scalar_content_navigator_storage_persist_count_sum" -> value,
    "scalar_parent_browser_engagement_tab_open_event_count_sum" -> value,
    "scalar_parent_browser_engagement_total_uri_count_sum" -> value,
    "scalar_parent_browser_engagement_unfiltered_uri_count_sum" -> value,
    "scalar_parent_browser_engagement_window_open_event_count_sum" -> value,
    "scalar_parent_devtools_copy_full_css_selector_opened_sum" -> value,
    "scalar_parent_devtools_copy_unique_css_selector_opened_sum" -> value,
    "scalar_parent_devtools_toolbar_eyedropper_opened_sum" -> value,
    "scalar_parent_dom_contentprocess_troubled_due_to_memory_sum" -> value,
    "scalar_parent_navigator_storage_estimate_count_sum" -> value,
    "scalar_parent_navigator_storage_persist_count_sum" -> value,
    "scalar_parent_storage_sync_api_usage_extensions_using_sum" -> value,
    "shutdown_kill_sum" -> value,
    "ssl_handshake_result_failure_sum" -> value,
    "ssl_handshake_result_success_sum" -> value,
    "int(round(subsession_hours_sum))" -> value,
    "sync_count_desktop_sum" -> value,
    "sync_count_mobile_sum" -> value,
    "int(round(total_hours_sum))" -> value,
    "web_notification_shown_sum" -> value
  )

  val genericTests = List(
    // test profile_age_in_days and profile_creation_date
    // WARNING requires UTC, can be set on the jvm with -Duser.timezone=UTC
    (
      List(
        // these rows do not handle nulls gracefully
        MainSummaryRow(
          subsession_start_date = Some("2018-01-05"),
          profile_creation_date = Some(17532) // 2018-01-01
        )
      ),
      Map(
        "profile_age_in_days" -> 4,
        "profile_creation_date" -> "2018-01-01 00:00:00"
      )
    ),
    // test that first collects non-null values
    (
      List(
        MainSummaryRow(),
        getRowAggFirst(Some("first"), Some(true), Some(1)),
        getRowAggFirst(Some("second"), Some(false), Some(0))
      ),
      getExpectAggFirst("first", true, 1)
    ),
    // test that first collects falsey values
    (
      List(
        getRowAggFirst(Some(""), Some(false), Some(0)),
        getRowAggFirst(Some("second"), Some(true), Some(1))
      ),
      getExpectAggFirst("", false, 0)
    ),
    // test max
    (
      List(
        getRowAggMax(Some(1)),
        getRowAggMax(Some(3)),
        MainSummaryRow()
      ),
      getExpectAggMax(3)
    ),
    // test that mean works with single values
    (
      List(
        getRowAggMean(Some(100))
      ),
      getExpectAggMean(100)
    ),
    // test that mean ignores null values
    (
      List(
        getRowAggMean(Some(100)),
        getRowAggMean(Some(0)),
        MainSummaryRow()
      ),
      getExpectAggMean(50)
    ),
    // test that sum handles null values, and collects values <= 0 correctly
    (
      List(
        getRowAggSum(Some(1)),
        getRowAggSum(Some(0)),
        getRowAggSum(Some(-2)),
        getRowAggSum(Some(5)),
        MainSummaryRow()
      ),
      getExpectAggSum(4)
    ),
    // test experiments
    (
      List(
        MainSummaryRow(),
        MainSummaryRow(experiments = Some(Map("A" -> None, "B" -> None))),
        MainSummaryRow(experiments = Some(Map("A" -> Some("1"), "B" -> Some("2"), "C" -> None, "D" -> None))),
        MainSummaryRow(experiments = Some(Map("C" -> Some("3")))),
        MainSummaryRow(experiments = Some(Map("D" -> Some("4")))),
        MainSummaryRow(experiments = Some(Map("A" -> Some("4"), "B" -> Some("3"), "C" -> Some("2"), "D" -> Some("1")))),
        MainSummaryRow(experiments = Some(Map("B" -> Some("1"), "C" -> Some("2"))))
      ),
      Map(
        "experiments['A']" -> "1",
        "experiments['B']" -> "2",
        "experiments['C']" -> "3",
        "experiments['D']" -> "4"
      )
    ),
    // test geo aggregates as a set on presence of country
    (
      List(
        MainSummaryRow(),
        MainSummaryRow(country = Some("??"), city = Some("Tilehurst"), geo_subdivision1 = Some("ENG"), geo_subdivision2 = Some("WBK")),
        MainSummaryRow(country = Some("CA")),
        MainSummaryRow(country = Some("US"), geo_subdivision1 = Some("WA")),
        MainSummaryRow(country = Some("US"), city = Some("Portland"), geo_subdivision1 = Some("OR")),
        MainSummaryRow(country = Some("GB"), city = Some("Tilehurst"), geo_subdivision1 = Some("ENG"), geo_subdivision2 = Some("WBK")),
        MainSummaryRow()
      ),
      Map(
        "country" -> "CA",
        "city" -> "??",
        "geo_subdivision1" -> "??",
        "geo_subdivision2" -> "??"
      )
    ),
    (
      List(
        MainSummaryRow(),
        MainSummaryRow(country = Some("US"), geo_subdivision1 = Some("WA")),
        MainSummaryRow(country = Some("US"), city = Some("Portland"), geo_subdivision1 = Some("OR")),
        MainSummaryRow(country = Some("GB"), city = Some("Tilehurst"), geo_subdivision1 = Some("ENG"), geo_subdivision2 = Some("WBK")),
        MainSummaryRow()
      ),
      Map(
        "country" -> "US",
        "city" -> "??",
        "geo_subdivision1" -> "WA",
        "geo_subdivision2" -> "??"
      )
    ),
    (
      List(
        MainSummaryRow(),
        MainSummaryRow(country = Some("US"), city = Some("Portland"), geo_subdivision1 = Some("OR")),
        MainSummaryRow(country = Some("GB"), city = Some("Tilehurst"), geo_subdivision1 = Some("ENG"), geo_subdivision2 = Some("WBK")),
        MainSummaryRow()
      ),
      Map(
        "country" -> "US",
        "city" -> "Portland",
        "geo_subdivision1" -> "OR",
        "geo_subdivision2" -> "??"
      )
    ),
    // test subsessions_started_on_this_day counts 1s
    (
      List(
        MainSummaryRow(subsession_counter = Some(1)),
        MainSummaryRow(subsession_counter = Some(0)),
        MainSummaryRow(subsession_counter = Some(1)),
        MainSummaryRow(subsession_counter = Some(2)),
        MainSummaryRow()
      ),
      Map("sessions_started_on_this_day" -> 2)
    ),
    // test search_counts
    (
      List(
        // two rows with counts to get summed
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(1)),
          SearchCount(None, Some("contextmenu"), Some(2)),
          SearchCount(None, Some("newtab"), Some(3)),
          SearchCount(None, Some("searchbar"), Some(4)),
          SearchCount(None, Some("system"), Some(5)),
          SearchCount(None, Some("urlbar"), Some(6)),
          SearchCount(None, Some("invalid"), Some(7)),
          SearchCount(None, None, Some(8))
        ))),
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(1)),
          SearchCount(None, Some("contextmenu"), Some(2)),
          SearchCount(None, Some("newtab"), Some(3)),
          SearchCount(None, Some("searchbar"), Some(4)),
          SearchCount(None, Some("system"), Some(5)),
          SearchCount(None, Some("urlbar"), Some(6)),
          SearchCount(None, Some("invalid"), Some(7)),
          SearchCount(None, None, Some(8))
        ))),
        // a row of invalid counts
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(-1)),
          SearchCount(None, Some("contextmenu"), Some(-2)),
          SearchCount(None, Some("newtab"), Some(-3)),
          SearchCount(None, Some("searchbar"), Some(-4)),
          SearchCount(None, Some("system"), Some(-5)),
          SearchCount(None, Some("urlbar"), Some(-6)),
          SearchCount(None, Some("invalid"), Some(-7)),
          SearchCount(None, None, Some(-8))
        ))),
        // an empty row
        MainSummaryRow(),
        // a row of null counts
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), None),
          SearchCount(None, Some("contextmenu"), None),
          SearchCount(None, Some("newtab"), None),
          SearchCount(None, Some("searchbar"), None),
          SearchCount(None, Some("system"), None),
          SearchCount(None, Some("urlbar"), None),
          SearchCount(None, Some("invalid"), None),
          SearchCount(None, None, None)
        ))),
        // a row of 1s and a row of 0s for good measure
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(1)),
          SearchCount(None, Some("contextmenu"), Some(1)),
          SearchCount(None, Some("newtab"), Some(1)),
          SearchCount(None, Some("searchbar"), Some(1)),
          SearchCount(None, Some("system"), Some(1)),
          SearchCount(None, Some("urlbar"), Some(1)),
          SearchCount(None, Some("invalid"), Some(1)),
          SearchCount(None, None, Some(1))
        ))),
        MainSummaryRow(search_counts = Some(List(
          SearchCount(None, Some("abouthome"), Some(0)),
          SearchCount(None, Some("contextmenu"), Some(0)),
          SearchCount(None, Some("newtab"), Some(0)),
          SearchCount(None, Some("searchbar"), Some(0)),
          SearchCount(None, Some("system"), Some(0)),
          SearchCount(None, Some("urlbar"), Some(0)),
          SearchCount(None, Some("invalid"), Some(0)),
          SearchCount(None, None, Some(0))
        )))
      ),
      Map(
        "search_count_all" -> 48,
        "search_count_abouthome" -> 3,
        "search_count_contextmenu" -> 5,
        "search_count_newtab" -> 7,
        "search_count_searchbar" -> 9,
        "search_count_system" -> 11,
        "search_count_urlbar" -> 13
      )
    )
  )
}
