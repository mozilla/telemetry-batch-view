As of 2017-05-07,
the current version of the `main_summary` dataset is `v4`,
and has a schema as follows:

```
root
 |-- document_id: string (nullable = false)
 |-- client_id: string (nullable = true)
 |-- sample_id: long (nullable = true)
 |-- channel: string (nullable = true)
 |-- normalized_channel: string (nullable = true)
 |-- country: string (nullable = true)
 |-- city: string (nullable = true)
 |-- os: string (nullable = true)
 |-- os_version: string (nullable = true)
 |-- os_service_pack_major: long (nullable = true)
 |-- os_service_pack_minor: long (nullable = true)
 |-- windows_build_number: long (nullable = true)
 |-- windows_ubr: long (nullable = true)
 |-- install_year: long (nullable = true)
 |-- is_wow64: boolean (nullable = true)
 |-- memory_mb: integer (nullable = true)
 |-- profile_creation_date: long (nullable = true)
 |-- subsession_start_date: string (nullable = true)
 |-- subsession_length: long (nullable = true)
 |-- subsession_counter: integer (nullable = true)
 |-- profile_subsession_counter: integer (nullable = true)
 |-- distribution_id: string (nullable = true)
 |-- submission_date: string (nullable = false)
 |-- sync_configured: boolean (nullable = true)
 |-- sync_count_desktop: integer (nullable = true)
 |-- sync_count_mobile: integer (nullable = true)
 |-- app_build_id: string (nullable = true)
 |-- app_display_version: string (nullable = true)
 |-- app_name: string (nullable = true)
 |-- app_version: string (nullable = true)
 |-- timestamp: long (nullable = false)
 |-- env_build_id: string (nullable = true)
 |-- env_build_version: string (nullable = true)
 |-- env_build_arch: string (nullable = true)
 |-- e10s_enabled: boolean (nullable = true)
 |-- e10s_cohort: string (nullable = true)
 |-- locale: string (nullable = true)
 |-- attribution: struct (nullable = true)
 |    |-- source: string (nullable = true)
 |    |-- medium: string (nullable = true)
 |    |-- campaign: string (nullable = true)
 |    |-- content: string (nullable = true)
 |-- active_experiment_id: string (nullable = true)
 |-- active_experiment_branch: string (nullable = true)
 |-- reason: string (nullable = true)
 |-- timezone_offset: integer (nullable = true)
 |-- plugin_hangs: integer (nullable = true)
 |-- aborts_plugin: integer (nullable = true)
 |-- aborts_content: integer (nullable = true)
 |-- aborts_gmplugin: integer (nullable = true)
 |-- crashes_detected_plugin: integer (nullable = true)
 |-- crashes_detected_content: integer (nullable = true)
 |-- crashes_detected_gmplugin: integer (nullable = true)
 |-- crash_submit_attempt_main: integer (nullable = true)
 |-- crash_submit_attempt_content: integer (nullable = true)
 |-- crash_submit_attempt_plugin: integer (nullable = true)
 |-- crash_submit_success_main: integer (nullable = true)
 |-- crash_submit_success_content: integer (nullable = true)
 |-- crash_submit_success_plugin: integer (nullable = true)
 |-- shutdown_kill: integer (nullable = true)
 |-- active_addons_count: long (nullable = true)
 |-- flash_version: string (nullable = true)
 |-- vendor: string (nullable = true)
 |-- is_default_browser: boolean (nullable = true)
 |-- default_search_engine_data_name: string (nullable = true)
 |-- default_search_engine_data_load_path: string (nullable = true)
 |-- default_search_engine_data_origin: string (nullable = true)
 |-- default_search_engine_data_submission_url: string (nullable = true)
 |-- default_search_engine: string (nullable = true)
 |-- loop_activity_counter: struct (nullable = true)
 |    |-- open_panel: integer (nullable = true)
 |    |-- open_conversation: integer (nullable = true)
 |    |-- room_open: integer (nullable = true)
 |    |-- room_share: integer (nullable = true)
 |    |-- room_delete: integer (nullable = true)
 |-- devtools_toolbox_opened_count: integer (nullable = true)
 |-- client_submission_date: string (nullable = true)
 |-- places_bookmarks_count: integer (nullable = true)
 |-- places_pages_count: integer (nullable = true)
 |-- push_api_notify: integer (nullable = true)
 |-- web_notification_shown: integer (nullable = true)
 |-- popup_notification_stats: map (nullable = true)
 |    |-- key: string
 |    |-- value: struct (valueContainsNull = true)
 |    |    |-- offered: integer (nullable = true)
 |    |    |-- action_1: integer (nullable = true)
 |    |    |-- action_2: integer (nullable = true)
 |    |    |-- action_3: integer (nullable = true)
 |    |    |-- action_last: integer (nullable = true)
 |    |    |-- dismissal_click_elsewhere: integer (nullable = true)
 |    |    |-- dismissal_leave_page: integer (nullable = true)
 |    |    |-- dismissal_close_button: integer (nullable = true)
 |    |    |-- dismissal_not_now: integer (nullable = true)
 |    |    |-- open_submenu: integer (nullable = true)
 |    |    |-- learn_more: integer (nullable = true)
 |    |    |-- reopen_offered: integer (nullable = true)
 |    |    |-- reopen_action_1: integer (nullable = true)
 |    |    |-- reopen_action_2: integer (nullable = true)
 |    |    |-- reopen_action_3: integer (nullable = true)
 |    |    |-- reopen_action_last: integer (nullable = true)
 |    |    |-- reopen_dismissal_click_elsewhere: integer (nullable = true)
 |    |    |-- reopen_dismissal_leave_page: integer (nullable = true)
 |    |    |-- reopen_dismissal_close_button: integer (nullable = true)
 |    |    |-- reopen_dismissal_not_now: integer (nullable = true)
 |    |    |-- reopen_open_submenu: integer (nullable = true)
 |    |    |-- reopen_learn_more: integer (nullable = true)
 |-- search_counts: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- engine: string (nullable = true)
 |    |    |-- source: string (nullable = true)
 |    |    |-- count: long (nullable = true)
 |-- active_addons: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- addon_id: string (nullable = false)
 |    |    |-- blocklisted: boolean (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- user_disabled: boolean (nullable = true)
 |    |    |-- app_disabled: boolean (nullable = true)
 |    |    |-- version: string (nullable = true)
 |    |    |-- scope: integer (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- foreign_install: boolean (nullable = true)
 |    |    |-- has_binary_components: boolean (nullable = true)
 |    |    |-- install_day: integer (nullable = true)
 |    |    |-- update_day: integer (nullable = true)
 |    |    |-- signed_state: integer (nullable = true)
 |    |    |-- is_system: boolean (nullable = true)
 |-- active_theme: struct (nullable = true)
 |    |-- addon_id: string (nullable = false)
 |    |-- blocklisted: boolean (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- user_disabled: boolean (nullable = true)
 |    |-- app_disabled: boolean (nullable = true)
 |    |-- version: string (nullable = true)
 |    |-- scope: integer (nullable = true)
 |    |-- type: string (nullable = true)
 |    |-- foreign_install: boolean (nullable = true)
 |    |-- has_binary_components: boolean (nullable = true)
 |    |-- install_day: integer (nullable = true)
 |    |-- update_day: integer (nullable = true)
 |    |-- signed_state: integer (nullable = true)
 |    |-- is_system: boolean (nullable = true)
 |-- blocklist_enabled: boolean (nullable = true)
 |-- addon_compatibility_check_enabled: boolean (nullable = true)
 |-- telemetry_enabled: boolean (nullable = true)
 |-- user_prefs: struct (nullable = true)
 |    |-- dom_ipc_process_count: integer (nullable = true)
 |-- events: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- timestamp: long (nullable = false)
 |    |    |-- category: string (nullable = false)
 |    |    |-- method: string (nullable = false)
 |    |    |-- object: string (nullable = false)
 |    |    |-- string_value: string (nullable = true)
 |    |    |-- map_values: map (nullable = true)
 |    |    |    |-- key: string
 |    |    |    |-- value: string (valueContainsNull = true)
 |-- ssl_handshake_result_success: integer (nullable = true)
 |-- ssl_handshake_result_failure: integer (nullable = true)
 |-- ssl_handshake_result: map (nullable = true)
 |    |-- key: string
 |    |-- value: integer (valueContainsNull = true)
 |-- active_ticks: integer (nullable = true)
 |-- main: integer (nullable = true)
 |-- first_paint: integer (nullable = true)
 |-- session_restored: integer (nullable = true)
 |-- total_time: integer (nullable = true)
 |-- plugins_notification_shown: integer (nullable = true)
 |-- plugins_notification_user_action: struct (nullable = true)
 |    |-- allow_now: integer (nullable = true)
 |    |-- allow_always: integer (nullable = true)
 |    |-- block: integer (nullable = true)
 |-- plugins_infobar_shown: integer (nullable = true)
 |-- plugins_infobar_block: integer (nullable = true)
 |-- plugins_infobar_allow: integer (nullable = true)
 <SCALARS>
```

### Scalars
Parent scalars are automatically added to the dataset, and as such, there is no longer a fixed schema.
They are ordered by the name of the scalar, which is composed of <namespace>.<scalar_name> (e.g. 
browser.engagement.max_concurrent_tab_count). The full schema name is scalar_<process>_<namespace>_<scalar> 
(e.g. scalars_parent_browser_engagement_max_concurrent_tab_count), where currently only parent process scalars are
available.

For more detail on where these fields come from in the
[raw data](https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/telemetry/data/main-ping.html),
please look [in the MainSummaryView code](src/main/scala/views/MainSummaryView.scala)
in the `buildSchema` function.

Most of the fields are simple scalar values, with a few notable exceptions:

* The `search_count` field is an array of structs, each item in the array representing
  a 3-tuple of (`engine`, `source`, `count`). The `engine` field represents the name of
  the search engine against which the searches were done. The `source` field represents
  the part of the Firefox UI that was used to perform the search. It contains values
  such as "abouthome", "urlbar", and "searchbar". The `count` field contains the number
  of searches performed against this engine+source combination during that subsession.
  Any of the fields in the struct may be null (for example if the search key did not
  match the expected pattern, or if the count was non-numeric).
* The `loop_activity_counter` field is a simple struct containing inner fields for each
  expected value of the `LOOP_ACTIVITY_COUNTER` Enumerated Histogram. Each inner field
  is a count for that histogram bucket.
* The `popup_notification_stats` field is a map of `String` keys to struct values,
  each field in the struct being a count for the expected values of the
  `POPUP_NOTIFICATION_STATS` Keyed Enumerated Histogram.
* The `places_bookmarks_count` and `places_pages_count` fields contain the **mean**
  value of the corresponding Histogram, which can be interpreted as the average number
  of bookmarks or pages in a given subsession.
* The `active_addons` field contains an array of structs, one for each entry in
  the `environment.addons.activeAddons` section of the payload. More detail in
  [Bug 1290181](https://bugzilla.mozilla.org/show_bug.cgi?id=1290181).
* The `theme` field contains a single struct in the same shape as the items in the
  `active_addons` array. It contains information about the currently active browser
  theme.
* The `user_prefs` field contains a struct with values for preferences of interest.
