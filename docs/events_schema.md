As of 2017-01-26,
the current version of the `events` dataset is `v1`,
and has a schema as follows:

```
root
 |-- document_id: string (nullable = true)
 |-- client_id: string (nullable = true)
 |-- normalized_channel: string (nullable = true)
 |-- country: string (nullable = true)
 |-- locale: string (nullable = true)
 |-- app_name: string (nullable = true)
 |-- app_version: string (nullable = true)
 |-- os: string (nullable = true)
 |-- os_version: string (nullable = true)
 |-- subsession_start_date: string (nullable = true)
 |-- subsession_length: long (nullable = true)
 |-- sync_configured: boolean (nullable = true)
 |-- sync_count_desktop: integer (nullable = true)
 |-- sync_count_mobile: integer (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- sample_id: string (nullable = true)
 |-- event_timestamp: long (nullable = false)
 |-- event_category: string (nullable = false)
 |-- event_method: string (nullable = false)
 |-- event_object: string (nullable = false)
 |-- event_string_value: string (nullable = true)
 |-- event_map_values: map (nullable = true)
 |    |-- key: string
 |    |-- value: string
 |-- submission_date_s3: string (nullable = true)
 |-- doc_type: string (nullable = true)
```

Currently, client-side event telemetry is undocumented -- this doc will link to
those once they're published.
