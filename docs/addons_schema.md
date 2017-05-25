As of 2017-03-16,
the current version of the `addons` dataset is `v2`,
and has a schema as follows:

```
root
 |-- document_id: string (nullable = true)
 |-- client_id: string (nullable = true)
 |-- subsession_start_date: string (nullable = true)
 |-- normalized_channel: string (nullable = true)
 |-- addon_id: string (nullable = true)
 |-- blocklisted: boolean (nullable = true)
 |-- name: string (nullable = true)
 |-- user_disabled: boolean (nullable = true)
 |-- app_disabled: boolean (nullable = true)
 |-- version: string (nullable = true)
 |-- scope: integer (nullable = true)
 |-- type: string (nullable = true)
 |-- foreign_install: boolean (nullable = true)
 |-- has_binary_components: boolean (nullable = true)
 |-- install_day: integer (nullable = true)
 |-- update_day: integer (nullable = true)
 |-- signed_state: integer (nullable = true)
 |-- is_system: boolean (nullable = true)
 |-- submission_date_s3: string (nullable = true)
 |-- sample_id: string (nullable = true)
```
For more detail on where these fields come from in the
[raw data](https://gecko.readthedocs.io/en/latest/toolkit/components/telemetry/telemetry/data/environment.html#addons),
please look 
[in the AddonsView code](src/main/scala/views/AddonsView.scala).

The fields are all simple scalar values.
