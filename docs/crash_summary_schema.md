```
root
 |-- client_id: string (nullable = true)
 |-- normalized_channel: string (nullable = true)
 |-- build_version: string (nullable = true)
 |-- build_id: string (nullable = true)
 |-- channel: string (nullable = true)
 |-- application: string (nullable = true)
 |-- os_name: string (nullable = true)
 |-- os_version: string (nullable = true)
 |-- architecture: string (nullable = true)
 |-- country: string (nullable = true)
 |-- experiment_id: string (nullable = true)
 |-- experiment_branch: string (nullable = true)
 |-- e10s_enabled: boolean (nullable = true)
 |-- e10s_cohort: string (nullable = true)
 |-- gfx_compositor: string (nullable = true)
 |-- payload: struct (nullable = true)
 |    |-- crashDate: string (nullable = true)
 |    |-- processType: string (nullable = true)
 |    |-- hasCrashEnvironment: boolean (nullable = true)
 |    |-- metadata: map (nullable = true)
 |    |    |-- key: string
 |    |    |-- value: string (valueContainsNull = true)
 |    |-- version: integer (nullable = true)


```
For more detail on where these fields come from in the
[raw data](https://gecko.readthedocs.io/en/latest/toolkit/components/telemetry/telemetry/data/crash-ping.html),
please look at the case classes [in the CrashSummaryView code](src/main/scala/views/CrashSummaryView.scala).
