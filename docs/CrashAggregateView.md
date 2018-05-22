The Crash Aggregate View
========================

To generate the dataset for April 10, 2016 to April 11, 2016:
```bash
sbt "runMain com.mozilla.telemetry.views.CrashAggregateView --from 20160410 --to 20160411 --bucket telemetry-bucket"
```

**Note:** Currently, due to [avro-parquet
issues](https://issues.apache.org/jira/browse/HIVE-12828), Parquet writing only
works under the `spark-submit` commands - the above example will fail. This
will be fixed when avro-parquet updates or is removed.

For distributed execution, we can build a self-contained JAR file, then run it
with Spark. To generate the dataset for April 10, 2016 to April 11, 2016:
```bash
sbt assembly
spark-submit --master yarn-client --class com.mozilla.telemetry.views.CrashAggregateView target/scala-2.11/telemetry-batch-view-*.jar --from 20160410 --to 20160411 --bucket telemetry-bucket
```
