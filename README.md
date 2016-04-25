# telemetry-batch-view

This is a Scala "framework" to build derived datasets, also known as [batch views](http://robertovitillo.com/2016/01/06/batch-views/), of [Telemetry](https://wiki.mozilla.org/Telemetry) data.

[![Build Status](https://travis-ci.org/mozilla/telemetry-batch-view.svg?branch=master)](https://travis-ci.org/mozilla/telemetry-batch-view)
[![codecov.io](https://codecov.io/github/mozilla/telemetry-batch-view/coverage.svg?branch=master)](https://codecov.io/github/mozilla/telemetry-batch-view?branch=master)

Raw JSON [pings](https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/toolkit/components/telemetry/telemetry/pings.html) are stored on S3 within files containing [framed Heka records](https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing). Reading the raw data in through e.g. Spark can be slow as for a given analysis only a few fields are typically used; not to mention the cost of parsing the JSON blobs. Furthermore, Heka files might contain only a handful of records under certain circumstances.

Defining a derived [Parquet](https://parquet.apache.org/) dataset, which uses a columnar layout optimized for analytics workloads, can drastically improve the performance of analysis jobs while reducing the space requirements. A derived dataset might, and should, also perform heavy duty operations common to all analysis that are going to read from that dataset (e.g., parsing dates into normalized timestamps).

The converted datasets are stored in the bucket specified in [*application.conf*](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/resources/application.conf#L2).

### Adding a new derived dataset

See the [streams](https://github.com/mozilla/telemetry-batch-view/tree/master/src/main/scala/streams) folder for `DerivedStream`-based datasets.

See the [views](https://github.com/mozilla/telemetry-batch-view/tree/master/src/main/scala/views) folder for view-based datasets.

See the [docs](https://github.com/mozilla/telemetry-batch-view/tree/master/docs) folder for more information about the derived datasets.

### Development

To set up a development environment, install [SBT](http://www.scala-sbt.org/) and [Spark](http://spark.apache.org/).

To run tests:
```bash
sbt test
```

To generate a `DerivedStream`-based dataset `MyStream` for October 28, 2015 to October 29, 2015:
```bash
sbt "run-main telemetry.DerivedStream --from-date 20151028 --to-date 20151029 MyStream"
```

To generate a view-based dataset `MyView` for October 28, 2015 to October 29, 2015:
```bash
sbt "run-main telemetry.views.MyView --from-date 20151028 --to-date 20151029"
```

### Distributed execution
Build an uber-JAR (a JAR with all dependencies and classes bundled) with:
```bash
sbt assembly
```

For a `DerivedStream`-based dataset `MyStream`, submit the job with:
```bash
spark-submit --master yarn-client --class telemetry.DerivedStream target/scala-2.10/telemetry-batch-view-*.jar --from-date 20151028 --to-date 20151029 MyStream
```

For a view-based dataset `MyView`, submit the job with:
```bash
spark-submit --master yarn-client --class telemetry.views.MyView target/scala-2.10/telemetry-batch-view-*.jar --from-date 20151028 --to-date 20151029
```

### Caveats
If you run into memory issues during compilation time issue the following command before running sbt:
```bash
export JAVA_OPTIONS="-Xss128M -Xmx2048M" 
```

Currently, due to [avro-parquet issues](https://issues.apache.org/jira/browse/HIVE-12828), Parquet writing only works under the `spark-submit` commands (under `sbt`, Parquet writing fails with multiple different errors).
