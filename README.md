# telemetry-parquet

This is a Scala "framework" to build derived datasets of [Telemetry](https://wiki.mozilla.org/Telemetry) data through either a set of scheduled batch jobs.

Raw JSON [pings](https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/toolkit/components/telemetry/telemetry/pings.html) are stored on S3 within [framed Heka records](https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing). Reading the raw data in through e.g. Spark can be quite slow as for a given analysis only a few fields are typically used; not to mention the cost of parsing the JSON blob. Defining a derived [Parquet](https://parquet.apache.org/) dataset that contains only a subset of fields can drastically improve the performance of analysis jobs.

The converted datasets are stored in the bucket specified in [*application.conf*](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/resources/application.conf#L2).

### Adding a new derived dataset

A derived dataset can be defined by subclassing the [`BatchDerivedStream`](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/scala/BatchDerivedStream.scala) trait and implementing:
- `streamName`, returns the dataset name used as input, as defined in *sources.json* in the metadata bucket, e.g. "telemetry-executive-summary"
- `buildSchema`, returns the Avro schema of the derived dataset
- `buildRecord`, given a [Heka message](https://hekad.readthedocs.org/en/latest/message/index.html#message-variables) returns a derived Avro record
- `[prefixGroup]`, returns the group in which an input key is going to be processed in; each group is further split in chunks that are processed in parallel tasks

See [`ExecutiveStream.scala`](https://github.com/vitillo/telemetry-parquet/blob/master/src/main/scala/streams/ExecutiveStream.scala) for a simple stream.

### Execution
Given a subtype of `BatchDerivedStream` of type `MyStream`, a dataset for the 28th of October can be generated with:
```
sbt "run-main telemetry.BatchDerivedStream --from-date 20151028 --to-date 20151028 MyStream"
```
