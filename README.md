# aws-lambda-parquet

This is a Scala framework to build derived datasets of [Telemetry](https://wiki.mozilla.org/Telemetry) data through either a set of scheduled batch jobs or a fleet of AWS lambda functions. 

Raw JSON [pings](https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/toolkit/components/telemetry/telemetry/pings.html) are stored on S3 within [framed Heka records](https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing). Reading the raw data in through e.g. Spark can be quite slow as for a given analysis only a few fields are typically used; not to mention the cost of parsing the JSON blob. Defining a derived [Parquet](https://parquet.apache.org/) dataset that contains only a subset of fields can drastically improve the performance of analysis jobs.

A derived dataset can be build online, with an AWS lambda function, or offline, with a scheduled batch job. The converted datasets are stored in the bucket specified in [*application.conf*](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/resources/application.conf#L2).

### Offline

A derived dataset can be build offline by subclassing the [`BatchDerivedStream`](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/scala/BatchDerivedStream.scala) trait and implementing:
- `buildSchema`, which retuns the Avro schema of the derived stream
- `buildRecord`, which given a [Heka message](https://hekad.readthedocs.org/en/latest/message/index.html#message-variables) returns a derived Avro record
- `streamName`, which returns the dataset name used as input, as defined in *sources.json* in the metadata bucket, e.g. "telemetry-executive-summary"

#### Execution
Given a subtype of `BatchDerivedStream` of type `MyStream`, a dataset for the 28th of October can be generated with:
```
sbt "run-main telemetry.BatchConverter --from-date 20151028 --to-date 20151028 MyStream"
```

### Online

An online derived stream can be defined by subclassing the [`OnlineDerivedStream`](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/scala/OnlineDerivedStream.scala) trait and implementing:
- `buildSchema`, which retuns the Avro schema of the derived stream
- `buildRecord`, which given a Heka message returns a derived Avro record

See [`ExampleStream.scala`](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/scala/streams/ExampleStream.scala) for a simple stream.

Upon receiving a notification that a new raw data file has been uploaded to S3, the AWS lambda function will fetch the raw data file, convert it to a derived Parquet file and finally upload it to the configured bucket.

#### Deployment
```
ansible-playbook ansible/deploy.yml -e '@ansible/envs/dev.yml' -i ansible/inventory
```
