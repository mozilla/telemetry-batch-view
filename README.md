# aws-lambda-parquet

This is a Scala framework to build derived data streams of Telemetry data through a fleet of AWS lambda functions. 

Raw JSON [pings](https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/toolkit/components/telemetry/telemetry/pings.html) are stored on S3 within [framed Heka records](https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing). Reading the raw data in through e.g. Spark can be quite slow as for a given analysis only a few fields are typically used; not to mention the cost of parsing the JSON blob. Defining a derived Parquet stream that contains only a subset of fields can help with that.

In its current incarnation, a derived stream can be defined by subclassing the *DerivedStream* trait and implementing:
- `buildSchema`, a method that retuns the Avro schema of the derived stream
- `buildRecord`, a method that given a [Heka message](https://hekad.readthedocs.org/en/latest/message/index.html#message-variables) returns a derived Avro record

See [`ExampleStream.scala`](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/scala/streams/ExampleStream.scala) for a simple stream.

Upon receiving a notification that a new raw data file has been uploaded to S3, the lambda function will fetch the raw data file, convert it to a derived Parquet file and finally upload it to a configurable bucket.

### Deployment
```
ansible-playbook ansible/deploy.yml -e '@ansible/envs/dev.yml' -i ansible/inventory
```
