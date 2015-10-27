# aws-lambda-parquet
Heka -> Parquet converter

This is an AWS lambda function written in Scala to convert [Heka files](https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing) to Parquet files.

In its current incarnation, the function does the following once it receives a *PUT* notification for key **K** in bucket **B**, where **K** matches *prefix/rest_of_key*:
- fetch the Avro schema in *prefix/schema.json*
- read the json payloads in the Heka file associated with **K**
- deserialize the json payloads to Avro records
- serialize the Avro records to a Parquet file
- upload the Parquet file to *parquet/prefix/rest_of_key*

An Heka file must contain a json blob per message that matches its corresponding Avro schema.

### Deployment
```
ansible-playbook ansible/deploy.yml -e '@ansible/envs/dev.yml' -i ansible/inventory
```
