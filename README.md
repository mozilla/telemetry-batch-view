# aws-lambda-parquet
Heka -> Parquet converter

This is an AWS lambda function written in Scala to convert [Heka files](https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing) to Parquet files.

### Deployment
```
ansible-playbook ansible/deploy.yml -e '@ansible/envs/dev.yml' -i ansible/inventory
```
