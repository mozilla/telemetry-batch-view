The Addons dataset
==================

Generating the dataset
======================

The dataset is generated in the same way that the [Main
Summary](MainSummary.md) is generated, with one exception. You can specify a
different bucket name to use for reading the Main Summary data with the
`inbucket` argument. Without this argument, it uses the same bucket for input
and output. For example:

```bash
spark-submit \
    --master yarn \
    --deploy-mode client \
    --class com.mozilla.telemetry.views.AddonsView \
    telemetry-batch-view-1.1.jar \
    --bucket example_bucket \
    --inbucket another_bucket \
    --from 20160412 \
    --to 20160428
```
