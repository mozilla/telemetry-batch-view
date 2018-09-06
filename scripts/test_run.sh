#!/bin/bash

set -uo pipefail
set -x

tbv_jar=$1
date=${2:-$(date +%Y%m%d --date='2 days ago')}
bucket=${3:-"telemetry-test-bucket"}
timeout=${4:-60}

if [[ -z ${tbv_jar} ]]; then
    echo "Missing telemetry-batch-view jar!"
    exit 1
fi

if [[ ${bucket} == "telemetry-parquet" ]]; then
    echo "Writing to telemetry-parquet is not allowed!"
    exit 1
fi


submit() {
    classname="$1"
    options="$2"

    timeout $timeout spark-submit \
        --master yarn \
        --deploy-mode client \
        --class com.mozilla.telemetry.views.${classname} \
        ${tbv_jar} ${options}

    # A timeout results in an error code of 124
    rv=$?
    if [[ $rv != 0 && $rv != 124 ]]; then
        echo "bad error code for $classname";
        exit 1;
    fi
}

baseopt="--to $date --from $date --bucket $bucket"

submit MainSummaryView          "$baseopt --channel nightly --read-mode aligned"
submit SyncView                 "$baseopt"
submit CrashAggregateView       "$baseopt"
submit MainEventsView           "$baseopt --inbucket telemetry-parquet"
submit AddonsView               "$baseopt --inbucket telemetry-parquet"
submit CrashSummaryView         "--to $date --from $date --outputBucket $bucket --dryRun"
submit LongitudinalView         "--to $date --bucket $bucket"
submit ExperimentAnalysisView   "--date $date --input s3://telemetry-parquet/experiments/v1 --output s3://$bucket/experiments_aggregates/v1"
submit ExperimentSummaryView    "$baseopt --inbucket telemetry-parquet --experiment-limit 1"


###########################################################
#               Jobs without Test Options
###########################################################

# submit SyncEventView
# submit SyncFlatView

# submit GenericCountView
# submit GenericLongitudinalView
# submit QuantumRCView
# submit RetentionView
