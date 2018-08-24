#!/bin/bash

set -euo pipefail
set -x

tbv_jar=$1
date=${2:-$(date +%Y%m%d --date=yesterday)}
bucket=${3:-"telemetry-test-bucket"}

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

    spark-submit \
        --master yarn \
        --deploy-mode client \
        --class com.mozilla.telemetry.views.${classname} \
        ${tbv_jar} ${options}
}

baseopt="--to $date --from $date --bucket $bucket"

submit MainSummaryView          "$baseopt --channel nightly --read-mode aligned"
submit ExperimentSummaryView    "$baseopt --experiment-limit 1"

# long running
submit CrashSummaryView         "$baseopt --dry-run"

###########################################################
#               Jobs without Test Options
###########################################################

# submit AddonsView
# submit CrashAggregateView
# submit ExperimentAnalysisView
# submit LongitudinalView
# submit MainEventsView
# submit SyncEventView
# submit SyncFlatView
# submit SyncView

# submit GenericCountView
# submit GenericLongitudinalView
# submit QuantumRCView
# submit RetentionView
