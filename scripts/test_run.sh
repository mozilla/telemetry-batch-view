#!/bin/bash
#
# Run a basic sanity check for command line arguments in jobs.
#
# This script will submit the telemetry-batch-view job to Spark. This is meant
# to catch runtime errors that occur within the first few minutes of execution.
# This script can be run to determine whether a job will fail on EMR.
#
# To run the test:
#   - sbt assembly
#   - scp the target/scala-*/telemetry-batch-view-*.jar to an ATMO cluster
#   - scp this script to the same directory
#   - execute
#       * `$ ./test_run.sh <jar_path>`
#
# The test will run each of the specified jobs for a minute and timeout. If the
# job is fated to fail, it'll generally happen when parsing the command line
# options ~40 seconds after submission.


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

    # timeout once the job has had enough time to run through scallop configurations
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
