#!/bin/bash

# NOTE: The version of this file on the master branch may also be pulled in
# by other projects (telemetry-streaming in particular), so keep that in mind
# when proposing changes.

# Copy $JAR and metadata to a local directory, then upload to S3.
# This uses the travis-ci/artifacts tool which references environment variables
# prefixed by AWS_ and ARTIFACTS_, some of which are injected by the CircleCI project.

set -x

# Change PWD to the root of the git repo.
cd $(git rev-parse --show-toplevel)

if [[ -z "$JAR" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

if [ ! -f "$JAR" ]; then
    echo "Artifact $JAR does not exist" 1>&2
    exit 1
fi


JAR_DIR="$CIRCLE_PROJECT_USERNAME/$CIRCLE_PROJECT_REPONAME"
JAR_NAME="$CIRCLE_PROJECT_REPONAME.jar"
TXT_NAME="$CIRCLE_PROJECT_REPONAME.txt"

# The "canonical jar" goes in a subdirectory with the job number so it won't be overwritten.
mkdir -p $JAR_DIR/$CIRCLE_BRANCH/$CIRCLE_BUILD_NUM
CANONICAL_JAR="$JAR_DIR/$CIRCLE_BRANCH/$CIRCLE_BUILD_NUM/$JAR_NAME"
cp $JAR $CANONICAL_JAR

# We also overwrite the current jar for the branch.
echo $CANONICAL_JAR >> $TXT_NAME
echo $CIRCLE_BUILD_URL >> $TXT_NAME
cp $JAR "$JAR_DIR/$CIRCLE_BRANCH/$JAR_NAME"
cp $TXT_NAME "$JAR_DIR/$CIRCLE_BRANCH/$TXT_NAME"

# Now upload to S3; see docs for artifact upload tool at https://github.com/travis-ci/artifacts
export ARTIFACTS_DEST="$HOME/bin/artifacts"
export ARTIFACTS_PERMISSIONS="public-read"
export ARTIFACTS_PATHS="$CIRCLE_PROJECT_USERNAME/"
export ARTIFACTS_TARGET_PATHS="/$CIRCLE_PROJECT_USERNAME/"
curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash
$ARTIFACTS_DEST upload
