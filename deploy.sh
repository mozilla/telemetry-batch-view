#!/bin/bash
set -x

if [[ -z "$JAR" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

if [ ! -f "$JAR" ]; then
    echo "Artifact $JAR does not exist" 1>&2
    exit 1
fi

TRAVIS_REPO_OWNER=${TRAVIS_REPO_SLUG%/*}
SLUG=${TRAVIS_REPO_SLUG#*/}
JAR_DIR="$TRAVIS_REPO_OWNER/$SLUG"
JAR_NAME="$SLUG.jar"
TXT_NAME="$SLUG.txt"

if [[ -z "$TRAVIS_TAG" ]]; then
    BRANCH_OR_TAG=$TRAVIS_BRANCH
    ID=$TRAVIS_JOB_NUMBER
else
    # Only continue uploading jar if the tag was on master branch
    pushd ${TRAVIS_BUILD_DIR}
    # Finds all branches that contain the commit sha of TRAVIS_TAG
    output=$(git branch -r --contains `git rev-parse --verify ${TRAVIS_TAG}^{commit}`)

    if [[ $output =~ .*origin\/master.* ]]; then
        echo "Tag ${TRAVIS_TAG} is on master branch. Continuing upload..."
    else
        echo "Tag ${TRAVIS_TAG} is not on master branch. Skipping upload"
        # Exit 0 so the travis build doesn't fail
        exit 0
    fi
    popd
    BRANCH_OR_TAG=tags
    ID=$TRAVIS_TAG
fi

CANONICAL_JAR="$JAR_DIR/$BRANCH_OR_TAG/$ID/$JAR_NAME"
echo $CANONICAL_JAR > $TXT_NAME

mkdir -p $JAR_DIR/$BRANCH_OR_TAG/$ID
cp $JAR $CANONICAL_JAR
cp $JAR "$JAR_DIR/$BRANCH_OR_TAG/$JAR_NAME"
cp $TXT_NAME "$JAR_DIR/$BRANCH_OR_TAG/$TXT_NAME"

curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash

export ARTIFACTS_PERMISSIONS="public-read"
export ARTIFACTS_PATHS="$TRAVIS_REPO_OWNER/"
export ARTIFACTS_TARGET_PATHS="/$TRAVIS_REPO_OWNER/"
artifacts upload
