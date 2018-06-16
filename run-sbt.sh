#!/bin/bash

# Wrapper script for executing sbt via docker, but interacting with
# the local filesystem. Useful for local development without installing
# Java and sbt.

set -e

# create dirs to cache .ivy2 and .sbt if they don't exist already
mkdir -p ~/.ivy2/ ~/.sbt/

IMAGE=$(sed -n 's/.*&sbt_image \(.*\)/\1/p' .circleci/config.yml)

docker run -it \
    -v ~/.ivy2:/root/.ivy2 \
    -v ~/.sbt:/root/.sbt \
    -v $PWD:/telemetry-batch-view \
    -w /telemetry-batch-view \
    $IMAGE \
    sbt "$@"
