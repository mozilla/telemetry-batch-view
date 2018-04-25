#!/bin/bash

set -e

# create dirs to cache .ivy2 and .sbt if they don't exist already
mkdir -p ~/.ivy2/ ~/.sbt/

# if we are not inside the docker container, run this command *inside* the
# docker container
if [ ! -f /.dockerenv ]; then
    docker run -t -i -v ~/.ivy2:/root/.ivy2 -v ~/.sbt:/root/.sbt -v $PWD:/telemetry-batch-view telemetry-batch-view ./run-sbt.sh "$@"
    exit $?
fi

# Run tests
if [ $TRAVIS_BRANCH ]; then
   # under travis, submit code coverage data
   sbt -J-Xss2M "$@"
   bash <(curl -s https://codecov.io/bash)
elif [ $# -gt 0 ]; then
   # if args specified, run the tests with those
   sbt -J-Xss2M "$@"
else
   # default: just run the tests
   sbt -J-Xss2M test
fi
