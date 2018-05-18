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

# Set options that sbt will pass to the JVM
export SBT_OPTS="-Xss2M -Djava.security.policy=java.policy"

# Run tests
if [ $TRAVIS_BRANCH ]; then
   # under travis, submit code coverage data
   sbt "$@"
   bash <(curl -s https://codecov.io/bash)
elif [ $# -gt 0 ]; then
   # if args specified, run the tests with those
   sbt "$@"
else
   # default: just run the tests
   sbt test
fi
