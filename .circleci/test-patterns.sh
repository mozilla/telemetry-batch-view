#!/bin/bash

# Find all test files and output a list of patterns to stdout like:
#  *AddonViewsTest
#  *ScalarsTest
#  etc.

# Change PWD to the root of the git repo.
cd $(git rev-parse --show-toplevel)

# Find the names of all classes extending FlatSpec and collect them into a file of patterns;
# we make liberal use of '  *' as a way of expressing "at least one space"
TEST_FILES=$(find src/test/ -name "*.scala")
sed -n 's/class  *\(.*\)  *extends  *FlatSpec.*/*\1/p' $TEST_FILES | sort | uniq > test_patterns.txt

# Use the circleci cli to choose a subset of test patterns to return;
# the output here will be different for each of the parallel jobs
# (based on CIRCLE_NODE_INDEX and CIRCLE_NODE_TOTAL vars);
# the output is piped through a few utilities to normalize whitespace.
circleci tests split test_patterns.txt | tr -d '\r' | grep . | paste -s -d' ' -
