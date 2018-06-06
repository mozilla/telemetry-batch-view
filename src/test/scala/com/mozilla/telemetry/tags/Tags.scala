package com.mozilla.telemetry.tags

import org.scalatest.Tag

// These tags are used to parallelize tests when they run on TravisCI;
// The .travis.yml defines the different test jobs and it's expected that
// there will be one job per tag defined here plus one additional job to
// catch all untagged tests.

object ScalarAnalyzerBuild extends Tag("ScalarAnalyzerBuild")
object ExperimentsBuild extends Tag("ExperimentsBuild")
object ClientsDailyBuild extends Tag("ClientsDailyBuild")

