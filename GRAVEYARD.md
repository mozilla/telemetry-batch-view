# Telemetry-batch-view Code Graveyard

This document records interesting code that we've deleted for the sake of discoverability for the future.

## Heavy Users

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/435)
* [Obsolete Dataset documentation](http://docs-origin.telemetry.mozilla.org/concepts/choosing_a_dataset.html#heavyusers)

Interesting bits: the Heavy Users job used a custom Paritioner called `ConsistentPartitioner` that optimized for copartitioning the same client_ids together even as the client_ids grow and wane.

## Pioneer Online News Dwell Time v2

* [Removal commit](https://github.com/mozilla/telemetry-batch-view/commit/df063c252f0211678347aa976b050ab22af976ac)
* [State machine diagram](https://goo.gl/FMVjtB)

This dataset was created as a one-off for the purposes of the Online News pioneer study. It created sessions that measured dwell time on a tld based on logs sent from users. It used a state machine to create the sessions, which is mildly interesting.

## Crash Aggregates

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/530)
* [Obsolete Dataset documentation](http://docs-origin.telemetry.mozilla.org/datasets/obsolete/crash_aggregates/reference.html)

This dataset was created to count crashes on a daily basis, before we introduced error aggregates.

## Quantum RC

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/531)

This dataset was created to monitor that various metrics conformed to the Quantum release criteria expectations.

## Crash Summary

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/553)

This dataset was used to access crash pings, before we introduced the crash ping table in BigQuery.

## Generic Count and DAU

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/553)

These datasets were created to count clients on a daily and monthly basis, before we introduced clients last seen.

## Longitudinal

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/553)

This dataset was used to access all histograms for a 1% sample of clients, before we introduced the main ping table in BigQuery.

## Experiments Aggregates

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/553)

This dataset was used for experiment analysis, before it was deprecated in [Bug 1515134](https://bugzilla.mozilla.org/show_bug.cgi?id=1515134).

## Main Summary, Clients Daily, and Addons

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/553)

These jobs were reimplemented as BigQuery SQL in [bigquery-etl/sql/telemetry_derived/](https://github.com/mozilla/bigquery-etl/tree/master/sql/telemetry_derived/).

## Experiments Summary

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/558)

This job was reimplemented as BigQuery SQL in [bigquery-etl/sql/telemetry_derived/experiments_v1/query.sql](https://github.com/mozilla/bigquery-etl/blob/master/sql/telemetry_derived/experiments_v1/query.sql).
