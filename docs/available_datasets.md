Data Set Documentation
======================

This document describes a set of datasets which can be queried using
re:dash/sql.telemetry.mozilla.org (s.t.m.o). In addition, they can be
queried using a Spark cluster - see 
[these directions](https://wiki.mozilla.org/Telemetry/Custom_analysis_with_spark#How_can_I_load_parquet_datasets_in_a_Jupyter_notebook.3F).
The Longditudinal dataset is also available natively within Spark, see the
[longitudinal tutorial](https://github.com/mozilla/emr-bootstrap-spark/blob/master/examples/Longitudinal%20Dataset%20Tutorial.ipynb).

Longitudinal
------------
[Complete documentation](Telemetry/LongitudinalExamples "wikilink")

The longitudinal dataset is a summary of main pings. If you're not sure which
dataset to use for your query, this is probably what you want. It differs from
the main_summary table in two important ways:

* The longitudinal dataset groups all data for a client-id in the same row.
  This makes it easy to report profile level metrics. Without this deduping,
  metrics would be weighted by the number of submissions instead of by clients.
* The dataset uses a 1% of all recent profiles, which will reduce query
  computation time and save resources. The sample of clients will be stable over
  time.

Accordingly, one should prefer using the Longitudinal dataset except in the
rare case where a 100% sample is strictly necessary.

As discussed in the [Longitudinal Data Set Example Notebook](https://gist.github.com/vitillo/627eab7e2b3f814725d2):

    The longitudinal dataset is logically organized as a table where rows
    represent profiles and columns the various metrics (e.g. startup time). Each
    field of the table contains a list of values, one per Telemetry submission
    received for that profile. [...]

    The current version of the longitudinal dataset has been build with all
    main pings received from 1% of profiles across all channels with [...] up to
    180 days of data.

Main Summary
------------

[Complete Documentation](https://github.com/mozilla/telemetry-batch-view/blob/master/docs/MainSummary.md)

Like the longitudinal dataset, main summary summarizes [main
pings](https://gecko.readthedocs.io/en/latest/toolkit/components/telemetry/telemetry/data/main-ping.html).
Each row corresponds to a single ping. This table does no sampling and
includes all desktop pings.

### Caveats

Querying against main summary on SQL.t.m.o/re:dash can **impact
performance for other users** and can **take a while to complete**
(\~30m for simple queries). Since main summary includes a row for every
ping, there are a large number of records which can consume a lot of
resources on the shared cluster.

Instead, we recommend using the Longitudinal dataset where possible if
querying from re:dash/s.t.m.o. The longitudinal dataset samples to 1% of
all data and organized the data by client\_id. In the odd case where
these queries are necessary, limit to a short submission\_date\_s3 range
and ideally make use of the sample\_id field. Ideally, users who require
this dataset would use Spark.

Cross Sectional
---------------

The Cross Sectional dataset is a simplified version of the Longitudinal
dataset.

The majority of Longitudinal columns contain array values with one
element for each ping, which is difficult to work with in SQL. The Cross
Sectional dataset **replaces these array-valued columns with summary
statistics**. To give an example, the Longitudinal dataset will contain
a column named "geo\_country" where each row is an array of locales for
one client (e.g. array\<"en\_US", "en\_US", "en\_GB"\>). Instead, the
Cross Sectional dataset includes a column named "geo\_country\_mode"
where each row contains a single string representing the mode (e.g.
"en\_US"). The Cross Sectional column is **easier to work with** in SQL
and is more representative than just choosing a single value from the
Longitudinal array.

Note that the Cross Sectional dataset is derived from the Longitudinal
dataset, so the dataset is a **1% sample of main pings**

This dataset is sometimes abbreviated as the **xsec dataset**. You can
find the current version of the code
[here](https://github.com/mozilla/telemetry-batch-view/blob/master/src/main/scala/com/mozilla/telemetry/views/CrossSectionalView.scala).
This dataset is under active development, please **contact
rharter@mozilla.com with any questions**.

Client Count
------------

The Client Count dataset is simply a count of clients in a time period,
separated out into a set of dimensions.

This is useful for questions similar to: *How many X type of users were
there during Y?* - where X is some dimensions, and Y is some dates.
Examples of X are: E10s Enabled, Operating System Type, or Country. For
a complete list of dimensions, see
[here](https://github.com/mozilla/telemetry-batch-view/blob/master/src/main/scala/com/mozilla/telemetry/views/ClientCountView.scala#L22).

Client Count does not contain a traditional int count column, instead
the counts are stored as a HyperLogLogs in the hll column. The count of
the hll is found using `cardinality(cast(hll AS HLL))`, and different
hll's can be merged using `merge(cast(hll AS HLL))`. An example can be
found in the [Firefox ER
Reporting](https://sql.telemetry.mozilla.org/queries/81/source#129).

### Caveats

Currently there is no Python wrapper for the HyperLogLog library, so the
client count dataset is unavailable in Spark.

Crash Aggregates
----------------

[Complete
Documentation](https://github.com/mozilla/telemetry-batch-view/blob/master/docs/CrashAggregateView.md)

The Crash Aggregates dataset compiles crash statistics over various
dimensions for each day. Example dimensions include channel and country,
example statistics include usage hours and plugin crashes. See the
[complete
documentation](https://github.com/mozilla/telemetry-batch-view/blob/master/docs/CrashAggregateView.md)
for all available dimensions and statistics.

This dataset is good for queries of the form *How many crashes did X
types of users get during time Y?* and *Which types of users crashed the
most during time Y?*.

Mobile Metrics
--------------

The android\_events, android\_clients, android\_addons, and
mobile\_clients tables are documented here:
<https://wiki.mozilla.org/Mobile/Metrics/Redash>

# Appendix
For reference, this documentation was previously hosted here:
https://wiki.mozilla.org/Telemetry/Available_Telemetry_Datasets_and_their_Applications
