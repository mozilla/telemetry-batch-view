# Analyzing Telemetry Data

This document provides a quick overview of the available tools for analyzing
telemetry data. For a description of the available datasets and their contents,
take a look at [Choosing a Dataset].

Before making final decisions with your findings, consider getting your query
or analysis reviewed by a member of the data pipeline team.

## Table of Contents

* Ad Hoc Analysis
  * [STMO](http://sql.telemetry.mozilla.org), Mozilla's instance of re:dash
  * [ATMO](http://analysis.telemetry.mozilla.org)

# Tools for Ad-Hoc Analysis

## STMO

STMO provides an interactive SQL interface for all of the datasets described in
[Choosing a Dataset].

STMO is the easiest and fastest way to get started, but SQL has its limits for
more complicated analyses. Keep in mind that STMO is on a shared cluster.
Queries over main_summary can bog down the cluster and affect performance for
other users.

STMO is Mozilla's instance of [re:dash](http://redash.io/).

STMO is pretty intuitive, so feel free to dive in. For reference, the
documentation is [here](http://wiki.mozilla.org/Custom_dashboards_with_re:dash).

## ATMO

ATMO allows you to spin up compute clusters with access to Telemetry data and a
preconfigured Python/Spark analysis environment.

Using an ATMO cluster allows you to perform more complicated analyses than are
possible with SQL on STMO. You can create ATMO clusters with up to 20 worker
nodes so analyses can complete faster. ATMO clusters have access to the
derived datasets as well as raw ping data.

Keep in mind there are some startup and maintenance costs in using ATMO. For
quicker analyses, prefer STMO. Be sure to save your work, the cluster will be
killed within 24 hours.

The cluster provides a Python environment in your browser using Jupyter
Notebooks. The Python environment comes preconfigured with Spark, which is a
general purpose large scale data processing engine. You will also have access
to the cluster via SSH.

For more information, review [Custom Analysis with
Spark](https://wiki.mozilla.org/Telemetry/Custom_analysis_with_spark)

[Choosing a Dataset]: choosing_a_dataset.md "Choosing a Dataset"
