The `crash_aggregates` table has 4 commonly-used columns:

* `submission_date` is the date pings were submitted for a particular aggregate.
    * For example, `select sum(stats['usage_hours']) from crash_aggregates where submission_date = '2016-03-15'` will give the total number of user hours represented by pings submitted on March 15, 2016.
    * The dataset is partitioned by this field. Queries that limit the possible values of `submission_date` can run significantly faster.
* `activity_date` is the day when the activity being recorded took place.
    * For example, `select sum(stats['usage_hours']) from crash_aggregates where activity_date = '2016-03-15'` will give the total number of user hours represented by activities that took place on March 15, 2016.
    * This can be several days before the pings are actually submitted, so it will always be before or on its corresponding `submission_date`.
    * Therefore, queries that are sensitive to when measurements were taken on the client should prefer this field over `submission_date`.
* `dimensions` is a map of all the other dimensions that we currently care about. These fields include:
    * `dimensions['build_version']` is the program version, like `46.0a1`.
    * `dimensions['build_id']` is the YYYYMMDDhhmmss timestamp the program was built, like `20160123180541`. This is also known as the "build ID" or "buildid".
    * `dimensions['channel']` is the channel, like `release` or `beta`.
    * `dimensions['application']` is the program name, like `Firefox` or `Fennec`.
    * `dimensions['os_name']` is the name of the OS the program is running on, like `Darwin` or `Windows_NT`.
    * `dimensions['os_version']` is the version of the OS the program is running on.
    * `dimensions['architecture']` is the architecture that the program was built for (not necessarily the one it is running on).
    * `dimensions['country']` is the country code for the user (determined using geoIP), like `US` or `UK`.
    * `dimensions['experiment_id']` is the identifier of the experiment being participated in, such as `e10s-beta46-noapz@experiments.mozilla.org`, or null if no experiment.
    * `dimensions['experiment_branch']` is the branch of the experiment being participated in, such as `control` or `experiment`, or null if no experiment.
    * `dimensions['e10s_enabled']` is whether E10S is enabled.
    * `dimensions['gfx_compositor']` is the graphics backend compositor used by the program, such as `d3d11`, `opengl` and `simple`. Null values may be reported as `none` as well.
    * All of the above fields can potentially be blank, which means "not present". That means that in the actual pings, the corresponding fields were null.
* `stats` contains the aggregate values that we care about:
    * `stats['usage_hours']` is the number of user-hours represented by the aggregate.
    * `stats['main_crashes']` is the number of main process crashes represented by the aggregate (or just program crashes, in the non-E10S case).
    * `stats['content_crashes']` is the number of content process crashes represented by the aggregate.
    * `stats['plugin_crashes']` is the number of plugin process crashes represented by the aggregate.
    * `stats['gmplugin_crashes']` is the number of Gecko media plugin (often abbreviated GMPlugin) process crashes represented by the aggregate.
    * `stats['content_shutdown_crashes']` is the number of content process crashes that were caused by failure to shut down in a timely manner.
    * `stats['gpu_crashes']` is the number of gpu process crashes represented by the aggregate.
