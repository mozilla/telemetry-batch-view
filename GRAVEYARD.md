# Telemetry-batch-view Code Graveyard

This document records interesting code that we've deleted for the sake of discoverability for the future.

## Heavy Users

* [Removal PR](https://github.com/mozilla/telemetry-batch-view/pull/435)
* [Obsolete Dataset documentation](http://docs-origin.telemetry.mozilla.org/concepts/choosing_a_dataset.html#heavyusers)

Interesting bits: the Heavy Users job used a custom Paritioner called `ConsistentPartitioner` that optimized for copartitioning the same client_ids together even as the client_ids grow and wane.

