# R66 PLAN

Confirm repeated or overlapping RocketMQ connects can overwrite and leak
producer/consumer pairs, serialize the full direct lifecycle with a dedicated
re-entrant lock, preserve the R56 shutdown exception semantics, add sequential
and event-driven concurrency regressions, verify, and atomically commit.
