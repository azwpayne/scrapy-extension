# R63 PLAN

Confirm overlapping direct MongoDB connects can allocate an orphaned client and
that cached connection options survive a disconnect, serialize the public
lifecycle with one re-entrant lock, refresh caches only for a new generation,
add deterministic concurrency and reconnect regressions, verify, and make one
atomic commit.
