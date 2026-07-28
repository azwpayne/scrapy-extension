# R84 SPEC — Memcached failed probe candidate cleanup

A Memcached client candidate that fails its private `stats()` probe must be
closed best-effort without allowing close-time control exceptions or diagnostics
to replace the probe's causal failure.
