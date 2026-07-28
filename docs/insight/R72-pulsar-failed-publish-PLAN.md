# R72 PLAN

Confirm a logging failure after Pulsar publication leaves a zombie client,
make rollback identity/generation guarded, preserve the causal exception during
best-effort cleanup, add deterministic post-publication and stale-cleanup
regressions, verify, and atomically commit.
