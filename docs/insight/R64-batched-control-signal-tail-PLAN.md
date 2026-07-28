# R64 PLAN

Confirm the flush snapshot is cleared before writes and only ordinary
exceptions trigger tail restoration, extend restoration to every
`BaseException` while preserving re-raise semantics, add a deterministic
`KeyboardInterrupt` retry-order regression, verify, and make one atomic
commit.
