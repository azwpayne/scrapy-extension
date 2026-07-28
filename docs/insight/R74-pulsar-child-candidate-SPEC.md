# R74 SPEC — Pulsar child candidate publication

Pulsar producer and consumer candidates interrupted after SDK construction but
before cache publication must be closed best-effort. A published handle belongs
to its live generation and must not be closed by stale candidate rollback.
