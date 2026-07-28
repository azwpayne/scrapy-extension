# R52 SPEC — Pulsar endpoint authority validation

Each comma-separated Pulsar endpoint must have a host and, when present, a
numeric in-range port. Paths, queries, fragments, and userinfo are rejected at
settings construction and connection-time revalidation, before SDK creation.
