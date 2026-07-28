# R58 SPEC — Elasticsearch validated runtime snapshot

Elasticsearch settings are mutable after construction, so `connect()` must
revalidate and freeze every transport, authentication, and capability-index
value before invoking the SDK. A live client must route all queue, set, and
storage operations through that immutable snapshot. Invalid post-construction
mutations must fail before client construction; disconnect clears the snapshot
with its client so a later valid reconnect can capture fresh settings.
