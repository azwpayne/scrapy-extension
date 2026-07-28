# R63 SPEC — MongoDB direct connection generations

MongoDB's public direct `connect()` and `disconnect()` operations must serialize
publication and retirement of a client graph. A complete live graph makes a
subsequent `connect()` an idempotent no-op. After a graph is retired, the next
connection generation must rebuild configuration-derived client options and
read preference from the current mutable settings before constructing its
client.
