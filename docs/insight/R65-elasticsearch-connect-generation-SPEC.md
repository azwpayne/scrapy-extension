# R65 SPEC — Elasticsearch direct connection generations

Elasticsearch direct lifecycle calls must create one private client candidate
per connection generation. Only a candidate that has passed its health check
and index initialization may be published with its immutable snapshot.
Overlapping `connect()` calls share that generation, and `disconnect()` waits
for its initialization before retiring it.
