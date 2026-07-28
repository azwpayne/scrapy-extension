# R71 SPEC — Elasticsearch failed-publish rollback

Any Elasticsearch `connect()` failure must leave no published client
generation. If failure occurs after candidate publication, detach only that
candidate, close it best-effort, and preserve the original connection failure
over cleanup errors.
