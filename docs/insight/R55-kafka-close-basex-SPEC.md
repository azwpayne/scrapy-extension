# R55 SPEC — Kafka detached-client BaseException cleanup

When a detached Kafka producer, consumer, or admin client close raises
`KeyboardInterrupt`/`SystemExit`, `disconnect()` must still attempt every
sibling close. It must re-raise the first control exception only after all
clients have been attempted. Ordinary close `Exception`s retain the existing
best-effort suppression behavior and lifecycle state remains detached first.
