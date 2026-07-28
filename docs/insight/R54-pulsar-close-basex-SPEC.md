# R54 SPEC — Pulsar detached-handle BaseException cleanup

When a Pulsar consumer, producer, or client close raises
`KeyboardInterrupt`/`SystemExit`, `disconnect()` must still attempt every
detached handle. It must retain and re-raise the first control exception only
after all sibling cleanup has run. Ordinary close `Exception`s remain logged
and suppressed; lifecycle state remains detached before cleanup starts.
