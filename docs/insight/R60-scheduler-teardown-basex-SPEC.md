# R60 SPEC — Scheduler full teardown after BaseException

Scheduler shutdown must attempt every independent teardown action after a
`KeyboardInterrupt`/`SystemExit`: both signal disconnects, queue close, owned
dupefilter close, and manager release. Ordinary cleanup errors remain logged
and suppressed. The first control exception is re-raised only after all
actions and state reset have completed.
