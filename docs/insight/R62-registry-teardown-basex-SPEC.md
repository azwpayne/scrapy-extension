# R62 SPEC — ConnectionManager forced-registry teardown

Forced registry eviction and `clear_registry()` must atomically retire and
detach every victim before invoking potentially blocking backend teardown.
Any victim cleanup failure, including `KeyboardInterrupt`/`SystemExit`, is
best-effort: it must not prevent later victims from being detached or turn a
successfully acquired replacement manager into an unpaired registry entry.
