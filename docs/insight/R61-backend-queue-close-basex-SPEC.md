# R61 SPEC — BackendQueue phase-wise BaseException teardown

`BackendQueue.close()` must continue through lease draining, snapshot
persistence, and destructive strategy close after a control exception from an
earlier phase. It records the first `KeyboardInterrupt`/`SystemExit`, marks
close completion and wakes concurrent close callers, then re-raises that first
exception. Ordinary snapshot failures remain best-effort as before.
