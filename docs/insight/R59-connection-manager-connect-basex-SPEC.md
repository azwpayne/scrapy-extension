# R59 SPEC — ConnectionManager failed-connect BaseException cleanup

When a backend `connect()` raises `KeyboardInterrupt` or `SystemExit` after
allocating resources, `ConnectionManager` must invoke its `disconnect()` just
as it does for ordinary failures. Cleanup errors, including control exceptions,
must never replace the original connection failure; the manager must not
publish the failed backend.
