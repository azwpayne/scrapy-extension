# R57 SPEC — SQS lease-drain BaseException cleanup

If SQS `disconnect()` is interrupted while waiting for active client leases,
it must preserve the first `KeyboardInterrupt`/`SystemExit`, finish draining
the admitted leases, clear detached state and caches, close the retired client,
then re-raise that exception. A later close control exception is re-raised only
when no earlier interruption exists. Ordinary close failures remain suppressed.
