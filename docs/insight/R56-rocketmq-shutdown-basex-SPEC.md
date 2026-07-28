# R56 SPEC — RocketMQ detached-client BaseException cleanup

RocketMQ must attempt both detached clients when shutdown raises
`KeyboardInterrupt`/`SystemExit`. Public `disconnect()` re-raises the first
control exception after sibling cleanup. Failed `connect()` cleanup suppresses
cleanup control exceptions so the original startup failure stays primary.
Ordinary cleanup `Exception`s retain their existing best-effort logging.
