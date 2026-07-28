# R53 SPEC — RabbitMQ detached-handle BaseException cleanup

Closing a RabbitMQ channel can raise `KeyboardInterrupt`/`SystemExit`; the
connection must still close. `_close_handles` must independently attempt both
handles, suppress ordinary close failures, retain the first process-control
exception, and re-raise it after sibling cleanup.
