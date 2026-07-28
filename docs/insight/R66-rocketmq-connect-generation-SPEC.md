# R66 SPEC — RocketMQ direct connection generations

RocketMQ's producer and consumer are one lifecycle generation. Direct
`connect()` and `disconnect()` must serialize startup, publication, retirement,
and shutdown. A complete live pair makes a later `connect()` a no-op; a
one-sided residual is retired before a fresh generation is attempted.
