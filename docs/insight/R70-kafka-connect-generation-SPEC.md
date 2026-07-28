# R70 SPEC — Kafka direct connection generations

Kafka producer and admin clients form one direct connection generation. Public
`connect()` and `disconnect()` must serialize its construction, publication,
retirement, and close. A complete live pair is idempotent; a one-sided
residual is retired before any fresh connection attempt.
