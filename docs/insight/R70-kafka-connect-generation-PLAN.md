# R70 PLAN

Confirm direct Kafka lifecycle calls can leak or publish a producer/admin pair
after disconnect, add a dedicated generation lock without extending the
delivery bookkeeping critical section, add deterministic repeated and
event-driven concurrent regressions, verify, and atomically commit.
