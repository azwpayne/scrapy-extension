# R76 SPEC — Kafka failed candidate cleanup

Kafka failed connection cleanup must detach both candidates, attempt both
closes, and preserve the causal connection error. Direct cleanup retains its
first control exception only after attempting every sibling.
