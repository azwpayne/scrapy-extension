# Round 39A — SPEC / PLAN / TASK: literal loopback transport classifiers

**Base:** `main` after Round 38 MongoDB connection hardening. **Scope:**
RabbitMQ and Memcached plaintext-development exceptions found by the fresh
whole-repository security swarm.

## Audit conclusion

Both components classified every `*.localhost` name as a loopback endpoint.
That suffix is still a hostname resolved by the active DNS/hosts policy, so an
operator could send RabbitMQ credentials or unauthenticated Memcached traffic
to a remote address while the configuration accepted the local plaintext
exception.

## Specification

1. A loopback exception accepts only the exact hostname `localhost` (including
   its harmless trailing-dot spelling) or a literal IP address for which
   `ip_address(...).is_loopback` is true.
2. Any other hostname, including `attacker.localhost`, is remote. RabbitMQ
   must require verified TLS; Memcached must require the explicit
   `allow_remote_plaintext=True` acknowledgement.
3. Construction-time and mutable-runtime validation must apply the identical
   classifier before any SDK constructor is called. Errors must never echo a
   credential.

## Plan and tasks

1. Replace suffix-based classifiers in RabbitMQ and Memcached with exact-name
   checks while retaining literal IPv4/IPv6 loopback support.
2. Add construction-time and runtime regression tests for standalone and
   RabbitMQ cluster-node lookalike hosts; assert the relevant SDK constructor
   is not called.
3. Run focused transport suites, then the standard lint/type/security/full
   test gates before one atomic commit. A later Round 39 task owns Redis and
   Kafka P1 findings separately.

## Acceptance evidence

- `attacker.localhost` is rejected as remote for RabbitMQ without TLS and for
  Memcached without the explicit remote-plaintext opt-in.
- Existing exact `localhost`, `localhost.`, `127.0.0.1`, and `::1` local
  development paths stay valid.
- Mutating a valid settings object to the lookalike host fails before
  `BlockingConnection` or `MemcachedClient` construction.
