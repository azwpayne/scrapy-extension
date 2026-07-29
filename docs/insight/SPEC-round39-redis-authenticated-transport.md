# Round 39B — SPEC / PLAN / TASK: authenticated Redis transport boundary

**Base:** `main` after Round 39A's literal-loopback classifier correction.
**Scope:** the Redis P1 identified by the fresh repository security swarm.

## Audit conclusion

Redis accepted remote standalone password authentication without TLS. Sentinel
and Cluster followed the same path: a credential could be handed to a control
plane, a discovered master, or a discovered cluster node without a verified
transport. The existing connection-plan snapshot already revalidated mutable
settings, but its settings validation did not express this transport contract.

## Specification

1. Authentication intent is present when any of `username`, `password`,
   `sentinel_username`, or `sentinel_password` is configured. The check never
   extracts or renders a `SecretStr` value.
2. An authenticated configuration may use plaintext only when its mode is
   `standalone` and its scalar host is exactly `localhost` (with an optional
   trailing dot) or a literal loopback IPv4/IPv6 address.
3. Sentinel and Cluster are never eligible for that exception, including when
   a seed is loopback: discovery can return a remote control-plane or data
   endpoint. The deprecated `master_slave` compatibility mode is also not an
   exception.
4. Every other authenticated path requires `ssl_enabled=True`, a non-blank CA
   file, certificate verification, and `ssl_check_hostname=True`. The Redis
   SDK must receive explicit `ssl_cert_reqs="required"`, so verification does
   not depend on an SDK default.
5. Construction and `RedisBackend.connect()` use the same invariant. A mutable
   settings object that attempts to change endpoint, authentication, TLS, or
   hostname verification must fail before a Redis, Sentinel, or RedisCluster
   SDK constructor is called. Public errors must not contain credentials.

## Plan and tasks

1. Add one strict loopback classifier and a complete authenticated-transport
   validator in Redis settings.
2. Route the existing strict connection-plan revalidation through that
   validator; retain the immutable non-secret generation snapshot.
3. Make verified TLS explicit in all Redis SDK construction paths.
4. Update existing mocked remote Sentinel/Cluster fixtures to model a valid
   TLS deployment instead of a now-forbidden plaintext one.
5. Add regression coverage for construction-time and runtime mutation,
   username-only and password authentication, all topology paths, hostname
   verification downgrade, `*.localhost`, and verified SDK kwargs.
6. Run focused Redis suites plus static, security, build, and full CI-equivalent
   gates before the Round 39B atomic commit.

## Acceptance evidence

- Remote authenticated standalone, Sentinel, Cluster, and `master_slave`
  configurations fail with the relevant named setting before SDK I/O.
- Exact `localhost`, `localhost.`, `127.0.0.1`, and `::1` standalone
  development configurations remain valid without TLS.
- A post-construction `ssl_check_hostname=False` downgrade fails before SDK
  I/O for standalone, Sentinel, and Cluster.
- Verified standalone and Cluster calls include TLS, CA, mandatory certificate
  verification, and hostname verification; the existing Sentinel suite covers
  both its control and discovered-master paths.
