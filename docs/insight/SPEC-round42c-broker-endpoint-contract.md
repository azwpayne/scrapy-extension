# Round 42C — SPEC / PLAN / TASK: broker endpoint grammar

**Base:** `main` after `3d8aa69`.

## Audit conclusion

Kafka currently accepts malformed bootstrap strings such as a URL or port zero.
RocketMQ accepts multi-DNS and IPv6 forms that the locked
`rocketmq-python-client==5.1.1` cannot safely interpret.  In particular, that
SDK permits at most one DNS endpoint and parses IPv6 with a colon split.
Allowing URL schemes is also misleading because RocketMQ transport security is
controlled only by `tls_enabled`.

## Specification

1. Add a shared no-network, ASCII-only broker-endpoint parser.  Errors are
   static and never echo an invalid input.
2. Kafka accepts comma lists of DNS, IPv4, bracketed IPv6, and raw portless
   IPv6 (canonicalized to brackets).  Explicit ports are ASCII decimal
   `1..65535`; schemes, userinfo, paths, queries, fragments, controls,
   Unicode hosts, pseudo IPv4, blank members, and raw IPv6-with-port fail.
3. RocketMQ requires an explicit `host:port`.  It allows a single DNS endpoint
   or a semicolon list of IPv4 endpoints; DNS+IPv4 mixes, multiple DNS values,
   IPv6, URL authority syntax, and invalid ports fail.
4. Kafka validates all three bootstrap fields while retaining an empty
   Confluent value's fallback behavior.  RocketMQ validates before SDK import
   or construction.  Runtime configuration mutation therefore fails before
   network I/O in both backends.
5. TLS choices remain solely in their typed security settings.  Examples are
   corrected to match the SDK contract.

## Plan

1. Implement the shared parser and static redaction allowlists.
2. Attach it to Kafka and RocketMQ settings/runtime validation seams.
3. Update example configuration and add valid, malformed, canonicalization,
   mutation-before-I/O, and TLS-regression tests.
4. Run focused/full checks and make a separate atomic commit.

## Task checklist

- [ ] Shared parser and settings validators.
- [ ] Runtime mutation guard and safe diagnostics.
- [ ] Documentation/example correction and regression matrix.
- [ ] Full verification and atomic commit.
