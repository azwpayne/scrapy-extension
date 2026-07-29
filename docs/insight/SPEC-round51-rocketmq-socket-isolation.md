# Round 51 — SPEC / PLAN / TASK: RocketMQ CI socket isolation

## Context and audit evidence

The exact GitHub Actions run for `f5b89a6` (`30461904820`) passed all five
unit-test lanes and 34 of 39 integration tests.  Its only failure was the
RocketMQ push/pop round trip after all broker containers had become ready.

The locked `rocketmq-python-client==5.1.1` calls
`Misc.get_local_ip()` while producing client metadata.  On a cache miss that
SDK opens a UDP socket and calls `connect(("8.8.8.8", 80))` only to select a
local route/source address.  The CI suite deliberately runs with
`pytest-socket` limited to `localhost`, `127.0.0.1`, and `::1`; its warning is
promoted to an exception by the project warning policy.  The SDK catches the
exception and returns `127.0.0.1`, but does not cache that fallback, so every
metadata update retries the forbidden probe and the live test becomes flaky.

The probe is neither a RocketMQ broker endpoint nor product traffic.  Adding
`8.8.8.8` to the CI allow-list would grant that public host for any protocol
and port because `pytest-socket` cannot express an UDP-only exception.

## Specification

- Keep the CI integration allow-list limited to loopback hosts; do not use
  `--force-enable-socket` or whitelist public IP addresses.
- The local RocketMQ integration fixture must pre-seed only the locked SDK's
  local-IP cache with `127.0.0.1` before `RocketMQBackend.connect()` and restore
  the prior value after the backend has disconnected.
- The adaptation is test-only: production RocketMQ connections retain their
  normal source-address discovery behavior.
- A regression test must prove the seeded SDK path returns loopback without
  calling `socket.socket.connect`.
- CI regression coverage must preserve the loopback-only allow-list and reject
  a public `8.8.8.8` exception or global socket enablement.

## Plan and independently verifiable tasks

- [ ] **R51-1 — Fixture isolation:** add a module-scoped cache fixture and
      make the live RocketMQ backend fixture depend on it.
- [ ] **R51-2 — Probe regression:** patch `socket.socket.connect` to fail and
      prove the cached SDK metadata lookup does not touch it.
- [ ] **R51-3 — CI guard:** assert the workflow retains only loopback hosts
      and does not force-enable all sockets.
- [ ] **R51-4 — Verify and re-audit:** run focused tests, the local RocketMQ
      fixture with the exact CI socket flags, static quality gates, an
      independent audit, and exact-SHA GitHub Actions.

## Acceptance criteria

1. The local and GitHub RocketMQ integration round trip completes while the
   effective socket allow-list contains only loopback hosts.
2. No fixture, test, or production path grants general outbound access to
   `8.8.8.8`.
3. The SDK cache is restored after the module fixture teardown, including when
   the backend fixture raises or disconnects.
4. The exact final `main` SHA has every GitHub Actions job successful.
