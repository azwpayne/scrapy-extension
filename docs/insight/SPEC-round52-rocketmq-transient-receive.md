# Round 52 — SPEC / PLAN / TASK: RocketMQ transient receive diagnostics

## Context and audit evidence

The local RocketMQ fixture exposed the raw failure hidden by the public
`pop()` / `pop_with_ack()` error boundary:

```text
rocketmq.v5.exception.client_exception.InternalErrorException
50001, null. NullPointerException.
org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageActivity.receiveMessage
```

The Apache 5.x proxy emits that known transient condition immediately after a
topic is created.  The integration suite already intends to retry it (and the
related `no topic to receive message` route-cache race), but it calls the
public methods.  Their deliberate redaction boundary rebuilds the exception
as `QueueError("Failed to pop RocketMQ message.")`, so the test can no longer
recognize the transient driver cause and retries never occur.

The NPE is an upstream telemetry-registration race: the Proxy can service a
receive before its settings manager has recorded the client's telemetry.  The
Round 51 local-IP cache remains correct and useful because it prevents the
SDK's unrelated public-route probe under pytest-socket; it neither causes nor
eliminates the Proxy race.  The loopback-only socket allow-list remains the
desired policy.

## Specification

- Keep production `pop()` and `pop_with_ack()` redaction unchanged: no raw
  RocketMQ details may cross the public API boundary.
- Test only the live broker's known transient receive conditions through the
  decorated functions' original implementation (`__wrapped__`), where the
  test can inspect a local driver cause without publishing it.
- Retry only the exact, known NPE and no-topic signatures until the existing
  deadline; unrelated failures must remain test failures.
- Keep the Round 51 SDK-cache pre-seed, but replace its process-global socket
  constructor mock with a mock of only the SDK module's socket reference so it
  cannot race background gRPC threads.
- Preserve the CI assertion that integration tests allow only loopback hosts
  and never force-enable all sockets.

## Plan and independently verifiable tasks

- [ ] **R52-1 — Test receive helper:** centralize test-only calls to the raw
      RocketMQ pop implementations and classify only the two known transient
      broker signatures from their direct cause.
- [ ] **R52-2 — Retry semantics:** route `_drain` and the empty-queue probe
      through that helper; retain bounded retries and NPE accounting.
- [ ] **R52-3 — Preserve isolated cache regression:** retain the SDK cache
      fixture and change its assertion to avoid mutating global socket state.
- [ ] **R52-4 — Verify and re-audit:** run focused unit tests and the exact
      local loopback fixture, inspect logs for no leaked driver detail, then
      verify exact-SHA GitHub Actions.

## Acceptance criteria

1. A proxy NPE or no-topic race retries in the integration test, while an
   unrelated `QueueError` still fails immediately.
2. Public RocketMQ callers still receive the original static redacted error.
3. No test mocks a process-global socket constructor while gRPC threads run.
4. Local and GitHub integration runs keep the loopback-only socket policy and
   complete all enabled RocketMQ tests.
