"""RocketMQ integration tests (R2-A4 foundation, part 6 — sextet complete).

Completes the integration sextet (Redis R46, MongoDB R47, ElasticSearch R49,
RabbitMQ R54, Kafka R55, RocketMQ here). This suite is the **verification of
the R7 fix**: pre-R7, ``connect()`` never called ``consumer.start()`` and
``pop()`` never subscribed, so pop *always* returned None — the entire
RocketMQ backend was silently broken, invisible because every test mocked
the consumer. A round-trip against a real broker is the only thing that
proves R7 actually fixed it.

RocketMQ semantics these tests respect (verified against rocketmq.py,
apache rocketmq-python-client 5.1.1 gRPC):
- **Deferred-ack** (initiative #4): ``pop`` / ``pop_with_ack`` return the
  body WITHOUT acking; the caller acks via ``ack(token=msg)``. A crash before
  ack → the broker's invisible-duration window redelivers (at-least-once).
  ``_drain`` acks each message as it arrives.
- A background pump owns RocketMQ Proxy's mandatory five-second long poll;
  ``pop(timeout=t)`` waits only on the local delivery condition.
- ``queue_len`` raises ``NotImplementedError`` (no broker-side depth RPC) so
  unknown depth cannot be mistaken for an empty queue.
- Topic name is ``{topic_prefix}_{queue_name}``. **RocketMQ topic names
  reject colons**, so this suite uses hyphen-delimited queue names (not the
  ``inttest:`` colon style of the other suites) or pushes fail.

What's pinned
-------------
- ``test_push_pop_round_trip`` — N in → N out, no loss. This is the R7
  verification: pre-R7 this returned 0 (pop always None).
- ``test_pop_empty_returns_none`` — pop on a topic with no messages returns
  None after the receive timeout (no spurious hang/raise).
- ``test_queue_len_reports_unsupported`` — RocketMQ reports that depth is
  unavailable instead of pretending the queue is empty.

Running
-------
Skipped by default. Point at a RocketMQ gRPC PROXY (the broker must run
with ``--enable-proxy``, which serves gRPC on 8081). The apache
``rocketmq-python-client`` 5.1.1 client speaks gRPC to the proxy, NOT the
legacy remoting port (10911)::

    SCRAPY_TEST_INTEGRATION=1 SCRAPY_TEST_ROCKETMQ_NAMESRV=localhost:8081 \
      uv run --no-sync pytest tests/integration -q \\
        --allow-hosts=localhost,127.0.0.1,::1

Each test uses a UUID-suffixed topic so concurrent runs and leftover data
can't interfere. Consumer/producer groups are unique per module run.

The client is pure-Python (gRPC + protobuf) — no native ``librocketmq`` is
needed (the old ctypes wrapper is gone). The suite skips before any import
when ``SCRAPY_TEST_ROCKETMQ_NAMESRV`` is unset.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from types import SimpleNamespace

import pytest

from scrapy_extension.exceptions import QueueError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("SCRAPY_TEST_ROCKETMQ_NAMESRV"),
        reason=(
            "Set SCRAPY_TEST_ROCKETMQ_NAMESRV to the gRPC proxy endpoint "
            "(e.g. localhost:8081) to run RocketMQ integration tests."
        ),
    ),
]


def _drain(backend, queue: str, n: int, deadline_s: float = 15.0):  # type: ignore[no-untyped-def]
    """Poll until ``n`` records consumed or deadline.

    Initiative #4 (at-least-once): the apache ``SimpleConsumer`` uses a
    deferred-ack model — ``pop_with_ack`` returns ``(body, token)`` WITHOUT
    acking; the caller acks via ``ack(token=msg)`` after receiving. This loop
    acks each message as it arrives so the broker doesn't redeliver within the
    invisible-duration window. The loop also absorbs subscription-propagation
    latency: the first receive(s) after ``subscribe`` can return empty until
    the subscription takes effect.
    """
    received: list[bytes] = []
    npe_hits = 0
    deadline = time.time() + deadline_s
    while len(received) < n and time.time() < deadline:
        try:
            body, token = _pop_with_ack_preserving_proxy_error(
                backend,
                queue,
                timeout=1.0,
            )
        except QueueError as exc:
            # apache rocketmq 5.x proxy has two broker-side propagation races:
            # NPE in ReceiveMessageActivity (delivery race), and "no topic to
            # receive message" (route-cache lag after topic creation). A pump
            # failure is sticky for its consumer generation, so migrate to a new
            # generation before the deadline-bounded loop retries. Other errors
            # propagate. Track NPE count for the final failure diagnostic.
            transient_state = _reconnect_after_proxy_receive_transient(backend, exc)
            if transient_state is None:
                raise
            if transient_state == "npe":
                npe_hits += 1
            continue
        if body is not None:
            backend.ack(queue, token=token)
            received.append(body)
    return received, npe_hits


# Container/nameserver constants for the docker-compose fixture
# (tests/integration/docker-compose.yml). Hardcoded because container_name is
# pinned in the compose file; override via env for non-standard setups.
_BROKER_CONTAINER = os.environ.get(
    "SCRAPY_TEST_ROCKETMQ_BROKER_CONTAINER", "scrapy-ext-rocketmq-broker"
)
_BROKER_ADDR = os.environ.get(
    "SCRAPY_TEST_ROCKETMQ_BROKER_ADDR", "scrapy-ext-rocketmq-broker:10911"
)
_NAMESRV_ADDR = os.environ.get(
    "SCRAPY_TEST_ROCKETMQ_INTERNAL_NAMESRV", "rocketmq-namesrv:9876"
)
_ROCKETMQ_SDK_LOCAL_IP_CACHE_ATTRIBUTE = "_Misc__LOCAL_IP"
_ROCKETMQ_SDK_LOOPBACK_ADDRESS = "127.0.0.1"
_ROCKETMQ_PROXY_NPE_SIGNATURE = "NullPointerException"
_ROCKETMQ_PROXY_NPE_CODE = "50001"
_ROCKETMQ_PROXY_RECEIVE_ACTIVITY_SIGNATURE = "ReceiveMessageActivity.receiveMessage"
_ROCKETMQ_PROXY_NO_TOPIC_SIGNATURE = "no topic to receive message"


def _pop_with_ack_preserving_proxy_error(backend, queue_name: str, timeout: float):  # type: ignore[no-untyped-def]
    """Use the pump's private live-test observer to classify Proxy startup races.

    Production never retains a driver's exception graph across the pump thread.
    This enabled-only suite observes it synchronously, then attaches it solely to
    the test-local sanitized error for the existing classifier.
    """
    driver_errors: list[BaseException] = []
    backend._receive_error_observer = driver_errors.append
    try:
        return backend.pop_with_ack(queue_name, timeout)
    except QueueError as error:
        if driver_errors:
            error.__cause__ = driver_errors[-1]
        raise
    finally:
        backend._receive_error_observer = None


def _proxy_receive_transient_state(error: QueueError) -> str | None:
    """Classify only the two known Apache Proxy receive-startup races."""
    cause = error.__cause__
    if cause is None:
        return None
    detail = str(cause)
    if (
        _ROCKETMQ_PROXY_NPE_CODE in detail
        and _ROCKETMQ_PROXY_NPE_SIGNATURE in detail
        and _ROCKETMQ_PROXY_RECEIVE_ACTIVITY_SIGNATURE in detail
    ):
        return "npe"
    if _ROCKETMQ_PROXY_NO_TOPIC_SIGNATURE in detail.lower():
        return "no-topic"
    return None


def _reconnect_after_proxy_receive_transient(backend, error: QueueError) -> str | None:  # type: ignore[no-untyped-def]
    """Migrate away from a sticky failed pump after a recognized Proxy race."""
    transient_state = _proxy_receive_transient_state(error)
    if transient_state is None:
        return None
    backend.disconnect()
    backend.connect()
    return transient_state


def _ensure_topic(backend, queue_name: str) -> None:  # type: ignore[no-untyped-def]
    """Pre-create the topic for ``queue_name`` via mqadmin.

    WHY: the apache 5.x gRPC proxy in LocalMode (``--enable-proxy``) does NOT
    honor ``broker.conf``'s ``autoCreateTopicEnable`` for the gRPC
    ``QueryRoute`` path — a fresh topic fails with "failed to fetch topic route"
    even though the broker would auto-create it for a remoting client. The
    proxy-level ``enableAutoTopicCreation`` config field is version-fragile
    across 5.x. Explicit pre-creation via mqadmin is the CI-stable path
    (validated against apache/rocketmq:5.3.1). Topic = ``{topic_prefix}_{queue}``.

    Skips the test (rather than failing) if mqadmin or docker is unavailable —
    the env-var gate already skips the suite when the broker isn't up.
    """
    topic = f"{backend.config.topic_prefix}_{queue_name}"
    result = subprocess.run(  # noqa: S603,S607 - trusted local fixture container
        [
            "docker",
            "exec",
            _BROKER_CONTAINER,
            "sh",
            "-c",
            # $ROCKETMQ_HOME is version-independent (e.g. rocketmq-5.3.1 OR 5.3.3).
            "cd $ROCKETMQ_HOME/bin && ./mqadmin updateTopic "
            f"-n {_NAMESRV_ADDR} -b {_BROKER_ADDR} -t {topic}",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        pytest.skip(
            f"could not pre-create topic {topic!r} via mqadmin (rc={result.returncode}). "
            f"stderr: {result.stderr[:200]}"
        )


@pytest.fixture(scope="module")
def _rocketmq_sdk_loopback_identity():  # type: ignore[no-untyped-def]
    """Prevent the SDK metadata path from probing a public route in CI.

    ``rocketmq-python-client==5.1.1`` discovers a local source IP by connecting
    an UDP socket to ``8.8.8.8``.  That address is neither a broker endpoint nor
    message traffic, and expanding pytest-socket's allow-list would permit any
    protocol/port to that public host.  The local fixture's real broker is
    loopback, which is also the SDK's documented fallback after probe failure;
    seed its private cache only for this integration module, then restore the
    caller's prior process-wide value after backend teardown.
    """
    from rocketmq.v5.util.misc import Misc

    previous = getattr(Misc, _ROCKETMQ_SDK_LOCAL_IP_CACHE_ATTRIBUTE)
    setattr(
        Misc,
        _ROCKETMQ_SDK_LOCAL_IP_CACHE_ATTRIBUTE,
        _ROCKETMQ_SDK_LOOPBACK_ADDRESS,
    )
    try:
        yield Misc
    finally:
        setattr(Misc, _ROCKETMQ_SDK_LOCAL_IP_CACHE_ATTRIBUTE, previous)


@pytest.fixture
def rocketmq_backend(_rocketmq_sdk_loopback_identity):  # type: ignore[no-untyped-def]
    """Connect one generation per test so each may select its own logical topic.

    Unique consumer/producer groups avoid cross-talk with any real
    ``scrapy-extension-*`` groups or prior runs.
    """
    from scrapy_extension.backends.rocketmq import RocketMQBackend
    from scrapy_extension.settings.rocketmq import RocketMQSettings

    suffix = uuid.uuid4().hex[:8]
    config = RocketMQSettings(
        namesrv_address=os.environ["SCRAPY_TEST_ROCKETMQ_NAMESRV"],
        consumer_group=f"inttest-cg-{suffix}",
    )
    backend = RocketMQBackend(config)
    backend.connect()  # R7: starts producer AND consumer
    yield backend
    backend.disconnect()


@pytest.fixture
def unique_prefix() -> str:
    """UUID-suffixed namespace → unique topic per test.

    Hyphen-delimited (NOT colon) because RocketMQ topic names reject colons:
    the topic is ``scrapy-queue_{queue_name}``.
    """
    return f"inttest-{uuid.uuid4().hex}"


def test_sdk_loopback_identity_avoids_public_route_probe(
    _rocketmq_sdk_loopback_identity, mocker
):  # type: ignore[no-untyped-def]
    """The fixture must use the SDK cache without constructing a probe socket."""
    probe_socket = mocker.Mock(
        side_effect=AssertionError("RocketMQ metadata attempted a public route probe")
    )
    mocker.patch(
        "rocketmq.v5.util.misc.socket",
        SimpleNamespace(
            AF_INET=socket.AF_INET,
            SOCK_DGRAM=socket.SOCK_DGRAM,
            socket=probe_socket,
        ),
    )

    assert (
        _rocketmq_sdk_loopback_identity.get_local_ip() == _ROCKETMQ_SDK_LOOPBACK_ADDRESS
    )
    probe_socket.assert_not_called()


def test_proxy_receive_classifier_retries_only_known_startup_npe() -> None:
    """An unrelated SDK/Proxy NPE must not turn the live suite falsely green."""
    known_error = QueueError("internal receive error")
    known_error.__cause__ = RuntimeError(
        "50001, null. NullPointerException. "
        "org.apache.rocketmq.proxy.grpc.v2.consumer."
        "ReceiveMessageActivity.receiveMessage(ReceiveMessageActivity.java:63)"
    )
    unrelated_error = QueueError("internal receive error")
    unrelated_error.__cause__ = RuntimeError("NullPointerException in unrelated RPC")

    assert _proxy_receive_transient_state(known_error) == "npe"
    assert _proxy_receive_transient_state(unrelated_error) is None


def test_proxy_transient_reconnects_before_retry() -> None:
    """A classified pump failure migrates generations before the next pop."""
    events: list[str] = []
    token = object()

    class _ScriptedBackend:
        _receive_error_observer = None
        pop_calls = 0

        def pop_with_ack(self, _queue: str, _timeout: float):  # type: ignore[no-untyped-def]
            events.append("pop")
            self.pop_calls += 1
            if self.pop_calls == 1:
                observer = self._receive_error_observer
                assert observer is not None
                observer(
                    RuntimeError(
                        "50001, null. NullPointerException. "
                        "org.apache.rocketmq.proxy.grpc.v2.consumer."
                        "ReceiveMessageActivity.receiveMessage(ReceiveMessageActivity.java:63)"
                    )
                )
                raise QueueError("RocketMQ receive pump failed.")
            return b"recovered", token

        def disconnect(self) -> None:
            events.append("disconnect")

        def connect(self) -> None:
            events.append("connect")

        def ack(self, _queue: str, *, token: object) -> None:
            events.append("ack")

    received, npe_hits = _drain(_ScriptedBackend(), "jobs", 1, deadline_s=1)

    assert received == [b"recovered"]
    assert npe_hits == 1
    assert events == ["pop", "disconnect", "connect", "pop", "ack"]


def test_push_pop_round_trip(rocketmq_backend, unique_prefix):
    """R7 verification: N in → N out, no loss.

    Pre-R7 this returned 0 — ``connect()`` never started the consumer and
    ``pop()`` never subscribed, so receive() always came back empty. Only a
    real broker round-trip proves the subscribe+start fix actually works.
    Deferred-ack: ``_drain`` acks each received message (initiative #4).
    """
    queue = f"{unique_prefix}-rt"
    _ensure_topic(rocketmq_backend, queue)
    n = 5
    sent = [f"item-{i:03d}".encode() for i in range(n)]
    for item in sent:
        rocketmq_backend.push(queue, item, priority=0.0)

    received, npe_hits = _drain(rocketmq_backend, queue, n)

    # The live contract is the complete multi-message round trip, not merely one
    # successful delivery. Compare the full multiset so loss, duplication, and an
    # unexpected body all fail while remaining independent of broker queue order.
    if not received:
        pytest.fail(
            f"broker delivered 0 of {n} pushed messages within the drain window "
            f"({npe_hits} receive NPE(s)); push succeeded (producer accepted all {n})."
        )
    assert sorted(received) == sorted(sent), (
        f"broker delivered {len(received)} of {n} expected messages "
        f"({npe_hits} receive NPE(s)): {received!r}"
    )


def test_pop_empty_returns_none(rocketmq_backend, unique_prefix):
    """pop on a topic with no messages returns None (receive times out)."""
    queue = f"{unique_prefix}-empty"
    _ensure_topic(rocketmq_backend, queue)
    # apache rocketmq 5.x proxy: cold receives right after topic creation hit two
    # transient broker-side races — (a) "There is no topic to receive message"
    # (route-cache lag; this is what failed #15's CI on main: the tight 4x2s
    # retry budget was exceeded on the slower CI runner), (b) NPE in
    # ReceiveMessageActivity. Both are broker-controlled and resolve within
    # seconds. Use a deadline-bounded poll (mirrors ``_drain``'s CI-proven
    # pattern) that tolerates both transients and returns on the first clean
    # None — the honest contract is "pop returns None once the route propagates",
    # not "pop returns None within a fixed retry count".
    deadline = time.time() + 30.0
    while time.time() < deadline:
        try:
            body, token = _pop_with_ack_preserving_proxy_error(
                rocketmq_backend,
                queue,
                timeout=1.0,
            )
            if body is None:
                return  # route propagated + queue empty → success
            if token is not None:
                rocketmq_backend.ack(queue, token=token)
        except QueueError as exc:
            if _reconnect_after_proxy_receive_transient(rocketmq_backend, exc) is None:
                raise  # non-transient error → surface, don't mask
        # transient race (migrated generation) OR a stray message → keep polling
    pytest.fail(
        "pop on empty topic did not return None within 30s "
        "(apache proxy route-cache lag did not resolve)"
    )


def test_push_pop_round_trip_fails_when_no_messages_are_delivered(mocker) -> None:
    """An enabled integration round-trip must fail on zero delivery."""
    backend = SimpleNamespace(
        config=SimpleNamespace(topic_prefix="test"),
        push=mocker.Mock(),
    )
    mocker.patch.object(pytest, "skip")
    mocker.patch.dict(
        test_push_pop_round_trip.__globals__,
        {"_ensure_topic": mocker.Mock(), "_drain": mocker.Mock(return_value=([], 0))},
    )

    with pytest.raises(pytest.fail.Exception, match="broker delivered 0"):
        test_push_pop_round_trip(backend, "test-queue")


def test_queue_len_reports_unsupported(rocketmq_backend, unique_prefix):
    """Unknown RocketMQ depth must not masquerade as an empty queue."""
    with pytest.raises(NotImplementedError, match="broker-side depth RPC"):
        rocketmq_backend.queue_len(f"{unique_prefix}-qlen")
