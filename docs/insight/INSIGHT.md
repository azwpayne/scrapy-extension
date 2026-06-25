# Deep Insight — scrapy-extension (2026-06-24)

Structured deep-dive by 5 parallel Claude-Code agents (architect/opus, code-reviewer/opus,
security-reviewer/sonnet, test-engineer/sonnet, explore/haiku) + orchestrator fact-check.

> **Method note:** the explore (haiku) agent's "missing settings files / mode-matrix" claims
> were **falsified by fact-check** — all 11 settings files exist
> (`ls src/scrapy_extension/settings/` → base + 10 backends). Its stub + `monitor/` findings were
> corroborated and retained. *Wrong conclusion discarded, corroborated evidence kept.*

---

## Theme A — Distributed-correctness debt (CRITICAL / HIGH)

| Sev | Issue | Location | Note |
|---|---|---|---|
| **CRITICAL** | Shared `ConnectionManager.close()` has **no refcounting** — colocated components (queue + dedup on the same Redis) tear the connection down out from under each other during shutdown ordering. | `backends/connectors.py:374` (close), `:236` (get_manager), `:255` (registry set) | grep confirms no `_users`/refcount exists |
| **HIGH** | Ack/nack **single-slot** (`_last_record` / `_last_delivery_tag`) — under default `CONCURRENT_REQUESTS=16` only the last popped msg is ackable → **silent at-least-once violation** (Kafka/RabbitMQ). Code only **warns**, does not enforce. | `backends/kafka.py:112,456-462,479-487`; `backends/rabbitmq.py:74,459-465,483-486`; `schedule/scheduler.py:56-66` | warn at `kafka.py:459`, `rabbitmq.py:462` |
| **HIGH** | Redis `_POP_LUA` escalates a **benign concurrent-consumer payload race** to a hard `QueueError` (treats a lost `HGET` as corruption). | `backends/redis.py:49-57,474-489` | integer `-1` sentinel |
| MEDIUM | ES queue pop = **search-then-delete** (non-atomic) → duplicate delivery under competing consumers. | `backends/elasticsearch.py:196-224` | ✅ **RESOLVED** (round-2 verified: atomic via `if_seq_no`/`if_primary_term` + conflict retry, `elasticsearch.py:195-246`; test `test_pop_retries_on_conflict`) |
| MEDIUM | `DelayQueueStrategy` holds items **in-process** (`heapq`); loses them on worker restart — distributed-correctness hole. | `queue/strategies/delay.py:38-39,69,151-168` | self-documented as v1 debt |

## Theme B — Connection lifecycle & concurrency

| Sev | Issue | Location |
|---|---|---|
| **HIGH** | `resolve_backend_config` **crashes on a programmatic `BackendType` enum** (non-string) inside `BackendType(...)`. | `backends/connectors.py:178-186` |
| MEDIUM | `connect()` retry `time.sleep` runs **inside the held `_lock`** → stalls every thread sharing the manager for the full backoff window. | `backends/connectors.py:310-346,438-441` |
| MEDIUM | **Legacy (pre-base64) queued bodies** → `binascii.Error` → silent drop on rolling upgrade (no migration path). | `queue/queue.py:153-163` |
| — | Zero concurrency tests on the registry; reconnect-after-close untested. | `tests/test_connectors.py`, `tests/test_connection_manager.py` |

## Theme C — Security defaults (verified)

| Sev | Issue | Location | Verified |
|---|---|---|---|
| **HIGH** | Redis `ssl_check_hostname=False` default → **MITM** when TLS enabled. | `settings/redis.py:195` | field confirmed |
| **HIGH** | RabbitMQ **hardcoded `guest/guest`** defaults → silent fallback auth. | `settings/rabbitmq.py:64-70` | `default="guest"` + `SecretStr("guest")` confirmed |
| MEDIUM | **No boundary size validation** on crawled content → DoS via oversize payloads (silent drop on backend cap). | `queue/queue.py:110-135`, `pipeline/pipeline.py:143-175` | |
| LOW | Confluent `api_key`/`api_secret` **not wrapped in `_RedactedStr`** (the SASL path is) → secret in `repr`/tracebacks. | `backends/kafka.py:218-219` | |
| LOW | `kafka-python>=2.0.2` unmaintained since 2021 (no CVE backports). | `pyproject.toml:43,54,87` | confirmed |
| LOW | RabbitMQ `ssl_verify_mode` free-form str (typo → `CERT_NONE`). | `settings/rabbitmq.py:118-120` | |

**Clean:** no deserialization vulns (JSON + base64 + Scrapy `request_from_dict`; no pickle/eval/marshal).
Redis Lua parameterized correctly via `KEYS`/`ARGV`. ElasticSearch uses structured DSL (`term` filters).

## Theme D — Observability gap
`monitor/__init__.py` is an **empty placeholder** (15 LOC, `TYPE_CHECKING` only). No backpressure /
queue-depth / dedup-hit signal; stats hand-rolled ad hoc (`scheduler.py:298-327`, `pipeline.py:159,172`).
The biggest operability gap for a distributed crawler lib. (architect bet #1)

## Theme E — Test infrastructure
- **Integration CI job is commented out** — `.github/workflows/ci.yml:47` (`# integration-tests:`); 27
  integration tests are dead; unit tests run `-m "not integration"` (`:35`). The 98.10% figure is
  **unit/mock-only**.
- **Zero property/fuzz tests** (no `hypothesis`). Cuckoo has no FP-rate assertion (bloom does).
  Redis SENTINEL/CLUSTER failover is only constructor-mocked. Multi-backend coexistence is only
  config-resolution-tested, never stood up for real.

## Theme F — Extensibility / consistency
- `StorageBackend` has **no strategy layer** (dedup + queue both do) — `pipeline.py:222-229` calls
  `store` directly. Visible asymmetry. (architect bet #4, S effort)
- **4 hand-synced backend registries** (`connectors.py:62,32-53` + `__init__.py` + doc matrix) — no
  entry-point / plugin registration; adding backend #11 risks missing one. (architect bet #2, M)
- RocketMQ Set/Storage stubs `raise NotImplementedError` but are **dead code** (capability gating
  already excludes them at the connector layer). (explore + code-reviewer)
