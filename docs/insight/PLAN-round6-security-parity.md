# PLAN — Round 6: Security-Parity Cluster (Tier-2)

The remaining Tier-2 security items (round-2 INSIGHTS Theme D). All are parity/defense-in-depth
(round-1 security fixes HOLD — no active vulnerability); graded MEDIUM/LOW by the round-2
security-reviewer. Bundled into ONE round as small file-disjoint fixes. Companion to
[`INSIGHTS-2026-06-25.md`](./INSIGHTS-2026-06-25.md) (Theme D) + [`PLAN-round4-backpressure.md`](./PLAN-round4-backpressure.md).

## Fix list (from round-2 INSIGHTS Theme D, file:line-anchored)

| ID | Sev | Fix | Anchor |
|---|---|---|---|
| SEC-1 | LOW | `_RedactedStr` parity — lift Kafka's `_RedactedStr` to a shared helper, apply to RabbitMQ/Mongo/ES/Pulsar/SQS/DynamoDB secret outputs handed to client libs (defense-in-depth: secrets not rendered in client-lib config repr/tracebacks). | `backends/kafka.py:48-60` (has it); `rabbitmq.py:198`, `mongodb.py:199`, `elasticsearch.py:74,84`, `pulsar.py`, `sqs.py`, `dynamodb.py` (need it) |
| SEC-2 | MEDIUM | Mongo `tls_allow_invalid_certificates=True` mode guard — `model_validator` warns/raises when set in ATLAS/SHARDED_CLUSTER/REPLICA_SET (mirror Redis `ssl_check_hostname` guidance). | `settings/mongodb.py:164-167` |
| SEC-3 | MEDIUM | ES cleartext-creds guard — validator raises `ConfigurationError` when any host is `http://` AND (`api_key` or `password`) is set (creds over cleartext). | `settings/elasticsearch.py:65,90-93` |
| SEC-4 | MEDIUM | SQS/DynamoDB `endpoint_url` scheme allowlist — `model_validator` requires `http://` or `https://` (reject typos / no-scheme); document `http://` is LocalStack-only. | `settings/sqs.py:44-47`, `settings/dynamodb.py:48` |
| SEC-5 | MEDIUM | Pulsar TLS decouple — `allow_insecure_connection` is gated behind `if tls_trust_certs_file:`; decouple so it's always passed (default False) for `pulsar+ssl://` URLs; only pass trust_certs when set. | `backends/pulsar.py:156-158` |
| SEC-6 | LOW-MEDIUM | Sentinel/Cluster malformed-entry wrap — `int(port_str)` raises raw `ValueError`; wrap in `BackendConnectionError`. (round-2 residual.) | `backends/redis.py:191,208-228,275` |
| SEC-7 | LOW | AWS half-cred XOR validator — `aws_access_key_id` set without `aws_secret_access_key` (or vice-versa) silently falls through to the boto3 default chain; XOR-validate (both-or-neither). | `backends/sqs.py:87-91`, `backends/dynamodb.py:79-83` |

## Units (parallel fan-out; file-disjoint by domain)

### Unit SEC-BE — backend `.py` files (one executor)
**Files**: NEW `src/scrapy_extension/backends/_redaction.py` (shared `_RedactedStr`); `backends/{kafka,rabbitmq,mongodb,elasticsearch,pulsar,sqs,dynamodb}.py` (redaction apply; kafka refactored to import shared); `backends/pulsar.py` (SEC-5 TLS decouple); `backends/{sqs,dynamodb}.py` (SEC-7 AWS half-cred XOR); `backends/redis.py` (SEC-6 Sentinel/Cluster wrap). Tests: `tests/test_kafka_backend.py`/`test_rabbitmq_backend.py`/etc. (redaction repr assertions), `tests/test_sqs_backend.py`/`test_dynamodb_backend.py` (half-cred), `tests/test_backends.py`/redis (Sentinel wrap).
- **SEC-1**: lift `_RedactedStr` → `backends/_redaction.py`; kafka imports it (remove local def); each of rabbitmq/mongo/es/pulsar/sqs/dynamodb wraps its `secret_value(password)`/`secret_value(access_key)` outputs handed to the client-lib config dict in `_RedactedStr`. Assertion: `repr(client_config["password"]) == "***"` (or the redaction marker).
- **SEC-5**: `backends/pulsar.py` connect — always pass `allow_insecure_connection=self.config.allow_insecure_connection` (default False) when `service_url` starts with `pulsar+ssl://`; only pass `tls_trust_certs_file` when set. Test: `pulsar+ssl://` + `allow_insecure_connection=False` + no trust_certs → both fields handled independently.
- **SEC-6**: `backends/redis.py` `_connect_sentinel`/`_connect_cluster` — wrap the `int(port_str)` parse + `master_for(...).ping()` in try/except → `BackendConnectionError(backend_type="redis")`. Test: malformed sentinels entry `"host:notaport"` → `BackendConnectionError` (not raw `ValueError`).
- **SEC-7**: `backends/{sqs,dynamodb}.py` connect — XOR-validate: if `aws_access_key_id` set XOR `aws_secret_access_key` set → raise `ConfigurationError` (both-or-neither; empty = use default chain). Test: key-without-secret → ConfigurationError; both-set → ok; neither → ok (default chain).

### Unit SEC-SET — settings `.py` files (one executor, parallel)
**Files**: `settings/mongodb.py` (SEC-2), `settings/elasticsearch.py` (SEC-3), `settings/sqs.py` + `settings/dynamodb.py` (SEC-4). Tests: `tests/test_config.py` (add validation tests per setting).
- **SEC-2**: `settings/mongodb.py` `model_validator(mode="after")` — if `tls_allow_invalid_certificates is True` and `mode in {ATLAS, SHARDED_CLUSTER, REPLICA_SET}` → raise `ConfigurationError` (standalone allowed for local dev). Mirror the existing RabbitMQ guest-guard pattern.
- **SEC-3**: `settings/elasticsearch.py` `model_validator` — if any host URL scheme is `http://` AND (`api_key` or `password`) is set → raise `ConfigurationError` (cleartext creds).
- **SEC-4**: `settings/sqs.py` + `settings/dynamodb.py` `model_validator` — `endpoint_url` (when set) must start with `http://` or `https://`; else `ConfigurationError`.

## TDD (RED first, then GREEN), per fix
Each fix gets a regression test that FAILS pre-fix (no validator / raw error / unredacted repr) and PASSES post-fix. Honest tests — no skipping/weakening. tests/ scoring-sensitive.

## Acceptance
- `uv run pytest -q -p no:randomly` green (existing 1306 + new); ruff clean; mypy clean (66+ files); bandit 0.
- Each fix RED→GREEN; backward-compat (existing valid configs still accepted; new guards only reject the insecure/malformed cases).
- Independent **security-reviewer** + code-reviewer approval lane: APPROVE, 0 CRITICAL/HIGH. (security-reviewer leads this round — it's a security cluster.)

## Non-goals (remain)
- Distributed Delay/Throttle. Sentinel failover re-discovery. rocketmq-client replacement.
- B5 reconnect in-flight-survival test. SQS/Pulsar real integration tests.
