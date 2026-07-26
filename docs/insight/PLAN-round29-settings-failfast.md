# Round 29 — PLAN: kafka runtime strip + mongodb/rocketmq name fail-fast

> Spec: [SPEC-round29-settings-failfast.md](./SPEC-round29-settings-failfast.md).
> TDD (RED → GREEN). Claude-Code-only.

## Commit 1 — R28-C-1 kafka runtime `.strip()` (MED, headline, self-caught)

`backends/kafka.py:496` `_bootstrap_servers` CONFLUENT arm uses bare `or`:
```python
return self.config.confluent_bootstrap_servers or self.config.bootstrap_servers
```
Realign with the R28-C validator's `.strip()` semantics:
```python
return (self.config.confluent_bootstrap_servers or "").strip() or self.config.bootstrap_servers
```
### RED
- `test_bootstrap_servers_strips_whitespace_confluent` — CONFLUENT + `confluent_bootstrap_servers="   "` + real `bootstrap_servers` → `_bootstrap_servers()` returns the real bootstrap, not the whitespace. (Currently returns `"   "`.)

## Commit 2 — R29-A/B/C/D mongodb name fields reject empty/whitespace (MED cluster)

`settings/mongodb.py`:
- **A (line 39):** in `validate_mongodb_collection_domains`, after the type check, add `if not all(name and name.strip() for name in validated_names): raise ConfigurationError("...must be non-empty.", setting_name="collection_names")`. Covers settings + backend (shared helper).
- **B (line 202):** add `@field_validator("replica_set_members", "mongos_routers")` rejecting empty/whitespace elements: `if any((not e) or (not e.strip()) for e in v): raise ConfigurationError(...)`.
- **C (line 178):** add `@field_validator("database")` rejecting empty/whitespace.
- **D (line 448):** strip before truthiness: `name_set = bool(self.replica_set_name) and bool(self.replica_set_name.strip())`; `if not name_set and not uri_has_rs: raise ...`.

### RED (one per field)
- `test_collection_names_empty_rejected` (A) — `MongoDBSettings(queue_collection="")` raises.
- `test_replica_set_members_empty_element_rejected` (B) — `replica_set_members=["host:27017", ""]` raises.
- `test_database_empty_rejected` (C) — `database=""` raises.
- `test_replica_set_name_whitespace_rejected` (D) — `mode=REPLICA_SET, replica_set_name="   "` raises.

## Commit 3 — R27-RMQ-1/2 rocketmq config-edge guards (LOW)

`settings/rocketmq.py`:
- **RMQ-1 (line 147):** `max_message_size: int = Field(default=1024 * 1024, ge=0)` → `gt=0`.
- **RMQ-2 (line 143):** `consumer_group: str = Field(default="scrapy-extension-consumer", min_length=1)` (+ field_validator rejecting whitespace, since `min_length=1` still admits `"  "`).

### RED
- `test_max_message_size_zero_rejected` (RMQ-1) — `max_message_size=0` raises ValidationError.
- `test_consumer_group_empty_rejected` (RMQ-2) — `consumer_group=""` raises; `consumer_group="   "` raises.

## Gates

`uv run ruff check` → `uv run mypy --strict src/scrapy_extension` → `uv run pytest`
(R28 baseline 3811 + ~7 new; use `UV_CACHE_DIR=$TMPDIR/uv-cache` + sandbox off).

## Reviewer

Claude-Code-only. Inline review or `general-purpose`+opus (NOT `agent-skills:code-reviewer`
— GLM). R28-C-1 is the headline (self-caught runtime/validator mismatch); verify the
`.strip()` change doesn't break the CLUSTER arm or the real-confluent path.
