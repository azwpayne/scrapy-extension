# Fix Code Review Issues

**Goal:** Fix 6 issues identified in code review: 3 CRITICAL, 2 HIGH, 1 doc improvement.
**Test:** `uv run --group test pytest` must pass after all fixes.

---

## Fix 1: Remove TOCTOU race in dupefilter [CRITICAL]

**File:** `src/scrapy_extension/components/dupefilter.py`
**Lines:** 126-131

**Problem:** `request_seen()` calls `contains()` then `add()` — not atomic. Another process can insert between the two calls, causing duplicate processing.

**Change:**

```python
# REMOVE these lines (126-128):
    # Prefer atomic add if supported; fallback to contains-check semantics.
    if set_backend.contains(self.key, fingerprint.encode()):
      return True

# KEEP these lines (130-131) — they are already correct:
    added = set_backend.add(self.key, fingerprint.encode())
    return not added
```

**Why it's safe:** `SetBackend.add()` returns `False` when the item already exists (see `RedisBackend.add`, `MongoDBBackend.add`). The `contains` check was redundant and introduced a race.

---

## Fix 2: Fix Kafka `ping()` always returning False [CRITICAL]

**File:** `src/scrapy_extension/backends/kafka_backend.py`
**Lines:** 255-268

**Problem:** The `else` clause of `try` runs when no exception occurs, overwriting the `return True`.

**Before:**
```python
  def ping(self) -> bool:
    try:
      if self._admin_client:
        self._admin_client.list_topics()
        return True
    except KafkaError:
      return False
    else:
      return False
```

**After:**
```python
  def ping(self) -> bool:
    try:
      if self._admin_client:
        self._admin_client.list_topics()
        return True
      return False
    except KafkaError:
      return False
```

---

## Fix 3: Document `peek()` non-atomic limitation [LOW]

**File:** `src/scrapy_extension/components/queue.py`
**Lines:** 132-144

**Before:**
```python
  def peek(self) -> Request | None:
    """Peek at the next request without removing it.

    Returns:
        The next request, or None if the queue is empty.
    """
    # For Redis sorted sets, we can't truly peek without popping
    # So we pop and push back (not atomic, but best effort)
```

**After:**
```python
  def peek(self) -> Request | None:
    """Peek at the next request without removing it.

    Warning:
        This operation is NOT atomic. Between pop and push, another
        consumer may take the item. Use only for monitoring/debugging,
        never for request processing in concurrent environments.

    Returns:
        The next request, or None if the queue is empty.
    """
    # Non-atomic: pop then push back. NOT safe for concurrent consumers.
    request = self.pop(timeout=0)
    if request:
      self.push(request, priority=request.priority)
    return request
```

---

## Fix 4: Replace `assert` with proper error handling in MongoDB backend [HIGH]

**File:** `src/scrapy_extension/backends/mongodb_backend.py`

**Problem:** `assert` statements are stripped by `python -O`. MongoDB backend uses them for connection validation throughout. Replace with explicit `BackendConnectionError` raises.

**Step 4a:** Fix `_assert_connected()` method (lines 349-353)

**Before:**
```python
  def _assert_connected(self) -> None:
    """Assert that all collections are initialized."""
    assert self._queue_collection is not None, "Not connected: call connect() first"
    assert self._set_collection is not None, "Not connected: call connect() first"
    assert self._storage_collection is not None, "Not connected: call connect() first"
```

**After:**
```python
  def _assert_connected(self) -> None:
    """Verify all collections are initialized.

    Raises:
        BackendConnectionError: If not connected.
    """
    if (
      self._queue_collection is None
      or self._set_collection is None
      or self._storage_collection is None
    ):
      msg = "Not connected: call connect() first"
      raise BackendConnectionError(msg, backend_type="mongodb")
```

**Step 4b:** Replace all inline `assert` calls in methods below with `self._assert_connected()`.

Locations to update (replace each `assert self._XXX_collection is not None` with `self._assert_connected()`):

| Line | Method | Statement to replace |
|------|--------|---------------------|
| 364 | `push` | `assert self._queue_collection is not None` |
| 383 | `pop` | `assert self._queue_collection is not None` |
| 402 | `queue_len` | `assert self._queue_collection is not None` |
| 411 | `clear_queue` | `assert self._queue_collection is not None` |
| 436 | `add` | `assert self._set_collection is not None` |
| 460 | `remove` | `assert self._set_collection is not None` |
| 479 | `contains` | `assert self._set_collection is not None` |
| 497 | `set_len` | `assert self._set_collection is not None` |
| 506 | `clear_set` | `assert self._set_collection is not None` |
| 518 | `store` | `assert self._storage_collection is not None` |
| 541 | `retrieve` | `assert self._storage_collection is not None` |
| 556 | `delete` | `assert self._storage_collection is not None` |
| 569 | `exists` | `assert self._storage_collection is not None` |
| 582 | `ttl` | `assert self._storage_collection is not None` |
| 600 | `clear_storage` | `assert self._storage_collection is not None` |

Each replacement is a single line swap:
```python
# BEFORE:
    assert self._queue_collection is not None

# AFTER:
    self._assert_connected()
```

---

## Fix 5: Add warnings to RabbitMQ default credentials [HIGH]

**File:** `src/scrapy_extension/settings/rabbitmq.py`

**Problem:** Default `guest:guest` credentials are a security risk if not overridden.

**Find and update these two fields:**

**Before:**
```python
  username: str = Field(
    default="guest",
    description="RabbitMQ authentication username",
  )
  password: str = Field(
    default="guest",
    description="RabbitMQ authentication password",
  )
```

**After:**
```python
  username: str = Field(
    default="guest",
    description="RabbitMQ username. MUST override in production via SCRAPY_RABBITMQ_USERNAME.",
  )
  password: str = Field(
    default="guest",
    description="RabbitMQ password. MUST override in production via SCRAPY_RABBITMQ_PASSWORD.",
  )
```

---

## Fix 6: Deduplicate exception handlers in Redis backend [LOW]

**File:** `src/scrapy_extension/backends/redis_backend.py`
**Lines:** 94-105

**Problem:** `RedisError` handler is redundant — it's a subclass of `Exception` and the second handler does the same thing.

**Before:**
```python
    except RedisError as e:
      msg = f"Failed to connect to Redis ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="redis",
      ) from e
    except Exception as e:
      msg = f"Failed to connect to Redis ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="redis",
      ) from e
```

**After:**
```python
    except Exception as e:
      msg = f"Failed to connect to Redis ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="redis",
      ) from e
```

---

## Verification

After all fixes:

```bash
cd /Users/payne/WorkSpace/Development/web-crawler/scrapy-extension
uv run --group test pytest
```

All 124 tests must pass. No new tests needed — these are internal bug fixes, not behavior changes (except Fix 2 which makes `ping()` actually work).
