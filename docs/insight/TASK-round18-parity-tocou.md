# TASK — Round 18: connect()-BaseException final parity + R17-B TOCTOU + docs drift

> Spec: [SPEC-round18-parity-tocou.md](SPEC-round18-parity-tocou.md)
> Plan:  [PLAN-round18-parity-tocou.md](PLAN-round18-parity-tocou.md)
> Constraints: one atomic commit per unit · all → main · Claude-only.

## Unit A — runbook phantom stat key (MED, docs)
**File:** `docs/runbook.md` (:602 table row, :630 ack-error prose). Ref `monitor/stats.py:165,170`.
1. `:602` — replace `dupefilter/filtered` → `dupefilter/hit_count`; update the description
   ("Count of duplicates filtered" stays accurate); optionally add a `dupefilter/miss_count`
   row for newly-seen requests if the table benefits.
2. `:630` — replace `dupefilter/filtered` → `dupefilter/hit_count` in the ack-error
   redelivery-side-effect prose.
3. Verify `grep -rn 'dupefilter/filtered' docs/ src/` returns nothing after.
4. Commit `docs(runbook): cite the real dupefilter/hit_count stat, not the phantom dupefilter/filtered`.

## Unit B — pulsar connect() BaseException arm (LOW, leak)
**File:** `src/scrapy_extension/backends/pulsar.py:403-444`; test `tests/test_pulsar_backend.py` (or sibling).
1. [RED] Test: mock `pulsar.Client` to return a tracked mock; raise `KeyboardInterrupt` after
   construction (before `self._client = client`). Assert the mock client's `close()` called.
   Run → RED.
2. [GREEN] Hoist `client: Any = None` before `client = pulsar.Client(...)` (line 430). Add
   `except BaseException:` arm (after the `except Exception` at 441):
   ```python
   except BaseException:
     # R18-B: Ctrl+C/SystemExit after pulsar.Client(...) (C++ bg threads) but before
     # publish must close the un-published client. Identity guard — 'self._client is
     # client' means it WAS published and disconnect() owns it. Resource leak, not wedge.
     # Mirror the R16-A/R17 connect() BaseException contract.
     if client is not None and self._client is not client:
       with _suppress_pulsar_errors():
         client.close()
     raise
   ```
   Leave the `from None` redaction at :444 untouched.
3. Run gate; commit `fix(pulsar): close un-published client on BaseException in connect() (R18-B, last-backend parity)`.

## Unit C — rabbitmq R17-B published-flag TOCTOU (LOW, R17 regression)
**File:** `src/scrapy_extension/backends/rabbitmq.py:540-551`; test `tests/test_rabbitmq_generation.py`.
1. [RED] Test: patch `_publish_handles_locked` to call the REAL publish (so `self._connection`
   becomes `candidate.connection`) THEN raise `KeyboardInterrupt`. `pytest.raises(KeyboardInterrupt)`;
   assert `candidate_connection.close.assert_not_called()` + `candidate_channel.close.assert_not_called()`
   + `backend._connection is candidate.connection`. Run → RED (current arm closes the live candidate).
2. [GREEN] Replace the guard at line 549:
   ```python
   if candidate is not None and self._connection is not candidate.connection:
     self._close_handles(candidate.channel, candidate.connection)
   ```
   Update the comment: the identity guard reads actual post-publish state (a published candidate
   has `self._connection is candidate.connection`), superseding the `published` flag which lagged
   the side-effect (R18-C TOCTOU fix). Keep `published` for the normal-flow `if not published:` at 529.
3. Verify the existing `test_connect_closes_candidate_on_baseexception_in_publish_window`
   (unpublished case) still GREEN.
4. Run gate; commit `fix(rabbitmq): identity-guard the publish-window BaseException arm so it cannot close a just-published live session (R18-C, R17-B TOCTOU regression)`.

## Unit D — CHANGELOG Kafka clear_queue (LOW, docs)
**File:** `.github/CHANGELOG.md:115`. Ref `kafka.py:1377`, README:527, migration-guide:449.
1. Update the Kafka `clear_queue` Breaking bullet to state it raises `QueueError` (parity with
   pulsar/rocketmq clear_queue + the pulsar/rocketmq `queue_len` NotImplementedError bullet).
2. Commit `docs(changelog): name QueueError for Kafka clear_queue (parity with README/migration-guide)`.

## Definition of done
- [ ] ruff clean · mypy --strict 0 issues · pytest ≥3763 passed (unsandboxed) · coverage ≥95%
- [ ] 4 atomic commits on `worktree-round18-parity-tocou`
- [ ] ff-merged to `main`, pushed, worktree branch deleted
- [ ] memory updated (R18 close-out note in `deep-insight-2026-07-23-ultracode.md`)
