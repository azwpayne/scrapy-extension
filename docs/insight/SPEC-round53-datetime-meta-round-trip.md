# Round 53 — SPEC / PLAN / TASK: datetime/date request.meta round-trip

## Context and audit evidence

`JSONSerializer` (src/scrapy_extension/backends/base.py) is the wire codec for
every queued Scrapy request. Its `_json_default` docstring (line 52) claims:

> `datetime` / `date` → ISO 8601 string (round-trips via `datetime.fromisoformat`)

This is **false**. The encode side converts `datetime`/`date` to a *plain* ISO
string with **no codec marker** (line 74-75), unlike `bytes` which gets a
tagged `{__scrapy_extension_json_type__: "bytes", data: ...}` marker. The decode
side `_decode_json_value` (line 173-212) reconstructs only `_CODEC_DICT`,
`_CODEC_BYTES`, and the legacy `__b64__` marker — there is **no `datetime`
branch**, so a `datetime` in `request.meta` becomes a `str` permanently after
one queue round-trip and the documented round-trip never happens.

Empirically verified:

```python
JSONSerializer().deserialize(JSONSerializer().serialize({"k": datetime(2026,8,6,12,0)}))
# -> {"k": "2026-08-06T12:00:00"}   (str, not datetime)
```

The `bytes` asymmetry was given the tagged-marker treatment and fixed on
2026-07-09 (DEEP-INSIGHT-2026-07-09-parallel-verified.md F1); the rich-type
`datetime`/`date` path was not. `datetime` is the single most common rich type
real spiders put in `meta` (retry scheduling, scraped-at timestamps, rate-limit
windows). The existing `test_datetime_serializes_to_isoformat`
(test_backends.py:91-100, "R17-followup") pins the one-way `str` form using the
same stopgap language `bytes` had at R17 before F1 promoted it to symmetric —
so it encodes a stopgap, not a permanent contract.

## Goal

A `datetime` or `date` stored in `request.meta` (or `cb_kwargs`, or `cookies`)
must survive a push → persist → pop round-trip as the **same type**, so a
downstream middleware reading `request.meta[key]` after a queue cycle gets back
exactly what was pushed — no manual `datetime.fromisoformat` workaround, no
silent `TypeError`.

## Specification

- Mirror the existing `_CODEC_BYTES` tagged-marker pattern exactly: add
  `_CODEC_DATETIME` and `_CODEC_DATE` tags; encode produces a tagged marker;
  decode reconstructs via `fromisoformat`; `_looks_like_codec_marker` recognizes
  the new tags so caller-owned dicts shaped like the marker are still escaped.
- `datetime` must be matched **before** `date` in the encode branch order
  (`datetime` subclasses `date`).
- Decode of a corrupt ISO string must fall through (return the marker dict
  unchanged) — same #31 "don't drop the whole pop" principle as corrupt base64
  bytes markers.
- Public `bytes` round-trip behavior and the legacy `__b64__` reader are
  unchanged.
- Sibling rich types (`Decimal`, `UUID`, `set`/`frozenset`, `tuple`,
  `pathlib.Path`) are **out of scope**: their docstrings correctly document
  *one-way* conversion. Only `datetime`/`date` carry a false round-trip claim.
  The broader sweep is deferred to a separate round if desired.

## Plan and independently verifiable tasks

- [ ] **R53-1 — RED: promote the pinning test.** Rewrite
      `test_datetime_serializes_to_isoformat` →
      `test_datetime_round_trips_to_datetime` asserting
      `isinstance(recovered, datetime)` and equality with the original
      (timezone-aware) datetime; add `test_date_round_trips_to_date` for the
      `date` sibling. Run → both FAIL (recovered is `str`).
- [ ] **R53-2 — RED: pin end-to-end via the property test.** Add
      `st.datetimes()` and `st.dates()` to `_scalars` in
      `tests/test_property_serialization.py` (mirroring the existing
      `st.binary` inclusion that pins the symmetric-bytes contract). Run →
      Hypothesis FAILS on datetime/date meta values.
- [ ] **R53-3 — GREEN: codec fix.** In `base.py`: add `_CODEC_DATETIME` /
      `_CODEC_DATE` constants; add `datetime`-then-`date` encode branches to
      `_encode_json_value`; update `_json_default`'s `datetime` branch to emit
      the marker (mirroring bytes' dual presence); add `datetime`/`date`
      decode branches to `_decode_json_value`; extend
      `_looks_like_codec_marker` to recognize the two new tags; correct the
      `_json_default` and `JSONSerializer.serialize` docstrings. Run all three
      tests → PASS.
- [ ] **R53-4 — Hardening test.** Add
      `test_corrupt_datetime_marker_does_not_crash_deserialize` mirroring
      `test_corrupt_b64_marker_does_not_crash_deserialize` (a marker with a
      non-ISO `data` string surfaces as the original dict, no crash).
- [ ] **R53-5 — Verify.** `uv run ruff check` then `uv run pytest` (CI runs
      ruff BEFORE pytest). Confirm 3833+ tests pass, ruff clean, mypy --strict
      clean.

## Acceptance criteria

1. `JSONSerializer().deserialize(JSONSerializer().serialize({"k": dt}))` yields
   a `datetime` equal to `dt` (and a `date` equal to a `date`), for the
   timezone-aware and naive cases.
2. A caller-owned dict shaped like
   `{"__scrapy_extension_json_type__": "datetime", "data": "..."}` still
   round-trips as a plain dict (escape still fires).
3. A corrupt datetime marker (`"data": "not-a-date"`) does not crash
   deserialize; the dict surfaces unchanged.
4. The Hypothesis property test passes with `datetime`/`date` in `_scalars`.
5. `ruff check`, `pytest`, and `mypy --strict` are all clean; no other test
   regresses.
