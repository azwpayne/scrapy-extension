# SPEC-round57 — codec-marker escape crashes on unhashable tag value

## Context and audit evidence

Found via the R64 deep-insight fire (cron `6a6e7f48`), dimension `base-codec`
(request-meta JSON codec round-trip in `src/scrapy_extension/backends/base.py`).
Confirmed REAL by an opus adversarial verifier (high confidence) and then
**independently re-reproduced by hand** before implementation.

The tagged-codec machinery round-trips arbitrary `request.meta` values through
JSON. To stop a caller-owned dict that *happens* to collide with a wire marker
(e.g. a spider's own `{"__scrapy_extension_json_type__": "datetime", "data":
...}`) from being silently re-typed during deserialize, `_looks_like_codec_marker`
detects the collision on the **encode** side and wraps the dict in a
`_CODEC_DICT` escape envelope. R53 (commit `c49a725`) hardened the datetime/date
type round-trip; this finding is a distinct gap in the *escape* machinery.

**The asymmetry (empirically reproduced):**

- Encode — `base.py:148-153`, `_looks_like_codec_marker`:
  ```python
  return (
      len(obj) == 2
      and obj.get(_CODEC_TAG)
      in {_CODEC_BYTES, _CODEC_DICT, _CODEC_DATETIME, _CODEC_DATE}   # line 151 — SET membership HASHES the left operand
      and _CODEC_DATA in obj
  )
  ```
  `x in {set}` hashes `x`. When the caller dict's tag value is an **unhashable**
  `list`/`dict`, this raises `TypeError: unhashable type: 'list'/'dict'` instead
  of returning a boolean.

- Decode — `base.py:204/221/231/241`, every marker branch uses equality:
  `obj.get(_CODEC_TAG) == _CODEC_*`. Equality never hashes, so decode of the
  identical shape returns the dict **unchanged** (graceful).

Reproduction (`uv run python`):
```
list-tag  SERIALIZE CRASH -> TypeError("unhashable type: 'list'")
dict-tag  SERIALIZE CRASH -> TypeError("unhashable type: 'dict'")
list-tag  DECODE OK       -> {'__scrapy_extension_json_type__': ['listval'], 'data': 'y'} (unchanged: True)
str-tag   ROUNDTRIP       -> True
```
So `serialize()` crashes on an input that `deserialize()` would have handled
fine — the escape contract ("any marker-shaped caller dict survives untouched",
encoded by the R53 test `test_datetime_marker_shaped_user_dict_round_trips_as_dict`)
holds for hashable string tags but **crashes the queue push** for unhashable tag
values.

**Caller path:** `BackendQueue._serializer` → `JSONSerializer()` →
`JSONSerializer.serialize` → `_encode_json_value` → (dict branch) →
`_looks_like_codec_marker(encoded)`. A Scrapy request whose `meta` contains a
marker-shaped dict with an unhashable tag value therefore crashes the queue
push with an opaque `TypeError` rooted in the escape check, not in the user's
data.

**Severity: low.** The triggering input is contrived (the marker key
`__scrapy_extension_json_type__` is an internal/private name, and the value
must be an unhashable `list`/`dict`). But the escape machinery exists *precisely*
to honor the survives-untouched contract, the encode/decode asymmetry is
concrete, and the fix is one-line and zero-risk for every hashable input.

## Goal

Make the encode-side marker check robust to unhashable tag values, restoring
encode/decode symmetry: a marker-shaped caller dict with *any* tag value
(hashable or not) must round-trip unchanged, never crash `serialize()`.

## Specification

In `_looks_like_codec_marker` (`src/scrapy_extension/backends/base.py:151`),
change the set-literal membership test to a tuple membership test:

```python
# before
        and obj.get(_CODEC_TAG)
        in {_CODEC_BYTES, _CODEC_DICT, _CODEC_DATETIME, _CODEC_DATE}
# after
        and obj.get(_CODEC_TAG)
        in (_CODEC_BYTES, _CODEC_DICT, _CODEC_DATETIME, _CODEC_DATE)
```

`x in (tuple)` compares with `==` pairwise and **never hashes** the left
operand, so for every hashable input it returns the identical boolean as the
set (the 4 valid tags match; everything else is `False`), and for an unhashable
tag value it returns `False` instead of raising. With the check returning
`False`, the dict serializes as-is and decode (which also returns `False` for
every `==` marker branch) returns it unchanged — symmetric. This mirrors the
decode side's equality approach exactly. No API or behavior change for any
hashable input.

## Plan and independently verifiable tasks

- **R57-1 — RED test.** Add a test asserting a marker-shaped caller dict with
  an *unhashable* tag value round-trips through `JSONSerializer` unchanged:
  ```python
  s = JSONSerializer()
  for unhashable_tag in (["listval"], {"nested": 1}):
      d = {_CODEC_TAG: unhashable_tag, _CODEC_DATA: "y"}
      assert s.deserialize(s.serialize(d)) == d
  ```
  Verify it FAILS on current code with `TypeError: unhashable type` at
  `serialize`. → verify: `uv run pytest tests/test_backends.py::<new>` exits
  non-zero with the TypeError.

- **R57-2 — GREEN fix.** Apply the one-line set→tuple change at `base.py:151`.
  → verify: the R57-1 test now PASSES.

- **R57-3 — hardening + no-regression.** Add the dict-tag variant and confirm
  the existing hashable-string escape test still passes (the str-tag control
  must round-trip exactly as before). → verify: full `tests/test_backends.py`
  green; `test_datetime_marker_shaped_user_dict_round_trips_as_dict` still
  passes.

## Acceptance criteria

1. `s.deserialize(s.serialize({_CODEC_TAG: ["listval"], _CODEC_DATA: "y"})) ==
   {_CODEC_TAG: ["listval"], _CODEC_DATA: "y"}` (and the dict-tag variant) —
   no `TypeError`.
2. All existing marker/serde tests still pass (no behavior change for hashable
   inputs).
3. Gate green: `uv run ruff check .` + `uv run pytest` + `uv run mypy --strict
   src/scrapy_extension`.
4. One atomic commit, ff-merged to `main`; CI green.
