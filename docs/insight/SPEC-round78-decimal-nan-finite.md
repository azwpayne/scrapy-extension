# SPEC-round78 — Reject non-finite `Decimal` in request serde (`_json_default`)

**Round log:** R87 (SPEC counter round 78; offset R−9 from the round log).
**Status:** queued from the R86 fresh scan (`base.py` request-serde dimension), confirmed real + surgical + clean-file.
**Surface:** request serde — `scrapy_extension/backends/base.py::_json_default` (in-scope rotation surface, NOT in the exhausted-themes list).

## Context and audit evidence

`JSONSerializer.serialize` encodes Scrapy request dicts with two layered guards against
non-finite numbers:

1. `_encode_json_value` rejects non-finite **`float`** up front (`base.py:192-193`):

   ```python
   if isinstance(obj, float) and not math.isfinite(obj):
       raise ValueError(f"JSON numbers must be finite, got {obj!r}")
   ```

2. `json.dumps(..., allow_nan=False)` (`base.py:349-353`) is the backstop that turns
   `float('nan')` / `float('inf')` into a hard error rather than the non-standard JSON
   tokens `NaN` / `Infinity`.

Both guards target **`float`**. A non-finite **`Decimal`** (`Decimal('NaN')`,
`Decimal('Infinity')`, `Decimal('-Infinity')`, `Decimal('sNaN')`) takes a different path:
it is not `float`/`str`/`int`/`bool`/`None`, so `_encode_json_value` falls through to
`_json_default` (`base.py:196`), which at `base.py:93-94` does:

```python
if isinstance(obj, Decimal):
    return str(obj)
```

`str(Decimal('NaN'))` → `"NaN"` (a plain JSON string). The value is then emitted as the
JSON string `"NaN"`, decoded back as the Python `str` `"NaN"` — **a silent type+value
corruption** (`Decimal('NaN')` → `str "NaN"`) that slips past *both* non-finite guards.
The caller asked for fail-fast on non-finite numbers (that is the entire point of
`allow_nan=False` and the float guard); `Decimal` quietly subverts it.

Finite `Decimal` → `str` is intentional and correct (preserves exact representation, no
float drift — covered by `test_decimal_serializes_as_str`, R19). Only the non-finite
case is a defect.

**Why this is not exhausted:** the exhausted list names datetime/date round-trip (R53)
and bytes serde symmetry. This finding is about the `allow_nan=False` / non-finite-number
invariant on a *different* numeric type (`Decimal`) — distinct surface, distinct guard.

## Goal

A non-finite `Decimal` in a serialized request dict must fail fast at serialize time with
a clear `ValueError`, exactly like a non-finite `float` does today — instead of being
silently stringified into a wrong-type value that round-trips as `"NaN"` / `"Infinity"`.

## Specification

In `_json_default` (`src/scrapy_extension/backends/base.py`), the `Decimal` branch must
reject non-finite values before the `str()` coercion:

- `Decimal.is_finite()` is the stdlib predicate (`True` for finite, `False` for `NaN` /
  `Infinity` / `-Infinity` / signaling `sNaN`).
- Raise `ValueError` (not `TypeError`) for non-finite `Decimal` — this is a *value*
  problem, mirroring the float guard at `base.py:192-193`, not an unhandled-type problem.
- Finite `Decimal` continues to return `str(obj)` unchanged (R19 contract preserved).
- No other branch, signature, or sibling type is touched.

`ValueError` propagates fail-fast: `serialize()` (`base.py:349-353`) has no `try/except`,
and `_encode_json_value` (`base.py:196`) calls `_json_default` inline — so the error
surfaces to the caller immediately, consistent with the existing float behavior.

## Plan and independently verifiable tasks

- **R87-1 (RED):** add `TestJSONSerializer.test_non_finite_decimal_rejected` —
  parametrized over `Decimal('NaN')`, `Decimal('Infinity')`, `Decimal('-Infinity')`,
  `Decimal('sNaN')`; each `serializer.serialize({"x": d})` must raise `ValueError`
  matching a "finite" message. Run → fails (today it returns `"NaN"`/`"Infinity"`).
- **R87-2 (GREEN):** in `_json_default`, guard the `Decimal` branch:
  `if not obj.is_finite(): raise ValueError(f"Cannot serialize non-finite Decimal: {obj!r}")`
  before `return str(obj)`. Run → passes.
- **R87-3 (HARDEN):** add `test_finite_decimal_still_str` (regression guard that finite
  `Decimal('19.99')` still round-trips as the string `"19.99"`) if not already covered —
  it IS covered by R19 `test_decimal_serializes_as_str`, so instead add an explicit
  `Decimal('0')` / `Decimal('-0')` finite edge to the parametrization to prove the guard
  does not over-reject finite sentinels.

## Acceptance criteria

- [ ] `pytest tests/test_backends.py::TestJSONSerializer` green, including the new
      non-finite-`Decimal` rejection test.
- [ ] `uv run ruff check .` clean.
- [ ] `uv run mypy --strict src/scrapy_extension` clean.
- [ ] `uv run pytest` full suite green (no regression).
- [ ] Finite `Decimal` round-trip unchanged (R19 test still green).
- [ ] One atomic commit; ff-merge to `main`; CI green.
