# Round 41 — SPEC / PLAN / TASK: configuration and plugin redaction boundary

**Base:** `main` after Round 40B startup-error redaction.
**Scope:** prevent mutable configuration, plugin metadata, and plugin failures
from escaping through public startup errors, exception graphs, traceback-frame
locals, or discovery logs.

## Audit conclusion

Round 40B removed raw driver diagnostics from backend connection failures, but
the next audit found startup boundaries that could still retain data in public
errors: Elasticsearch/MongoDB revalidation chains, host interpolation,
mutable MongoDB mode/seed/write-concern input, plugin loader and metadata
paths in `ConnectionManager`, registry key enumeration, and direct Pydantic
`ValidationError` rendering/structured APIs. Retry conversion also trusted
arbitrary `__int__` / `__float__` exceptions.

## Specification

1. Startup configuration failures expose a typed, static
   `ConfigurationError`; only a verified bundled field name may be retained.
   Error text is retained only when it matches a literal whitelist or a
   structural parser over bundled-only values. An exception subclass is never
   treated as a provenance signal.
2. The public error is raised after the relevant `except` suite, leaving both
   `__cause__` and `__context__` unset.
3. Bundled optional-dependency failures keep the plain `ImportError` family
   and no-retry behavior. At resolver and manager public boundaries they are
   rebuilt with vetted static text and no chain/loader frames; direct backend
   module imports retain their existing detailed optional-dependency behavior.
   Untrusted plugin load/constructor failures are static configuration errors.
4. Registry discovery remains graceful-skip and bundled-wins, but its
   diagnostics, lookup failures, capability errors, and valid-value hints
   never include entry-point metadata, exception text, or a traceback.
5. Direct settings validation keeps its `ValidationError` control flow, but
   rebuilds failures with verified field locations, `input=None`, and static
   diagnostics so `str`, `repr`, traceback, `errors()`, `json()`, and package
   traceback locals do not retain host, credential, or malformed-input data.
   Trusted settings classes are resolved by actual class identity, not mutable
   module/name metadata.
6. Plugin settings metadata is read fail-closed, and every configuration
   adaptation / retry / construction path shares the same plugin-loader
   boundary.
7. Network-operation errors retain `BackendConnectionError` semantics. Public
   MongoDB and Elasticsearch startup boundaries rebuild that same error type
   after a failure so mutable URI/host snapshots cannot survive in traceback
   frames.
8. Every public `ConnectionManager` startup/access route rebuilds propagated
   configuration, import, and connection failures after manager frames unwind.
   Safe retry-count grammar and bundled backend labels may be retained; plugin
   metadata uses exact built-in strings before any set/dict operation.

## Plan and tasks

1. Sanitize `ConnectionManager` plugin path, settings construction,
   metadata lookup, constructor, retry-policy, configuration-adaptation,
   failed registry snapshot, and release paths. Put terminal boundaries on
   direct connection, backend property, capability accessors, durable-push
   startup path, and health probe so rethrows cannot restore `self.settings`
   through traceback locals.
2. Raise static Elasticsearch and MongoDB mutable-snapshot errors after their
   handlers; remove host interpolation and conversion/URI/seed exception
   chains.
3. Make registry discovery/conflict/lookup diagnostics static, list bundled
   keys only, and discard retained entry-point source metadata after
   validation.
4. Introduce a shared `RedactedBaseSettings` boundary to preserve typed
   Pydantic errors while removing raw input from rendered and structured APIs,
   hostile exception subclasses, and spoofed model metadata.
5. Add marker regressions for direct settings, mutable snapshots, manager
   construction/retry/adaptation policy, all public manager access paths,
   plugin metadata, and registry logs. Assertions must inspect manager
   `self.settings` and wrapper `args`, not only `repr(frame.f_locals)`;
   preserve no-retry, lazy-load, bundled-wins, and bundled-ImportError type
   behavior.
6. Run focused suites, static checks, full non-integration tests, package
   verification, and CI before/after the atomic commit.

## Acceptance evidence

- Synthetic markers are absent from public error strings, `__dict__`,
  formatted tracebacks, package-frame locals (including direct manager
  `self.settings` and wrapper arguments), Pydantic `errors()` / `json()`, log
  records, causes, and contexts.
- Invalid configuration performs no backend constructor/client I/O and is not
  retried.
- Valid bundled settings and third-party discovery semantics retain their
  existing behavior; public optional-dependency failures retain their type and
  no-retry contract with static diagnostics.
