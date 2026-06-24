# SPEC — Hardening Pass (2026-06-24)

Companion to [INSIGHT.md](./INSIGHT.md) (evidence) and [PLAN.md](./PLAN.md) (execution).

## Problem
Insight surfaced **1 CRITICAL + 4 HIGH** correctness/security defects that survive the 98% unit
coverage bar, plus lifecycle/concurrency and test-infra gaps. Several are **silent under default
Scrapy settings** (`CONCURRENT_REQUESTS=16`) — the path-of-least-resistance failure mode.

## Goal
Close the high-severity correctness + security defects with **surgical, unit-verifiable** changes
that preserve the 98% coverage bar and the existing public API. Convert **silent** data-loss /
insecure defaults into **loud, fail-fast** errors.

## In-scope (Tier 1 — this pass)
1. **ConnectionManager refcounting** for colocated close (CRITICAL) + lock-during-retry fix (MEDIUM)
   + `BackendType` enum normalization (HIGH).
2. **Redis pop race** — distinguish a lost-payload race from corruption (HIGH).
3. **Security defaults** — Redis `ssl_check_hostname=True`; RabbitMQ require credentials (HIGH × 2).
4. **Boundary validation + legacy-body compat** — max-item-bytes at push/store; pre-base64 body
   fallback with deprecation warning (MEDIUM × 2).
5. **Message-queue ack fail-fast** — `CONCURRENT_REQUESTS>1` on Kafka/RabbitMQ → `ConfigurationError`
   (with opt-out) + Confluent secret redaction + RocketMQ stub gating (HIGH + LOW × 2).

## Non-goals (Tier 2/3 — deferred; spec'd in PLAN.md)
- Full ack/nack in-flight-set correlation (meta-stashed tokens) — replaces the fail-fast gate.
- Observability: open `monitor/` namespace; `Monitor` protocol + ScrapyStats default; backpressure.
- `StorageBackend` strategy layer; entry-point plugin registration; circuit-breaker.
- Distributed `DelayQueueStrategy`; ES atomic pop.
- Re-enable integration CI job (needs service containers); `hypothesis` property tests;
  `kafka-python` → `kafka-python-ng` migration.

## Constraints
- **Public API stable** (component factories, settings field names). New settings are additive
  (no removal of existing keys; defaults may tighten where the old default was insecure).
- **TDD**: regression test first (RED) → minimal fix (GREEN) → refactor. Coverage ≥ 95% on changed
  lines; project floor is 95%, current ~98%.
- **Immutability + no silent error swallowing** (project `CLAUDE.md`).
- All changes **unit-verifiable without real backend services** (integration CI is disabled).

## Acceptance (done when)
- `uv run pytest` fully green (existing suite + new regression tests).
- `uv run ruff check` clean; mypy clean on changed files.
- Each Tier-1 defect has a regression test that **fails pre-fix, passes post-fix**.
- No new public-API breakage (additive only).
