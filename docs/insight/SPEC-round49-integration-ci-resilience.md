# Round 49 — SPEC / PLAN / TASK: integration CI resilience

## Context and audit evidence

The Round 48 CI audit verified that the current remote main run
`30434874958` was cancelled before checkout: its `integration-tests` job spent
almost the entire configured 15-minute job budget in GitHub's `Initialize
containers` phase.  Every unit matrix lane passed.  The immediately preceding
run (`30424974372`) used byte-identical workflow code and completed integration
in roughly two and a half minutes, which identifies a cold service-image
initialization event rather than a source-test regression.

The same audit found a deterministic local integration mismatch: the locked
client and CI service use Elasticsearch 9.4.1, while the documented local
`tests/integration/docker-compose.yml` still starts Elasticsearch 8.11.3.
The CI workflow records that the 9.x client sends a compatibility header which
Elasticsearch 8 rejects, so the local fixture contradicts the supported test
environment.

## Specification

- The integration job retains a bounded runtime while allowing sufficient
  headroom for a one-off cold pull before its live backend tests begin.
- Local Docker Compose and GitHub Actions use the exact same Elasticsearch
  image version supported by the locked client.
- Static regression tests parse both YAML files and enforce the timeout and
  Elasticsearch image contract, so future edits cannot silently reintroduce
  the divergence.

## Plan and independently verifiable tasks

- [ ] **R49-1 — Integration startup budget:** raise the integration job cap
      from 15 to 30 minutes and explain that service-image startup is included.
- [ ] **R49-2 — Elasticsearch fixture parity:** upgrade the local Compose
      Elasticsearch service and its comment from 8.11.3 to 9.4.1.
- [ ] **R49-3 — Static CI contract:** add YAML-based regression assertions for
      the timeout and matching image strings.
- [ ] **R49-4 — Verify and re-audit:** run focused CI-config tests, YAML and
      shell validation, full quality gates, then inspect the exact pushed SHA.

## Acceptance criteria

1. The integration job still has a finite timeout but no longer fails solely
   because a cold service initialization consumes the former 15-minute cap.
2. CI and local integration Compose both select
   `docker.elastic.co/elasticsearch/elasticsearch:9.4.1`.
3. Automated tests parse and lock both configuration values.
4. The final pushed SHA completes all GitHub Actions jobs successfully.
