# R48 SPEC — Pull-request CI admission gate

## Finding

The CI workflow triggers only on pushes to `main`, despite contributor
documentation stating that the unit matrix and integration suite run on every
push and pull request. A PR can therefore merge without executing lint, type,
security, artifact, unit, or integration checks.

## Required behavior

Keep the existing `push` trigger restricted to `main` and add a
`pull_request` trigger. The same existing jobs must then run before merge.

## Acceptance criteria

- `.github/workflows/ci.yml` declares both `push` and `pull_request` triggers.
- The workflow remains valid YAML/GitHub Actions syntax.
- The contributor-facing CI claim matches the workflow.

