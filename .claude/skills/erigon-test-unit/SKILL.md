---
name: erigon-test-unit
description: Run Erigon unit tests locally using make test-short. Use this for fast pre-push verification. Equivalent to the "Unit tests" CI workflow.
---

# Erigon Unit Tests

Runs the full unit test suite with `-short -failfast`. Catches most regressions in ~5 minutes.

## Command

```bash
make test-short
```

Equivalent to the **"Unit tests"** GitHub Actions workflow (`ci.yml`).

## Run Specific Package

```bash
go test -short ./execution/stagedsync/...
go test -short -run TestName ./path/to/package/...
```

## Run from Repository Root

Must be run from the repository root (where the Makefile lives):

```bash
cd /path/to/erigon
make test-short
```

## When to Use

- Before every push as a fast gate
- After fixing a bug to confirm no regressions
- Part of the quick gate: `make lint && make test-short`

## CI Equivalent

| Local command | CI workflow | Trigger |
|---------------|-------------|---------|
| `make test-short` | Unit tests (`ci.yml`) | push/PR to main or dispatch |

To dispatch remotely on a branch without a PR:
```bash
gh workflow run "Unit tests" --ref <branch>
```
