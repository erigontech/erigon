---
name: erigon-test-unit
description: Run Erigon short tests locally for affected packages or the full suite with make test-short. Use for targeted regression checks, broad changes, or a requested full short test run.
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
GOEXPERIMENT=jsonv2 go test -short ./execution/stagedsync/...
GOEXPERIMENT=jsonv2 go test -short -run TestName ./path/to/package/...
```

## Run from Repository Root

Must be run from the repository root (where the Makefile lives):

```bash
cd /path/to/erigon
make test-short
```

## When to Use

- Run affected package tests during local iteration and after fixing a bug.
- Run the full short suite for broad changes or when explicitly requested.
- Select lint and build checks using [Local verification in AGENTS.md](../../../AGENTS.md#local-verification).

## CI Equivalent

| Local command | CI workflow | Trigger |
|---------------|-------------|---------|
| `make test-short` | Unit tests (`ci.yml`) | push/PR to main or dispatch |

To dispatch remotely on a branch without a PR:
```bash
gh workflow run "Unit tests" --ref <branch>
```
