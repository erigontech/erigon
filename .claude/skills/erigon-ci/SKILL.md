---
name: erigon-ci
description: |
  Run Erigon CI checks locally and/or trigger them remotely on a branch via GitHub Actions workflow_dispatch.
  Use this when you need to verify a branch passes all CI before or after pushing — especially for branches
  like bal-devnet-2 that don't auto-trigger on push/PR events.
---

# Erigon CI Skill

Orchestrates CI verification: run test groups locally and/or dispatch GitHub Actions on any branch.

Each test group has its own dedicated skill for drill-down on failures. Use those when a specific group fails and you need to re-run individual tests.

---

## Local Test Groups

| Group | Skill | Command | Speed | Use When |
|-------|-------|---------|-------|----------|
| lint | *(inline)* | `make lint` | ~1 min | Before every push |
| unit | `erigon-test-unit` | `make test-short` | ~5 min | Pre-push gate |
| all | `erigon-test-all` | `GOGC=80 make test-all` | ~30 min | Before PR review |
| race | `erigon-test-race` | `make test-all-race` | ~60 min | Concurrency changes |
| hive | `erigon-test-hive` | `make test-hive` | ~20 min | EL/CL interop changes |
| rpc | `erigon-test-rpc` | *(requires synced DB)* | ~10 min | RPC API changes |
| assertoor | *(remote only)* | dispatch only | — | Kurtosis network test |

### Lint (run first — non-deterministic, may need multiple runs)
```bash
make lint
```

### Quick gate (before pushing a fix)
```bash
make lint && make test-short
```

### Full gate (before marking PR ready for review)
```bash
make lint && make erigon integration && GOGC=80 make test-all
```

### Race check (for concurrency-sensitive changes)
```bash
make lint && make test-all-race
```

---

## Part 2: Remote Dispatch (trigger CI on any branch)

Use `gh workflow run` to trigger workflows on a branch. Required for branches not targeting main (e.g. `bal-devnet-2`).

### Dispatch all workflows on a branch

```bash
BRANCH=$(git branch --show-current)  # or set explicitly: BRANCH="bal-devnet-2"

for wf in \
  "Unit tests" \
  "All tests" \
  "Lint" \
  "Test Hive" \
  "Kurtosis Assertoor GitHub Action" \
  "QA - RPC Integration Tests" \
  "QA - RPC Integration Tests Remote" \
  "QA - RPC Integration Tests (Gnosis)" \
  "Windows downloader tests"; do
  echo "Dispatching: $wf"
  gh workflow run "$wf" --ref $BRANCH
done
```

### Workflow dispatch support

| Workflow name (GitHub)              | dispatch? | Local skill | File |
|-------------------------------------|-----------|-------------|------|
| Unit tests                          | yes       | `erigon-test-unit` | `ci.yml` |
| All tests                           | yes       | `erigon-test-all` | `test-all-erigon.yml` |
| Lint                                | yes       | *(make lint)* | `lint.yml` |
| Test Hive                           | yes       | `erigon-test-hive` | `test-hive.yml` |
| Kurtosis Assertoor GitHub Action    | yes       | *(remote only)* | `test-kurtosis-assertoor.yml` |
| QA - RPC Integration Tests          | yes       | `erigon-test-rpc` | `qa-rpc-integration-tests.yml` |
| QA - RPC Integration Tests Remote   | yes       | `erigon-test-rpc` | `qa-rpc-integration-tests-remote.yml` |
| QA - RPC Integration Tests (Gnosis) | yes       | `erigon-test-rpc` | `qa-rpc-integration-tests-gnosis.yml` |
| Windows downloader tests            | yes       | *(remote)* | `test-win-downloader.yml` |
| Consensus spec                      | **no**    | *(PR-only)* | `test-integration-caplin.yml` |

Note: **Consensus spec** has no `workflow_dispatch` — only fires on pull_request events targeting main/release.

### Monitor runs

```bash
BRANCH=$(git branch --show-current)
gh run list --branch $BRANCH --limit 15
```

Check for failures:
```bash
gh run list --branch $BRANCH --limit 20 | grep -E "failure|cancelled"
```

Watch a specific run:
```bash
gh run watch <run-id>
```

---

## Part 3: Full CI Gate (local + remote)

When preparing a devnet/feature branch for review:

1. **Local gate** — fast feedback before pushing:
   ```bash
   make lint && make erigon integration && make test-short
   ```

2. **Push branch**:
   ```bash
   git push origin <branch>
   ```

3. **Dispatch all remote workflows** (use the loop from Part 2).

4. **Monitor** until all pass:
   ```bash
   gh run list --branch $(git branch --show-current) --limit 15
   ```

5. **Race check** (for concurrency-heavy changes):
   ```bash
   make test-all-race
   ```
