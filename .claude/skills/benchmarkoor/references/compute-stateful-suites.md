# Compute and stateful suites

Use this reference when locating, selecting, filtering, running, or validating a Benchmarkoor
compute or stateful suite.

## Contents

- Discover the current files
- Distinguish the suites
- Validate the selection
- Run the selected suite
- Validate the result
- Recover from interruption

## Discover the current files

Search the checked-out `benchmarkoor-tests` revision instead of assuming an old path still exists:

```bash
CONTEXTS_DIR="$TESTS_DIR/configs/contexts/repricing/v1"
rg --files "$CONTEXTS_DIR" |
  rg '/test-source\.(compute|stateful)\.runner\.ya?ml$' |
  sort

rg -l --glob 'test-source.*.runner.yaml' \
  'name:\s*state-actor-glamsterdam-devnet-7-(compute|stateful)\s*$' \
  "$CONTEXTS_DIR"
```

For the non-`full` Glamsterdam devnet 7 suites, resolve and require these files:

```bash
CONTEXT_DIR="$CONTEXTS_DIR/glamsterdam-devnet-7"
COMPUTE_CONFIG="$CONTEXT_DIR/test-source.compute.runner.yaml"
STATEFUL_CONFIG="$CONTEXT_DIR/test-source.stateful.runner.yaml"

test -f "$COMPUTE_CONFIG"
test -f "$STATEFUL_CONFIG"
test -f "$CONTEXT_DIR/global.yaml"
test -f "$CONTEXT_DIR/clients.yaml"
```

The sibling `glamsterdam-devnet-7-full` directory is a different fixture set. Select it only when
the user explicitly requests the `full` suite. Files ending in `.builder.yaml` build fixture
archives; use `.runner.yaml` to run downloaded fixtures locally.

## Distinguish the suites

| Property | Compute | Stateful |
|---|---|---|
| Runner file | `test-source.compute.runner.yaml` | `test-source.stateful.runner.yaml` |
| `test-type` label | `compute` | `stateful` |
| Fixture URL | contains `-compute-` | contains `-stateful-` |
| Expected rollback | `none` | `container-recreate` |
| Typical test path | `benchmark/compute/...` | `benchmark/stateful/bloatnet/...` |
| Datadir lifetime | one disposable overlay for the run | fresh container and overlay per fixture |

At `benchmarkoor-tests` commit `b9ddbab85b44fd8327940bcba9a8370798e064df`, successful local
runs produced these checkpoints:

| Suite | Tests | Suite hash |
|---|---:|---|
| Compute | 4,773 | `3c6dc791050b116d` |
| Stateful | 1,461 | `3f6a0898955dff4f` |

Treat those counts and hashes as revision-specific evidence, not permanent constants.

## Validate the selection

Inspect the resolved runner file before starting:

```bash
SUITE_KIND=${SUITE_KIND:?set to compute or stateful}
case "$SUITE_KIND" in compute|stateful) ;; *) exit 2 ;; esac
SUITE_RUNNER_CONFIG="$CONTEXT_DIR/test-source.$SUITE_KIND.runner.yaml"

rg -n 'name:|test-type:|fixtures_url:|fixtures_subdir:|rollback_strategy:' \
  "$SUITE_RUNNER_CONFIG"
git -C "$TESTS_DIR" rev-parse HEAD
git -C "$TESTS_DIR" status --short --branch
```

Require all five fields to describe the requested suite. The shared
`blockchain_tests_stateful_engine` fixtures subdirectory does not distinguish compute from
stateful. If the fixture archive is cached, the run log may omit a download line; retain the
selected YAML blob and URL as provenance.

For a subset, put `runner.benchmark.tests.filter` in the last local override. EEST names can carry
a leading `tests/` while filtering even though persisted result keys omit it, so anchor local
regular expressions with `^(tests/)?benchmark/...`, not only `^benchmark/...`. A filter changes the
derived suite hash. Record the parent runner config and full-suite hash alongside the filtered run,
and reject a smoke run that resolves to zero tests.

## Run the selected suite

Check the read-only lower, image, free space, and residual state first:

```bash
findmnt -n -o SOURCE,OPTIONS "$LOWER_ROOT/erigon"
findmnt -rn -t overlay | rg "$OVERLAY_TMP|benchmarkoor-overlay" || true
docker image inspect "$IMAGE_TAG"
docker ps --format '{{.Names}} {{.Image}} {{.Status}}'
df -h "$OVERLAY_TMP"
```

Require `ro` in the lower mount options and no unexplained overlay or client container. Preserve
this config order and keep the local override last:

```bash
cd "$TESTS_DIR"
sudo -n "$BENCHMARKOOR_BIN" run \
  --config configs/global.yaml \
  --config configs/resource-limits-eip-7870-fullnode.yaml \
  --config configs/datadirs/state-actor/v1/global.yaml \
  --config configs/datadirs/state-actor/v1/runner.yaml \
  --config "$CONTEXT_DIR/global.yaml" \
  --config "$SUITE_RUNNER_CONFIG" \
  --config "$CONTEXT_DIR/clients.yaml" \
  --config local-run-overrides.yaml \
  --limit-instance-client=erigon \
  --limit-instance-id=erigon-bal-full
```

For stateful, confirm the Benchmarkoor revision eagerly removes each previous container and
OverlayFS datadir before preparing the next. During a smoke run, require at most one live matching
overlay mount. Compute does not recreate per fixture, but its single upper layer is still disposable
and must never be the pristine directory.

## Validate the result

After exit, validate the persisted result rather than trusting the input variables:

```bash
jq '{suite_hash,status,test_counts,instance:{
  id:.instance.id,
  rollback_strategy:.instance.rollback_strategy,
  resource_limits:.instance.resource_limits,
  datadir:.instance.datadir
}}' "$RUN_DIR/config.json"

jq -r '.tests | keys[0:5][]' "$RUN_DIR/result.json"
rg 'Downloading fixtures tarball|Suite output created|Running tests with' \
  "$RUN_DIR/benchmarkoor.log"
```

Require compute result keys and rollback behavior to match compute, or stateful keys and rollback
behavior to match stateful. Confirm the resolved test count is nonzero, pass/fail totals are final,
the datadir method is `overlayfs`, the lower remained read-only, and no overlay mounts or benchmark
containers remain.

## Recover from interruption

First confirm the benchmark process and client container stopped. Run `benchmarkoor cleanup
--force` with the identical config stack, then require zero matching OverlayFS mounts and temporary
directories. Never unmount an overlay while its client is running, and never target the pristine
directory during cleanup.
