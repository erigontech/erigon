# Compute and stateful suites

Use this reference when locating, selecting, filtering, running, or validating a Benchmarkoor
compute or stateful suite.

## Contents

- Discover the current files
- Distinguish the suites
- Validate the selection
- Locate, download, and enumerate fixtures
- Run the selected suite
- Validate the result
- Recover from interruption
- Pre-populated snapshot workflow

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
the user explicitly requests that context. This context name is independent of running all versus
a filtered subset of its fixtures. Files ending in `.builder.yaml` build fixture archives; use
`.runner.yaml` to run downloaded fixtures locally.

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

## Locate, download, and enumerate fixtures

For EEST suites, the pinned runner YAML is the authoritative download specification:

```yaml
runner:
  benchmark:
    tests:
      source:
        eest_fixtures:
          fixtures_url: https://host/path/to/fixtures.tar.gz
          fixtures_subdir: path/inside/extracted/archive
```

Copy the exact `fixtures_url` and `fixtures_subdir` from the selected runner or authoritative
hosted run; do not guess a release from the suite name. A standalone `fixtures_url` is either a
direct `.tar.gz`/release asset or a GitHub Actions artifact URL. Benchmarkoor downloads and
extracts it automatically. GitHub Actions artifact URLs require the configured GitHub token;
public release assets normally do not. If the source instead uses `github_release`, an artifact
name, or a local source, preserve that mode rather than converting it silently.

Set `global.directories.cachedir` to a task-owned path outside every pristine/lower tree. It
defaults to `~/.cache/benchmarkoor`; standalone URLs are extracted below
`$CACHE_DIR/eest-url/<url-hash>/fixtures`. The hash directory is an internal detail, so discover it
from the run output instead of hard-coding it. A cache hit can suppress the download log; provenance
must still come from the pinned YAML and persisted suite summary.

After source preparation or the required smoke run, enumerate both the full archive inventory and
the fixtures selected by the current filter:

```bash
SUITE_HASH=$(jq -r .suite_hash "$RUN_DIR/config.json")
SUITE_DIR="$RESULTS_DIR/suites/$SUITE_HASH"
SUITE_SUMMARY="$SUITE_DIR/summary.json"
EEST_INDEX="$SUITE_DIR/.eest-meta/index.json"

test -f "$SUITE_SUMMARY"
test -f "$EEST_INDEX"
jq '{source, filter, resolved_tests: (.tests | length), eest_metadata}' "$SUITE_SUMMARY"
jq '{archive_test_count: .test_count, indexed_cases: (.test_cases | length)}' "$EEST_INDEX"
jq -r '.test_cases[].id' "$EEST_INDEX"
```

`.eest-meta/index.json` is copied from the extracted artifact and lists the source archive's full
fixture inventory even when the smoke or measured suite is filtered. `summary.json.tests` lists
only the fixtures selected for that suite hash.

To run every fixture in the selected source, assemble a measured config stack in which no loaded
file sets `runner.benchmark.tests.filter`; specifically, do not reuse the smoke/subset override.
Require the persisted summary filter to be empty and the selected count to equal the archive count:

```bash
test "$(jq -r '.filter // ""' "$SUITE_SUMMARY")" = ""
test "$(jq -r '.tests | length' "$SUITE_SUMMARY")" -eq \
  "$(jq -r '.test_count' "$EEST_INDEX")"
```

For a subset, retain the explicit filter and compare the resolved names against the intended IDs.
Never present a filtered count or suite hash as the full artifact.

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

This generic recovery applies only when no deliberately retained staging overlay exists. First
confirm the benchmark process and client container stopped. Run `benchmarkoor cleanup --force`
with the identical config stack, then require zero matching OverlayFS mounts and temporary
directories. Never unmount an overlay while its client is running, and never target the pristine
directory during cleanup. For a staged pre-populated run, use the exact-target recovery below;
broad cleanup can destroy the advanced baseline.

## Pre-populated snapshot workflow

Use this variant when the suite starts from an existing bloated, pruned, or otherwise pre-populated
datadir instead of State Actor. Resolve the authoritative hosted run and record its benchmarkoor and
benchmarkoor-tests commits, image digest and embedded client commit, ordered configs, genesis, fork
override, arguments, fixture URL and digest, filter, rollback strategy, resource limits, base head,
and pre-run end head. Keep the hosted API token in protected config or environment; never print or
persist it.

An old hosted run may use `schelk`. Match its dataset, fixtures, client, and limits while replacing
only the storage mechanism with OverlayFS. Check historical arguments against the current Erigon
source and image help so removed flags do not survive in the local override.

### Protect and identify the original

For the jochemnet host, use these roles:

```bash
PRISTINE_DIR=/home/erigon/jochemnet/erigon_snapshot_pruned
RUN_ROOT=/absolute/path/to/benchmarkoor-prepopulated-run
ORIGINAL_LOWER="$RUN_ROOT/lower-ro/erigon"
ADVANCED_LOWER="$RUN_ROOT/advanced-lower-ro/erigon"
OVERLAY_TMP="$RUN_ROOT/overlay-runtime"
```

Apply the main skill's read-only bind, probe-overlay canary, sizes, full-tree metadata fingerprint,
and dataset-appropriate critical hashes before starting a client. Require `ro` as a distinct mount
option first, then require an expected-failure write probe through the protected bind without ever
writing through the original path:

```bash
READONLY_CANARY=.benchmarkoor-readonly-canary
test ! -e "$ORIGINAL_LOWER/$READONLY_CANARY"
test ! -e "$PRISTINE_DIR/$READONLY_CANARY"
! sudo touch "$ORIGINAL_LOWER/$READONLY_CANARY"
test ! -e "$ORIGINAL_LOWER/$READONLY_CANARY"
test ! -e "$PRISTINE_DIR/$READONLY_CANARY"
```

For this dataset, include stable sidecars and lock files plus a full hash of `chaindata/mdbx.dat`
when feasible; the State Actor filenames in the main skill need not exist. Keep every upper, work,
cache, result, and probe path outside the pristine and lower trees.

The bind protects accesses through `ORIGINAL_LOWER`; the raw pristine path is still writable on the
host. Before starting, require no process has an open file below `PRISTINE_DIR`, no container uses
it as a mount source, and no alternate bind exposes it to a workload. Inspect host handles, mount
tables, and every running container's mounts rather than checking names alone. During staging and
measurement, keep a recursive write-event watcher on `PRISTINE_DIR`, periodically repeat the handle
and mount checks, and require every task container to mount only `ORIGINAL_LOWER` or
`ADVANCED_LOWER`. On any unexplained access or write event, stop the exact task-owned process and
ask the user; do not stop or alter the unrelated accessor.

Download sidecars can be stale after bloating. Boot the exact client only through a disposable
OverlayFS over `ORIGINAL_LOWER`, query `eth_getBlockByNumber("latest")`, and record its block,
hash, and state root. Stop it gracefully and remove its exact container and overlay. Require the
live head to match the selected suite's base head.

### Replace the remote datadir config

Do not load a hosted `schelk` runner file and override only `method`: Viper retains sibling
`schelk_options`, yielding an invalid mixed config. Load the dataset's global/genesis file and
supply a complete local map after it:

```yaml
global:
  env:
    ERIGON_SNAPSHOT_DIR: /absolute/path/to/lower-ro/erigon
  directories:
    cachedir: /absolute/path/to/task-owned-cache

runner:
  container_runtime: docker
  cleanup_on_start: false
  live_reporting:
    enabled: false
  directories:
    tmp_datadir: /absolute/path/to/overlay-runtime
  benchmark:
    results_dir: /absolute/path/to/results
    tests:
      metadata:
        labels:
          data-disk-type: overlayfs
          snapshot-kind: pre-populated
  client:
    config:
      genesis:
        erigon: ${ERIGON_GENESIS}
    datadirs:
      erigon:
        source_dir: ${ERIGON_SNAPSHOT_DIR}
        method: overlayfs
```

Copy the complete selected instance from the pinned `clients.yaml`; instance lists replace rather
than merge. Keep `cleanup_on_start: false` while a deliberate staging overlay is mounted because
broad orphan cleanup can destroy that baseline.

### Stage a pre-run once

If the source has no `pre_runs`, skip staging and use `ORIGINAL_LOWER` for the measured run.
Otherwise, a plain `container-recreate` run would restore the raw lower and replay the bundle for
every fixture. Build an immutable advanced baseline:

1. Run the exact stateful source against `ORIGINAL_LOWER` with
   `--debug.stop-after-prerun`. Omit the CPU and memory performance-limit config during this
   unmeasured command. The debug flag deliberately retains the staged datadir and container state
   for inspection; it does not make this a measured run.
2. Wait for the debug benchmarkoor process to exit successfully and preserve its exit status and
   logs. Require successful replay to the expected end block/hash/root, then resolve the exact
   container and data mount under `OVERLAY_TMP`.
3. Stop that container gracefully and wait until it has exited. Before removal, preserve its final
   logs and state; require a clean Erigon shutdown, no timeout, SIGKILL, or OOM kill, and no process
   or open handle below the merged directory. Run `sync`, then remove only that container. Leave
   its disposable merged overlay mounted. If any shutdown gate fails, discard the stage instead of
   promoting potentially inconsistent MDBX state.
4. Bind the merged directory to `ADVANCED_LOWER`, remount the bind read-only, and prove a direct
   write is rejected.
5. Canary-test a short-lived OverlayFS over `ADVANCED_LOWER`. The canary must appear only in the
   probe upper, never in either lower, the staging merged path, or the pristine snapshot.

After creating `ADVANCED_LOWER`, never address the writable staging-merged alias from a client or
benchmark command; use it only to verify the bind and later identify the exact cleanup target.

Create a field-for-field copy of the selected test-source map and remove only `pre_runs`. Preserve
its name, labels, fixture URL, fixture subdirectory, rollback strategy, runner settings, and any
other source fields. Do not try to delete the inherited key with a partial later map. During
measured runs, use this complete local source and `ADVANCED_LOWER`, and restore the resource-limit
config.

For the ordered command shown earlier in this reference, replace both State Actor datadir configs
with the pre-populated dataset's `global.yaml` and the complete local OverlayFS config. Omit its
hosted runner config. Replace the upstream source with the complete no-pre-runs source when using an
advanced baseline. Keep the context global and clients files from the pinned suite, followed by the
local instance, advanced-lower, and optional exact-filter overrides.

The two effective config stacks are therefore:

```text
staging: configs/global.yaml
         -> dataset global/genesis
         -> complete local OverlayFS config using ORIGINAL_LOWER
         -> context global
         -> complete original stateful source (with pre_runs)
         -> pinned clients
         -> complete local staging instance

measured: configs/global.yaml
          -> resource-limit config
          -> dataset global/genesis
          -> complete local OverlayFS config using ADVANCED_LOWER
          -> context global
          -> complete local stateful source (without pre_runs)
          -> pinned clients
          -> complete local measured instance and optional exact-filter override
```

Use `--debug.stop-after-prerun` only on the staging command. Use the same
`--limit-instance-client` and `--limit-instance-id` on both. Let the smoke create its persisted
`config.json`, inspect that effective configuration and its nonzero resolved tests, and only then
start the requested measured run. If the requested subset has at most ten fixtures, it may itself
serve as the smoke when it uses this identical final stack and passes every smoke gate. For an
all-fixtures run, remove the smoke filter and pass the archive-count equality gate above.

### Monitor and clean up

The staged design has one fixed overlay visible at its writable merged mount and read-only bind,
plus at most one disposable per-test overlay. Count unique upper directories or explicitly exclude
the fixed paths; a raw mountpoint count double-counts the bind. Require the benchmarkoor revision to
remove the prior per-test container and mount before creating the next one. Stop immediately if two
per-test uppers persist.

After staging, measure the fixed upper's allocated and apparent sizes and re-check filesystem
availability. The remaining availability must cover the conservative per-test copy-up budget from
the main skill plus the predeclared emergency floor. During measured runs, monitor free space and
the aggregate size of both the fixed staging upper and the current per-test upper; accounting for
only the per-test layer understates peak consumption.

After result validation, require zero per-test mounts, directories, containers, and benchmarkoor
processes. Compare pristine fingerprints, sizes, path count, and critical hashes while
`ORIGINAL_LOWER` is still mounted. Then unmount `ADVANCED_LOWER`, unmount and remove the exact
disposable staging tree, and unmount `ORIGINAL_LOWER` last. Require zero task overlays and no
canary in the pristine snapshot.

If a measured run is interrupted, do not use `benchmarkoor cleanup --force` while the staged
baseline is retained. Stop the exact recorded benchmarkoor PID and its exact client container,
wait for both to exit, and require no open handle before unmounting anything. Then resolve and
remove only that run's disposable per-test mount and directory. Preserve the fixed staging mount
and `ADVANCED_LOWER`, re-prove both lower binds read-only, and repeat the smoke gates before
resuming. If staging itself is interrupted, shutdown was not clean, or the mapping of a mount to
its upper is uncertain, stop all exact task-owned processes and containers, wait for exit, unmount
in the cleanup order above, discard only the verified task run root, and restage from
`ORIGINAL_LOWER`; never guess with a broad mount, process, or cleanup target.

`/home/erigon/jochemnet/erigon_snapshot_pruned` is designated pristine. A validated
`glamsterdam-devnet-7` stateful run downloaded its 1,463-case archive from the
[jochemnet stateful GitHub release asset](https://github.com/ethpandaops/benchmarkoor-tests/releases/download/eest-payloads-jochemnet-v1-amsterdam-stateful-d9ad55b3-20260807-000744/eest-payloads-jochemnet-v1-amsterdam-stateful-geth.tar.gz),
found raw head 24,402,727, staged to head 24,410,463, and selected 18 `ether_transfers` fixtures at
300M gas with an optional filter. Treat that URL, count, heads, and filtered result as
revision-specific evidence and repeat every verification.
