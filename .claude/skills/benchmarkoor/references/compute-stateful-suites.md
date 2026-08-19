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

After source preparation or the required smoke run, set `RESULTS_DIR` to that command's effective
`runner.benchmark.results_dir`. Select `RUN_DIR` only from its `runs` child; never infer the results
root from `$TESTS_DIR` or from an unrelated run. Then enumerate both the full archive inventory and
the fixtures selected by the current filter:

```bash
RESULTS_DIR=${RESULTS_DIR:?set to the effective runner.benchmark.results_dir}
RESULTS_DIR=$(realpath -e -- "$RESULTS_DIR")
RUNS_DIR=$(realpath -e -- "$RESULTS_DIR/runs")
RUN_DIR=${RUN_DIR:?set to a smoke or measured run below RESULTS_DIR/runs}
RUN_DIR=$(realpath -e -- "$RUN_DIR")
test "$(dirname -- "$RUN_DIR")" = "$RUNS_DIR"
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

### Select the dataset context

Resolve the pre-populated dataset's context independently of the State Actor `CONTEXT_DIR` defined
earlier. The dataset context supplies fork-schedule variables and the matching clients; never reuse
the generic context or mix files from the two trees. For the jochemnet Glamsterdam devnet 7 run:

```bash
DATASET_DATADIR_GLOBAL="$TESTS_DIR/configs/datadirs/jochemnet/v1/global.yaml"
DATASET_CONTEXT_DIR="$TESTS_DIR/configs/contexts/repricing/jochemnet/v1/glamsterdam-devnet-7"
DATASET_CONTEXT_GLOBAL="$DATASET_CONTEXT_DIR/global.yaml"
DATASET_CLIENTS_CONFIG="$DATASET_CONTEXT_DIR/clients.yaml"
DATASET_COMPUTE_CONFIG="$DATASET_CONTEXT_DIR/test-source.compute.runner.yaml"
DATASET_STATEFUL_CONFIG="$DATASET_CONTEXT_DIR/test-source.stateful.runner.yaml"

test -f "$DATASET_DATADIR_GLOBAL" || exit 1
test -f "$DATASET_CONTEXT_GLOBAL" || exit 1
test -f "$DATASET_CLIENTS_CONFIG" || exit 1

case "${SUITE_KIND:?set to compute or stateful}" in
  compute) DATASET_SOURCE_CONFIG="$DATASET_COMPUTE_CONFIG" ;;
  stateful) DATASET_SOURCE_CONFIG="$DATASET_STATEFUL_CONFIG" ;;
  *) exit 2 ;;
esac
test -f "$DATASET_SOURCE_CONFIG" || exit 1

EXPECTED_AMSTERDAM_ACTIVATION_TS=$(yq -r \
  '.global.env.AMSTERDAM_ACTIVATION_TS // ""' "$DATASET_CONTEXT_GLOBAL")
test -n "$EXPECTED_AMSTERDAM_ACTIVATION_TS" || exit 1
rg -n 'override\.amsterdam=.*AMSTERDAM_ACTIVATION_TS' \
  "$DATASET_CLIENTS_CONFIG" || exit 1
```

Resolve equivalent paths from the authoritative hosted run for another dataset or fork. Require
every variable referenced by the selected source and client instance to be defined by an earlier
config in both the staging and measured stacks. For jochemnet, confirm the persisted Erigon command
contains `--override.amsterdam=$EXPECTED_AMSTERDAM_ACTIVATION_TS`; an empty or different value is a
hard failure.

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
and dataset-appropriate critical hashes before starting a client. Set `INTEGRITY_ROOT` to
`ORIGINAL_LOWER` after its read-only remount, and use that path for every traversal, `du`, content
hash, and before/after fingerprint. Never collect integrity data through `PRISTINE_DIR`: reads on a
writable `relatime` mount can mutate atime. Require `ro` as a distinct mount option first, then
require an expected-failure write probe through the protected bind without ever writing through or
inspecting the original path:

```bash
INTEGRITY_ROOT="$ORIGINAL_LOWER"
READONLY_CANARY=.benchmarkoor-readonly-canary
test ! -e "$INTEGRITY_ROOT/$READONLY_CANARY"
! sudo touch "$INTEGRITY_ROOT/$READONLY_CANARY"
test ! -e "$INTEGRITY_ROOT/$READONLY_CANARY"
```

For this dataset, include stable sidecars and lock files plus a full hash of `chaindata/mdbx.dat`
when feasible; the State Actor filenames in the main skill need not exist. Keep every upper, work,
cache, result, and probe path outside the pristine and lower trees.

The bind protects accesses through `ORIGINAL_LOWER`; the raw pristine path is still writable on the
host. Before starting, require no process has an open file below `PRISTINE_DIR`, no container uses
it as a mount source, and no alternate bind exposes it to a workload. Inspect host handles, mount
tables, and every running container's mounts rather than checking names alone; perform any required
tree traversal through `ORIGINAL_LOWER`. During staging and measurement, keep a recursive
write-event watcher on `ORIGINAL_LOWER` and periodically repeat the handle and mount checks.

With `method: overlayfs`, the container must mount a task-owned `merged` directory, not either
protected lower directly. Verify the container's datadir source and the corresponding host overlay
as one chain. Use `ORIGINAL_LOWER` as `expected_lower` for compute and during stateful staging. Use
`ADVANCED_LOWER` for stateful smoke and measured runs after staging; replace `/data` only when the
effective client config uses another datadir target:

```bash
verify_task_overlay() {
  local container_id=${1:?set container ID}
  local expected_lower=${2:?set expected read-only lower}
  local container_datadir=${3:-/data}
  local -a data_sources
  local merged mount_target overlay_options lowerdir lower_options
  local resolved_mount_target resolved_merged resolved_lowerdir resolved_expected_lower

  mapfile -t data_sources < <(
    docker inspect "$container_id" |
      jq -r --arg target "$container_datadir" \
        '.[0].Mounts[] | select(.Destination == $target) | .Source'
  )
  test "${#data_sources[@]}" -eq 1 || return 1
  merged=${data_sources[0]}
  case "$merged" in
    "$OVERLAY_TMP"/benchmarkoor-overlay-*/merged) ;;
    *) return 1 ;;
  esac

  test "$(findmnt -n -T "$merged" -o FSTYPE)" = overlay || return 1
  mount_target=$(findmnt -n -T "$merged" -o TARGET) || return 1
  resolved_mount_target=$(realpath -e -- "$mount_target") || return 1
  resolved_merged=$(realpath -e -- "$merged") || return 1
  test "$resolved_mount_target" = "$resolved_merged" || return 1

  overlay_options=$(findmnt -n -T "$merged" -o OPTIONS) || return 1
  lowerdir=${overlay_options#*lowerdir=}
  test "$lowerdir" != "$overlay_options" || return 1
  lowerdir=${lowerdir%%,*}
  resolved_lowerdir=$(realpath -e -- "$lowerdir") || return 1
  resolved_expected_lower=$(realpath -e -- "$expected_lower") || return 1
  test "$resolved_lowerdir" = "$resolved_expected_lower" || return 1

  lower_options=$(findmnt -n -T "$expected_lower" -o OPTIONS) || return 1
  case ",$lower_options," in
    *,ro,*) ;;
    *) return 1 ;;
  esac

  printf '%s\n' "$merged"
}
```

Require exactly one datadir source from Docker inspection and repeat this verification whenever
benchmarkoor recreates the client. Neither protected lower should appear directly in a task
container's mount sources. On any unexplained access, mount chain, or write event, stop the exact
task-owned process and ask the user; do not stop or alter the unrelated accessor.

Download sidecars can be stale after bloating. Boot the exact client only through a disposable
OverlayFS over `ORIGINAL_LOWER`, query `eth_getBlockByNumber("latest")`, and record its block,
hash, and state root. Stop it gracefully and remove its exact container and overlay. Require the
live head to match the selected suite's base head.

### Replace the remote datadir config

Do not load a hosted `schelk` runner file and override only `method`: Viper retains sibling
`schelk_options`, yielding an invalid mixed config. Load the dataset's global/genesis file and
supply a complete local map after the selected test source and clients. Test-source files can set
`runner.container_runtime` and `data-disk-type`; if loaded later, those values override Docker and
OverlayFS settings from an earlier local map:

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

Copy the complete selected instance from `DATASET_CLIENTS_CONFIG`; instance lists replace rather
than merge. Keep `cleanup_on_start: false` while a deliberate staging overlay is mounted because
broad orphan cleanup can destroy that baseline.

Record the exact absolute `results_dir` used by each command. Before using the shared validation
or fixture-enumeration recipes, assign that path to the shell `RESULTS_DIR`. If staging, smoke, and
measured commands use different results roots, reset `RESULTS_DIR` for each corresponding run; the
recipes intentionally never fall back to `$TESTS_DIR/results`.

### Run compute with one protected overlay

For compute, require the complete selected source to declare `test-type: compute` and
`rollback_strategy: none`. Preserve its `pre_runs` map when one exists. Load the dataset context and
complete compute source rather than either State Actor source or the stateful source:

```text
compute: configs/global.yaml
         -> resource-limit config
         -> $DATASET_DATADIR_GLOBAL
         -> $DATASET_CONTEXT_GLOBAL
         -> $DATASET_COMPUTE_CONFIG (complete source, including pre_runs)
         -> $DATASET_CLIENTS_CONFIG
         -> complete local Docker/OverlayFS config using ORIGINAL_LOWER
         -> complete local compute instance and optional exact-filter override
```

Omit the hosted datadir runner config and reassert Docker, the complete OverlayFS datadir map, and
`data-disk-type: overlayfs` after the source and clients. Do not use
`--debug.stop-after-prerun`, a no-`pre_runs` source copy, or `ADVANCED_LOWER` for this path. When the
protected original is at the raw base head, benchmarkoor replays the pre-run once into the compute
upper before measuring fixtures. When the datadir is already at the pre-run end head, allow the
replay to be skipped only after block, hash, and state root all match the pinned source.

The same compute container and OverlayFS layer then remain active for every selected fixture, as
required by `rollback_strategy: none`. Require exactly one task-owned overlay during the run and
verify it with `verify_task_overlay "$CONTAINER_ID" "$ORIGINAL_LOWER"`. Give every smoke and
measured command a fresh upper; never reuse or promote a smoke upper. A fresh command may therefore
replay the pre-run again, but it must never write to `ORIGINAL_LOWER` or the pristine path.

After the smoke, require the persisted rollback strategy to be `none`, the suite label to be
`test-type: compute`, and the datadir method to be `overlayfs`. The persisted `start_block` is the
head observed before executor-level pre-runs: it must equal the raw tuple when replay was needed, or
the post-pre-run tuple when benchmarkoor verified that replay could be skipped. In the raw case,
require a successful `Pre-run steps completed` record before the first fixture and no failed pre-run
step. A filtered smoke proves configuration and isolation, not the fixture count or state/cache
history of an unfiltered compute run.

### Stage a stateful pre-run once

For stateful, require `rollback_strategy: container-recreate`. If the source has no `pre_runs`, skip
staging and use `ORIGINAL_LOWER` for the measured run. Otherwise, a plain run would restore the raw
lower and replay the bundle for every fixture. Build an immutable advanced baseline:

1. Run the exact stateful source against `ORIGINAL_LOWER` with
   `--debug.stop-after-prerun`. Omit the CPU and memory performance-limit config during this
   unmeasured command. The debug flag deliberately retains the staged datadir and container state
   for inspection; it does not make this a measured run. Keep a controlling shell or exact process
   handle so the command can be waited after the retained container is stopped.
2. Follow the staging log until both `Pre-run steps completed` and the
   `--stop-after-prerun set` record appear. Do not wait for the benchmarkoor process yet. Require
   successful replay to the expected end block/hash/root, resolve the exact retained container and
   logged data mount, then set
   `STAGING_MERGED=$(verify_task_overlay "$CONTAINER_ID" "$ORIGINAL_LOWER")`. Require the logged
   data mount to resolve to `STAGING_MERGED`. From the logged run directory, require the persisted
   Erigon command to contain
   `--override.amsterdam=$EXPECTED_AMSTERDAM_ACTIVATION_TS` exactly.
3. Stop that exact container gracefully and wait until it has exited. This closes container stdio
   so benchmarkoor's following log stream can reach EOF. Record its stopped state and run `sync`,
   but do not remove the container or unmount its overlay yet.
4. Only after the container has stopped, wait for the exact benchmarkoor process. Preserve and
   require its successful exit status and final logs. Require a clean Erigon shutdown with no
   timeout, SIGKILL, or OOM kill and no process or open handle below `STAGING_MERGED`. Preserve the
   stopped container's final state, then remove only that container. Leave the disposable merged
   overlay mounted. If any shutdown or process-exit gate fails, discard the stage instead of
   promoting potentially inconsistent MDBX state.
5. Bind `STAGING_MERGED` to `ADVANCED_LOWER`, remount the bind read-only, and prove a direct
   write is rejected.
6. Canary-test a short-lived OverlayFS over `ADVANCED_LOWER`. The canary must appear only in the
   probe upper, never in either lower, the staging merged path, or the pristine snapshot.

After creating `ADVANCED_LOWER`, never address the writable staging-merged alias from a client or
benchmark command; use it only to verify the bind and later identify the exact cleanup target.

Create a field-for-field copy of the selected test-source map and remove only `pre_runs`. Preserve
its name, labels, fixture URL, fixture subdirectory, rollback strategy, runner settings, and any
other source fields in that copy. Do not try to delete the inherited key with a partial later map.
Deliberately replace its environment-specific runtime and storage-label values with the final local
Docker/OverlayFS config described below. During measured runs, use this complete local source and
`ADVANCED_LOWER`, and restore the resource-limit config.

Do not reuse the earlier State Actor command's `CONTEXT_DIR`, `SUITE_RUNNER_CONFIG`, or clients.
Replace both State Actor datadir configs with `DATASET_DATADIR_GLOBAL`, use
`DATASET_CONTEXT_GLOBAL`, and load `DATASET_CLIENTS_CONFIG`. Omit the hosted datadir runner config.
Use `DATASET_STATEFUL_CONFIG` for staging and its complete no-pre-runs copy for an advanced
baseline. After the dataset context, source, and clients files, load the complete local
Docker/OverlayFS config, then the local instance and optional exact-filter overrides. The final
local config must reassert `container_runtime: docker`, the complete OverlayFS datadir map, and
`data-disk-type: overlayfs`.

The two stateful config stacks are therefore:

```text
staging: configs/global.yaml
         -> $DATASET_DATADIR_GLOBAL
         -> $DATASET_CONTEXT_GLOBAL
         -> $DATASET_STATEFUL_CONFIG (complete source with pre_runs)
         -> $DATASET_CLIENTS_CONFIG
         -> complete local Docker/OverlayFS config using ORIGINAL_LOWER
         -> complete local staging instance

measured: configs/global.yaml
          -> resource-limit config
          -> $DATASET_DATADIR_GLOBAL
          -> $DATASET_CONTEXT_GLOBAL
          -> complete no-pre-runs copy of $DATASET_STATEFUL_CONFIG
          -> $DATASET_CLIENTS_CONFIG
          -> complete local Docker/OverlayFS config using ADVANCED_LOWER
          -> complete local measured instance and optional exact-filter override
```

Use `--debug.stop-after-prerun` only on the staging command. Use the same
`--limit-instance-client` and `--limit-instance-id` on both. Let the smoke create its persisted
`config.json`, require its instance runtime to be `docker` and datadir method to be `overlayfs`,
require the persisted suite summary's `data-disk-type` to be `overlayfs`, and confirm its nonzero
resolved tests before starting the requested measured run. If the requested subset has at most ten
fixtures, it may itself serve as the smoke when it uses this identical final stack and passes every
smoke gate. For an all-fixtures run, remove the smoke filter and pass the archive-count equality
gate above.

### Monitor and clean up

Compute has one disposable per-run upper over `ORIGINAL_LOWER`; it persists and may grow throughout
the suite. Budget at least the original lower's allocated size plus the emergency floor, monitor the
single upper and free space, and stop if another unexplained task overlay appears. After a compute
run, require its container, overlay mount, upper directory, and benchmarkoor process to be gone;
then compare the pristine integrity records through `ORIGINAL_LOWER` before unmounting that guard.

The staged stateful design has one fixed overlay visible at its writable merged mount and read-only
bind, plus at most one disposable per-test overlay. Count unique upper directories or explicitly
exclude the fixed paths; a raw mountpoint count double-counts the bind. Require the benchmarkoor
revision to remove the prior per-test container and mount before creating the next one. Stop
immediately if two per-test uppers persist.

After staging, measure the fixed upper and the complete read-only advanced baseline, then re-check
the writable filesystem. The per-test upper may have to copy any file visible through
`ADVANCED_LOWER`, including files created or enlarged during staging, so the original pristine size
is not a sufficient bound:

```bash
EMERGENCY_FLOOR_BYTES=${EMERGENCY_FLOOR_BYTES:?set the predeclared free-space floor}
STAGING_BASE=$(dirname -- "$(realpath -e -- "$STAGING_MERGED")")
STAGING_UPPER="$STAGING_BASE/upper"

du -sx --block-size=1 "$STAGING_UPPER"
du -sx --apparent-size --block-size=1 "$STAGING_UPPER"
ADVANCED_ALLOCATED_BYTES=$(du -sx --block-size=1 "$ADVANCED_LOWER" | awk '{print $1}')
ADVANCED_APPARENT_BYTES=$(du -sx --apparent-size --block-size=1 \
  "$ADVANCED_LOWER" | awk '{print $1}')
AVAILABLE_BYTES=$(df -B1 --output=avail "$OVERLAY_TMP" | awk 'NR == 2 {print $1}')

for byte_count in "$EMERGENCY_FLOOR_BYTES" "$ADVANCED_ALLOCATED_BYTES" \
  "$ADVANCED_APPARENT_BYTES" "$AVAILABLE_BYTES"; do
  case "$byte_count" in
    ''|*[!0-9]*) exit 2 ;;
  esac
done
REQUIRED_BYTES=$((ADVANCED_ALLOCATED_BYTES + EMERGENCY_FLOOR_BYTES))
test "$AVAILABLE_BYTES" -ge "$REQUIRED_BYTES" || exit 1
printf 'advanced allocated=%s apparent=%s available=%s required=%s\n' \
  "$ADVANCED_ALLOCATED_BYTES" "$ADVANCED_APPARENT_BYTES" \
  "$AVAILABLE_BYTES" "$REQUIRED_BYTES"
```

Measure this gate after the fixed staging upper exists, so `AVAILABLE_BYTES` already accounts for
it. The emergency floor must also cover result, cache, log, and operating headroom. During measured
runs, monitor free space and the aggregate size of both the fixed staging upper and the current
per-test upper; accounting for only the per-test layer understates peak consumption.

After stateful result validation, require zero per-test mounts, directories, containers, and
benchmarkoor processes. Compare pristine fingerprints, sizes, path count, and critical hashes while
`ORIGINAL_LOWER` is still mounted. Then unmount `ADVANCED_LOWER`, unmount and remove the exact
disposable staging tree, and unmount `ORIGINAL_LOWER` last. Require zero task overlays and no canary
in the pristine snapshot.

If a measured run is interrupted, do not use `benchmarkoor cleanup --force` while the staged
baseline is retained. Stop the exact recorded benchmarkoor PID and its exact client container,
wait for both to exit, and require no open handle before unmounting anything. Then resolve and
remove only that run's disposable per-test mount and directory. Preserve the fixed staging mount
and `ADVANCED_LOWER`, re-prove both lower binds read-only, and repeat the smoke gates before
resuming. If staging itself is interrupted, shutdown was not clean, or the mapping of a mount to
its upper is uncertain, stop every exact task-owned container first so its log stream can drain,
then wait for or stop its exact benchmarkoor process. After all have exited, unmount in the cleanup
order above, discard only the verified task run root, and restage from `ORIGINAL_LOWER`; never guess
with a broad mount, process, or cleanup target.

`/home/erigon/jochemnet/erigon_snapshot_pruned` is designated pristine. At benchmarkoor-tests
commit `6805e129a92d368f201f41197c80c5208d7940dc` and benchmarkoor
`v0.1.0-201-g1e0b9d4`, the hosted
[`glamsterdam-devnet-7` compute run](https://benchmarkoor.core.ethpandaops.io/runs/1787170961_518a2c68_erigon-bal-full)
used `rollback_strategy: none`, resolved suite hash `97abdb9608284004`, and passed all 4,884 cases
from the
[jochemnet compute GitHub release asset](https://github.com/ethpandaops/benchmarkoor-tests/releases/download/eest-payloads-jochemnet-v1-amsterdam-compute-46d56762-4525339a-r2/eest-payloads-jochemnet-v1-amsterdam-compute-geth.tar.gz).
Its schelk datadir was already at pre-run end block 24,410,463, hash
`0x4525339a4634947f0b81d111728a7252542f21419f26d7f0eee8f76a36102c4e`, and state root
`0xd46c2706dfd14d0f45bda68fd8d5277b31e2f072e7a0096f78a50aa388ac3c6a`, so benchmarkoor verified
the tuple and skipped the 10,062,313,486-byte pre-run. A raw local snapshot at block 24,402,727 must
instead replay that pre-run once inside its disposable compute overlay.

A validated `glamsterdam-devnet-7` stateful run downloaded its 1,463-case archive from the
[jochemnet stateful GitHub release asset](https://github.com/ethpandaops/benchmarkoor-tests/releases/download/eest-payloads-jochemnet-v1-amsterdam-stateful-d9ad55b3-20260807-000744/eest-payloads-jochemnet-v1-amsterdam-stateful-geth.tar.gz),
found raw head 24,402,727, staged to head 24,410,463, and selected 18 `ether_transfers` fixtures at
300M gas with an optional filter. Treat that URL, count, heads, and filtered result as
revision-specific evidence and repeat every verification.
