---
name: benchmarkoor
description: Run repeatable benchmarkoor performance benchmarks against Erigon on current and future devnets, using protected native Linux OverlayFS with either pristine State Actor datadirs or pre-populated snapshots. Use for generic benchmark testing, compute or stateful suites, first-time setup, local client image overrides, pristine-state protection, result extraction, before/after MGas/s comparisons, and cross-client ranking tables.
---

# Benchmarkoor

Run benchmarkoor on Linux with native OverlayFS without mutating the pristine State Actor datadir
or pre-populated snapshot. Put a kernel-enforced read-only bind mount in front of the pristine
directory and keep all writable overlay data in a disposable external directory.

## Read current upstream material

Consult these sources before adapting a current suite:

- [Local benchmarking guideline](https://hackmd.io/@QmVpC8TxQ8a1nTCW46EsEQ/SygwzXUrMl)
- [benchmarkoor configuration reference](https://github.com/ethpandaops/benchmarkoor/blob/master/docs/configuration.md)
- [benchmarkoor-tests configs](https://github.com/ethpandaops/benchmarkoor-tests)

Record the benchmarkoor, benchmarkoor-tests, Erigon, and State Actor image revisions. Do not assume
rolling branches, fixture URLs, or image tags still identify the same code.

## Preserve these invariants

- Obey the repository's `AGENTS.md`. For Erigon work, build from a dedicated Git worktree rather
  than the primary checkout.
- Designate the completed State Actor directory or pre-populated snapshot as pristine and never use
  it as a `direct` runner datadir.
- Use `method: overlayfs` for these Linux benchmark hosts.
- Configure `source_dir` to a verified read-only bind mount of the pristine datadir, never to the
  original writable path.
- Perform recursive inspection, size measurement, and hashing through that read-only bind. Reading
  files through the writable path can update access times on a `relatime` filesystem.
- Keep OverlayFS `upperdir`, `workdir`, and `merged` paths outside the pristine and lower trees.
- Verify the lower mount is `ro` before startup and remains `ro` throughout the run.
- Treat every other datadir method as an alternative that requires an explicit user request; never
  inherit an upstream datadir default silently.
- Do not stop unrelated workloads merely to improve benchmark isolation. Report contention and
  ask before changing out-of-scope processes.

## Discover paths and pin revisions

Resolve these common inputs from the current host and requested suite. They are required inputs,
not defaults copied from a previous machine:

If checkout paths were not supplied, inspect only the task-approved workspace roots, identify Git
repositories from their remotes and contents rather than their directory names, and clone a missing
benchmarkoor or benchmarkoor-tests repository into a new task-owned path. Use
`git rev-parse --show-toplevel`, `git remote -v`, and the expected `go.mod` or `configs/` layout to
validate each candidate. Do not search the host for datadirs.

```bash
BENCHMARKOOR_SRC=${BENCHMARKOOR_SRC:?set the benchmarkoor checkout}
TESTS_DIR=${TESTS_DIR:?set the benchmarkoor-tests checkout}
ERIGON_WT=${ERIGON_WT:?set the dedicated Erigon worktree}
CONTEXT_NAME=${CONTEXT_NAME:?set the exact context directory name}
SUITE_KIND=${SUITE_KIND:?set to compute or stateful}
RUN_ROOT=${RUN_ROOT:?set a new task-owned runtime directory}

BENCHMARKOOR_SRC=$(realpath -e -- "$BENCHMARKOOR_SRC")
TESTS_DIR=$(realpath -e -- "$TESTS_DIR")
ERIGON_WT=$(realpath -e -- "$ERIGON_WT")
RUN_ROOT=$(realpath -m -- "$RUN_ROOT")
LOWER_DIR="$RUN_ROOT/lower-ro"
OVERLAY_TMP="$RUN_ROOT/overlay-runtime"
CACHE_DIR="$RUN_ROOT/cache"
RESULTS_DIR="$RUN_ROOT/results"
BENCHMARKOOR_BIN="$BENCHMARKOOR_SRC/bin/benchmarkoor"
ERIGON_SHORT_COMMIT=$(git -C "$ERIGON_WT" rev-parse --short=12 HEAD)
IMAGE_TAG="erigon-local:$CONTEXT_NAME-$ERIGON_SHORT_COMMIT"
```

Use the exact context name requested by the user, such as a future devnet directory; do not select
a similarly named `-full` or older devnet. For a State Actor build, choose a new `STATE_ROOT` and
set `PRISTINE_DIR="$STATE_ROOT/erigon"`. For a pre-populated run, require the operator to designate
the existing `PRISTINE_DIR`; never infer which valuable datadir is pristine by scanning the host.
Canonicalize it with `realpath -e` before creating mounts. Require `RUN_ROOT`, cache, results,
OverlayFS writable layers, and build contexts to be outside the pristine tree and every protected
lower. Stop on path overlap instead of silently choosing another directory.

Before selecting a base image or writing an override, follow
[Discover the current files](references/compute-stateful-suites.md#discover-the-current-files) to
resolve the exact runner, context globals, clients file, and instance from the checked-out revision.

Verify repository state and record commits:

```bash
git -C "$BENCHMARKOOR_SRC" status --short --branch
git -C "$BENCHMARKOOR_SRC" rev-parse HEAD
git -C "$TESTS_DIR" status --short --branch
git -C "$TESTS_DIR" rev-parse HEAD
git -C "$ERIGON_WT" status --short --branch
git -C "$ERIGON_WT" rev-parse HEAD
```

Fetch the authoritative Erigon and benchmarkoor-tests branches immediately before creating the
worktree and resolving the suite, then record both commits. Re-fetch after a long run. If a remote
tip advanced, report both hashes and describe the intervening changes; call the result "latest at
launch," not the current tip. For benchmarkoor-tests, also compare the selected runner-config blob
and fixture source because the repository can advance without changing the suite that ran. Do not
silently move the worktree or relabel an already-produced result.

## Build benchmarkoor

```bash
cd "$BENCHMARKOOR_SRC"
make build-core
"$BENCHMARKOOR_BIN" version
```

Use the source build because datadir handling, result formats, pre-run handling, and State Actor
orchestration change across benchmarkoor revisions.

Before a long suite whose rollback strategy is `container-recreate`, verify the selected
benchmarkoor revision eagerly releases the previous datadir after removing its container. At
benchmarkoor `709ad43`, cleanup stayed deferred until process teardown, so a stateful run retained
one OverlayFS mount and upper layer per completed fixture. Local patch `ea92a86` was validated to
release each previous mount before preparing the next; use an upstream equivalent when available.
Do not infer safety from a successful short command alone: smoke-test the actual binary and assert
the live disposable-overlay count never exceeds one. A pre-populated run may also retain the one
fixed staging overlay described below; do not count that fixed upper as a per-test upper.

## Generate a separate pristine State Actor datadir

```bash
STATE_ROOT=${STATE_ROOT:?set a new State Actor output root}
STATE_ROOT=$(realpath -m -- "$STATE_ROOT")
PRISTINE_DIR="$STATE_ROOT/erigon"
```

Stop if `$PRISTINE_DIR` already exists and its ownership is unclear. Choose a new path rather than
using `--force` against a valuable snapshot.

Append a local override after all upstream configs:

```yaml
global:
  env:
    STATE_DIR: /absolute/path/to/new-state-actor-root

runner:
  container_runtime: docker
  live_reporting:
    enabled: false
```

Do not put `runner.client.datadirs` in this build-only override. Datadir isolation is a run-time
concern, and the later run-only override supplies the protected OverlayFS source.

From the benchmarkoor-tests repository, build only Erigon:

```bash
rg --files "$TESTS_DIR/configs/datadirs/state-actor" |
  rg '/(global|builder)\.ya?ml$' |
  sort

STATE_ACTOR_GLOBAL_CONFIG=${STATE_ACTOR_GLOBAL_CONFIG:?select the matching global config}
STATE_ACTOR_BUILDER_CONFIG=${STATE_ACTOR_BUILDER_CONFIG:?select the matching builder config}
STATE_ACTOR_BUILD_OVERRIDE=${STATE_ACTOR_BUILD_OVERRIDE:?set the build-only local override}
GLOBAL_CONFIG=$(realpath -e -- "$TESTS_DIR/configs/global.yaml")
STATE_ACTOR_GLOBAL_CONFIG=$(realpath -e -- "$STATE_ACTOR_GLOBAL_CONFIG")
STATE_ACTOR_BUILDER_CONFIG=$(realpath -e -- "$STATE_ACTOR_BUILDER_CONFIG")
STATE_ACTOR_BUILD_OVERRIDE=$(realpath -e -- "$STATE_ACTOR_BUILD_OVERRIDE")

cd "$TESTS_DIR"
"$BENCHMARKOOR_BIN" build \
  --config "$GLOBAL_CONFIG" \
  --config "$STATE_ACTOR_GLOBAL_CONFIG" \
  --config "$STATE_ACTOR_BUILDER_CONFIG" \
  --config "$STATE_ACTOR_BUILD_OVERRIDE" \
  --limit-state-actor-target=erigon \
  --rebuild-on-diff
```

State Actor can exceed its final-size projection while creating `streamsort-*` intermediates and
explicit templates. Reserve peak working headroom and trust only the completed filesystem
measurement after temporary data is cleaned up. One historical build peaked near 1,056.5 GiB and
settled at 516.83 GiB; treat that 2.04x ratio as capacity evidence, not a bound. Wait for a successful
exit and inspect CPU and disk activity instead of treating a quiet finalization phase or ETA as a
failure.

After success, canonicalize and recheck the path boundary before installing the read-only bind:

```bash
PRISTINE_DIR=$(realpath -e -- "$PRISTINE_DIR")
case "$RUN_ROOT/" in "$PRISTINE_DIR/"*) exit 1 ;; esac
case "$PRISTINE_DIR/" in "$RUN_ROOT/"*) exit 1 ;; esac
```

Install the bind in the protection section before inspecting the completed tree. Once that section
has set `INTEGRITY_ROOT`, inspect only through that path:

```bash
jq . "$INTEGRITY_ROOT/state-actor-manifest.json"
jq . "$INTEGRITY_ROOT/.benchmarkoor-build.json"
find "$INTEGRITY_ROOT" -maxdepth 1 -type f -printf '%f %s bytes\n' | sort
```

The manifest records State Actor metadata and the resolved builder-image digest; the sidecar records
benchmarkoor's rebuild fingerprint. Do not rerun State Actor against this path after declaring it
pristine.

## Measure pristine and overlay capacity

First identify the filesystems containing the pristine tree and writable overlay. Defer recursive
allocated/apparent-size measurement until the read-only bind in the protection section is mounted;
large database files may be sparse, and reading them through the writable path can update atime:

```bash
df -B1 --output=size,used,avail,target "$PRISTINE_DIR" "$OVERLAY_TMP"
findmnt -T "$PRISTINE_DIR"
findmnt -T "$OVERLAY_TMP"
```

Do not assume OverlayFS needs a second full copy for this workload, but do not assume the upper
will stay small either. Before the first run, treat the pristine datadir's allocated size as a
conservative copy-up budget and require operational headroom beyond it. During the run, measure the
actual upper layer and stop before the filesystem becomes critically full. A historical full
compute run observed about 1.8 GiB of upper-layer data over a 516.83 GiB lower; this is capacity
evidence, not a bound for a different source or client revision.

## Build an image from the exact Erigon worktree

Build the binary from the exact commit under test:

```bash
cd "$ERIGON_WT"
make erigon
./build/bin/erigon --version
sha256sum ./build/bin/erigon
```

When a compatible image for the same branch already exists, a small overlay image avoids rebuilding
the unchanged container environment. Create a dedicated build-context directory containing the
built `erigon` binary and this Dockerfile:

```dockerfile
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY --chown=0:0 erigon /usr/local/bin/erigon
```

Set `BASE_IMAGE` to the selected context's compatible client image pinned as
`repository@sha256:digest`; obtain it from the effective selected instance, not from a prior
devnet. Build and verify that the image contains the byte-identical local binary:

```bash
BASE_IMAGE=${BASE_IMAGE:?set the selected client base image pinned by digest}
case "$BASE_IMAGE" in *@sha256:*) ;; *) exit 2 ;; esac
IMAGE_CONTEXT=${IMAGE_CONTEXT:?set the task-owned image build context}
IMAGE_CONTEXT=$(realpath -e -- "$IMAGE_CONTEXT")
docker build --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$IMAGE_TAG" "$IMAGE_CONTEXT"
docker run --rm "$IMAGE_TAG" --version
docker run --rm --entrypoint sha256sum "$IMAGE_TAG" /usr/local/bin/erigon
```

Resolve and pin the base-image digest. If the branch changes the container environment or is
incompatible with that base, build the repository's complete Dockerfile instead. Do not rely on a
rolling remote image's Erigon binary when benchmarking a local branch.

## Create a run-only override

Copy the complete chosen instance from the current `clients.yaml` into the last override, then
change its image and pull policy. Viper replaces lists rather than merging list entries, so an
override containing `runner.instances` replaces the entire upstream instance list.

For the selected context, the override should have this shape. Replace every placeholder from the
resolved context and the complete selected instance; do not copy values from an older devnet:

```yaml
global:
  env:
    ERIGON_DATADIR_LOWER: /absolute/path/to/read-only-lower

runner:
  container_runtime: docker
  live_reporting:
    enabled: false
  directories:
    tmp_datadir: /absolute/path/to/overlay-runtime
  benchmark:
    results_owner: "<invoking-uid>:<invoking-gid>"
    tests:
      metadata:
        labels:
          data-disk-type: overlayfs
          erigon-commit: <full-erigon-commit>
  client:
    datadirs:
      erigon:
        source_dir: ${ERIGON_DATADIR_LOWER}
        method: overlayfs
  instances:
    - id: <selected-instance-id>
      client: erigon
      image: <local-image-tag>
      pull_policy: never
      # Copy every other field from the selected current instance.
```

Re-copy the instance when upstream `clients.yaml` changes; do not let this example silently erase
new required flags. Check every copied `extra_args` option against `docker run --rm "$IMAGE_TAG"
--help` before starting the long suite.

For current Erigon, `--experimental.parallel-commitment` is the CLI equivalent of
`COMMITMENT_PARALLEL=true`: it sets `ExperimentalParallelCommitment` and selects the parallel hex
Patricia/`ParallelPatriciaHashed` commitment variant. Recheck that mapping in the exact source under
test and confirm the persisted container command includes the flag.

Verify the configured CPU IDs map to the intended number of distinct physical cores:

```bash
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
```

Do not infer topology from consecutive CPU numbers or from a config comment.

## Protect the pristine datadir with OverlayFS

Put a kernel-enforced read-only bind mount in front of the pristine datadir rather than configuring
the original path directly:

```bash
sudo mkdir -p "$LOWER_DIR" "$OVERLAY_TMP"
sudo mount --bind "$PRISTINE_DIR" "$LOWER_DIR"
sudo mount -o remount,bind,ro "$LOWER_DIR"
findmnt -n -o SOURCE,OPTIONS "$LOWER_DIR"
INTEGRITY_ROOT="$LOWER_DIR"
```

Require `ro` as a distinct mount option before continuing. Record allocated/apparent size,
dataset-appropriate critical file hashes, and a metadata fingerprint that covers every path's
type, size, atime, mtime, ctime, mode, owner, group, and symlink target. From this point onward, run
every recursive read and content hash through `INTEGRITY_ROOT`, never through `PRISTINE_DIR`. The
State Actor files below are examples, not a universal list for pre-populated snapshots:

```bash
pristine_fingerprint() {
  local integrity_root=${1:?set integrity root to the read-only bind}
  find "$integrity_root" -xdev \
    -printf '%P\0%y\0%s\0%A@\0%T@\0%C@\0%m\0%u\0%g\0%l\0' |
    LC_ALL=C sort -z |
    sha256sum
}

du -sx --block-size=1 "$INTEGRITY_ROOT"
du -sx --apparent-size --block-size=1 "$INTEGRITY_ROOT"
sha256sum \
  "$INTEGRITY_ROOT/state-actor-manifest.json" \
  "$INTEGRITY_ROOT/.benchmarkoor-build.json" \
  "$INTEGRITY_ROOT/chainspec.json" \
  "$INTEGRITY_ROOT/genesis.json" \
  "$INTEGRITY_ROOT/nodekey"
pristine_fingerprint "$INTEGRITY_ROOT"
```

Before the real run, manually mount a small probe overlay with this read-only bind as `lowerdir`.
Create a canary through the merged path and require it to appear in `upperdir` but never in the
bind mount. Because the bind and pristine path expose the same underlying tree, inspect only the
read-only bind:

```bash
PROBE_ROOT=$(mktemp -d "$OVERLAY_TMP/benchmarkoor-overlay-probe.XXXXXX")
CANARY=.benchmarkoor-overlay-canary
mkdir "$PROBE_ROOT/upper" "$PROBE_ROOT/work" "$PROBE_ROOT/merged"

(
  set -euo pipefail
  trap 'sudo umount "$PROBE_ROOT/merged" 2>/dev/null || true' EXIT
  sudo mount -t overlay overlay \
    -o "lowerdir=$LOWER_DIR,upperdir=$PROBE_ROOT/upper,workdir=$PROBE_ROOT/work" \
    "$PROBE_ROOT/merged"
  sudo touch "$PROBE_ROOT/merged/$CANARY"
  test -e "$PROBE_ROOT/upper/$CANARY"
  test ! -e "$LOWER_DIR/$CANARY"
  sudo umount "$PROBE_ROOT/merged"
  trap - EXIT
)
```

Inspect and remove only this verified temporary probe tree afterward. Keep `upperdir` and `workdir`
together on the same writable filesystem.

Use the run-only override above so benchmarkoor writes its disposable overlay under `OVERLAY_TMP`
and reads from the protected bind. Keep the explicit `data-disk-type: overlayfs` label because an
upstream runner config may otherwise misclassify published results.

Run a 5-10-fixture smoke subset with the exact binary, config stack, image, and protected lower
before either full suite. During smoke and production runs, periodically require the lower
mount to remain `ro`, monitor free space, measure the current `upper` directory, and count matching
OverlayFS mounts. Zero is valid during the handoff between fixtures; more than one means cleanup is
lagging and the run must be stopped before mounts and copied-up data accumulate. For a long run,
capture the exact benchmarkoor PID and use a separate free-space watchdog that sends `SIGINT` only
when a predeclared floor is crossed. Never use a broad process match as the kill target.

After benchmarkoor exits, require zero disposable benchmark overlay mounts, temporary overlay
directories, containers, and benchmarkoor processes. A staged stateful pre-populated run retains
its one fixed staging overlay only until post-run validation, then removes it in the order in the
reference. Eager cleanup can leave old deferred container callbacks;
`No such container` warnings are harmless only when the run exited successfully and all of those
postconditions pass. Compare the full-tree metadata fingerprint, sizes, and critical hashes before
and after while the read-only guard is still mounted. Unmount only the guard after those checks
pass. Never unmount or remove the pristine directory itself.

## Reuse a pre-populated snapshot

Read [Pre-populated snapshot workflow](references/compute-stateful-suites.md#pre-populated-snapshot-workflow)
before using an existing bloated or pruned datadir. Require its path to be designated explicitly,
then assign it to `PRISTINE_DIR`. Never configure that raw path directly, run Erigon against it, or
use it as an OverlayFS upper or work directory.

Protect the original with the same read-only bind, canary, fingerprint, size, and critical-hash
checks used for State Actor. Treat download sidecars as hints; verify the live block, hash, and state
root by booting the exact client through a disposable OverlayFS view.

For a compute source with `rollback_strategy: none`, keep its complete source map, including any
`pre_runs`, and run it through one disposable OverlayFS layer over the protected original lower.
Benchmarkoor applies the pre-run at most once in that layer and then retains the same container and
overlay for the suite. Do not use the stateful no-`pre_runs` copy against a raw lower, and do not
promote the compute upper into the pristine snapshot.

If the selected stateful source has `pre_runs`, do not replay that bundle for every
`container-recreate` fixture. Stage it once in a disposable overlay with
`--debug.stop-after-prerun`. After validating the completion marker and head, stop the retained
client gracefully before waiting for benchmarkoor to exit; stopping the container lets its log
stream drain. Then expose the staged merged directory through a second read-only bind. Run smoke
and production fixtures with a fresh per-test OverlayFS layer over that advanced bind and a
complete test-source override that omits `pre_runs`. Never promote into or write back to the
pristine snapshot. Copy the complete source map and remove only `pre_runs`; do not construct a
partial override and rely on merge deletion.

Before stateful smoke or measured fixtures over a staged baseline, recalculate the conservative
per-test copy-up budget from the allocated size of the read-only advanced baseline. The original
pristine size is no longer a safe bound after staging has created or enlarged files.

Treat the dataset datadir global, context global, selected compute or stateful source, and clients
as one pinned set.
Do not reuse the generic State Actor context for a pre-populated dataset; fork variables and client
arguments must come from the matching dataset context.

Do not load a `schelk` runner config and then override only `method`: Viper retains sibling
`schelk_options`, producing an invalid mixed datadir config. Load the dataset's global/genesis
config and supply a complete local OverlayFS datadir map instead. A compute run has one writable
per-run overlay. While a stateful staged baseline is retained, disable broad cleanup-on-start and
monitor one fixed staging overlay plus at most one writable per-test overlay.

## Choose and run compute or stateful

Read [Compute and stateful suites](references/compute-stateful-suites.md) before selecting or
filtering a suite. It defines how to discover the current runner files, distinguish the adjacent
contexts with similar names, validate fixture provenance, assemble the ordered config stack, and
check the completed result.

The selected runner's `runner.benchmark.tests.source.eest_fixtures` map is authoritative for where
fixtures come from. A missing/empty `runner.benchmark.tests.filter` selects every fixture in that
source; add a filter only for an explicitly requested subset and never carry a smoke filter into an
all-fixtures run.

The essential runtime distinction is:

- Compute uses `test-source.compute.runner.yaml` and normally `rollback_strategy: none`; one
  disposable overlay serves the run.
- Stateful uses `test-source.stateful.runner.yaml` and normally
  `rollback_strategy: container-recreate`; each fixture must start from the protected lower and the
  previous container and overlay must be released before the next one.

Never infer the suite from `fixtures_subdir`: both can say `blockchain_tests_stateful_engine`.
Require the selected file's `test-type`, suite name, fixture source, and rollback strategy to agree.
Run benchmarkoor as root or through an approved privileged wrapper because native OverlayFS needs
mount operations; Docker-group membership alone is insufficient.

## Validate and summarize results

Set `RESULTS_DIR` to the exact effective `runner.benchmark.results_dir` of the command being
validated. Do not assume the default `$TESTS_DIR/results`: staging, smoke, and measured commands
may each use a different results root. Find the run only below that configured root and confirm
counts:

```bash
RESULTS_DIR=${RESULTS_DIR:?set to the effective runner.benchmark.results_dir}
RESULTS_DIR=$(realpath -e -- "$RESULTS_DIR")
RUNS_DIR="$RESULTS_DIR/runs"
test -d "$RUNS_DIR"

find "$RUNS_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %f\n' |
  sort -nr | head
RUN_DIR="$RUNS_DIR/<run-id>"
test -f "$RUN_DIR/config.json"
jq '{status, test_counts, suite_hash, instance: {
  id: .instance.id,
  image: .instance.image,
  image_sha256: .instance.image_sha256,
  datadir: .instance.datadir
}}' "$RUN_DIR/config.json"
jq '{
  test_count: (.tests | length),
  tests_with_failed_steps: ([
    .tests | to_entries[] |
    select(any(.value.steps[]; .aggregated.fail > 0))
  ] | length)
}' "$RUN_DIR/result.json"
```

Current benchmarkoor writes run status and pass/fail totals to `config.json`; `result.json` contains
the keyed per-test results. Looking for `tests_total` at the root of `result.json` returns nulls.
During a run, `result.json` can briefly expose a null or shorter `.tests` object while it is being
rewritten. Retry the read and corroborate it with the monotonically increasing `Executing test`
index; use the stable files after process exit for final counts.

Confirm the suite source and effective resource limits from the persisted run, not just the input
YAML:

```bash
rg 'Downloading fixtures tarball|Resource limits configured|CPU frequency info|turbo boost' \
  "$RUN_DIR/benchmarkoor.log"
jq '.instance | {
  rollback_strategy,
  resource_limits,
  image,
  image_sha256,
  datadir
}' "$RUN_DIR/config.json"
SUITE_HASH=$(jq -r .suite_hash "$RUN_DIR/config.json")
SUITE_DIR="$RESULTS_DIR/suites/$SUITE_HASH"
test -f "$SUITE_DIR/summary.json"
jq '.metadata.labels' "$SUITE_DIR/summary.json"
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
```

Count the configured CPU IDs and confirm that their `(socket, core)` pairs are distinct. For a
32 GiB limit, expect `memory_bytes: 34359738368`; also confirm `swap_disabled: true` when required.
CPU pinning and memory enforcement can be correct even when turbo control fails. Treat requested
frequency settings as a separate check: inspect every pinned CPU's reported `scaling_min` and
`scaling_max`, and disclose partial application.

Scan parallel-execution telemetry separately from benchmark pass/fail status, using only one of the
mirrored client logs to avoid double counting:

```bash
rg 'parallel executed.*(abort=[1-9][0-9]*|invalid=[1-9][0-9]*)' \
  "$RUN_DIR/container.log"
```

Associate each match with the latest `Executing test` line. Nonzero `abort` or `invalid` counters
indicate speculative retry work even when the fixture passed.

For local before/after, per-fixture MGas/s, website peer, or PR-style ranking comparisons, read
[Cross-client ranking comparisons](references/ranking-comparisons.md). It defines the raw timing
fields, full-name intersection, weighted aggregates, ranking formulas, and comparability checks.

## Handoff checklist

Report:

- all repository commits and container image digests;
- pristine allocated/apparent sizes and remaining filesystem space;
- protected lower path, disposable upper location, and observed upper-layer peak;
- exact before/after pristine fingerprint and read-only lower-mount proof;
- selected compute/stateful runner, fixture source and digest, suite filter/test count, run ID,
  pass/fail count,
  and results path;
- effective datadir method and published `data-disk-type` label;
- effective memory bytes, swap policy, CPU IDs, physical-core mapping, and frequency-control result;
- whether an authoritative branch advanced after launch and, if so, both the run and handoff tips;
- any host contention or fidelity caveat.
