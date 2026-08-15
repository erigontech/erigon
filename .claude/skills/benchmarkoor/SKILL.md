---
name: benchmarkoor
description: Build pristine State Actor datadirs and run repeatable benchmarkoor performance benchmarks against a locally built Erigon image using protected native Linux OverlayFS. Use for first-time benchmarkoor setup, Glamsterdam compute or stateful repricing suites, local client image overrides, pristine-state protection, result extraction, before/after MGas/s comparisons, and cross-client ranking tables.
---

# Benchmarkoor

Run benchmarkoor on Linux with native OverlayFS without mutating the pristine State Actor datadir.
Put a kernel-enforced read-only bind mount in front of the pristine directory and keep all writable
overlay data in a disposable external directory.

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
- Give the completed State Actor directory a name containing `pristine` and never use it as a
  `direct` runner datadir.
- Use `method: overlayfs` for these Linux benchmark hosts.
- Configure `source_dir` to a verified read-only bind mount of the pristine datadir, never to the
  original writable path.
- Keep OverlayFS `upperdir`, `workdir`, and `merged` paths outside the pristine and lower trees.
- Verify the lower mount is `ro` before startup and remains `ro` throughout the run.
- Treat every other datadir method as an alternative that requires an explicit user request; never
  inherit an upstream datadir default silently.
- Do not stop unrelated workloads merely to improve benchmark isolation. Report contention and
  ask before changing out-of-scope processes.

## Discover paths and pin revisions

Define task-specific paths after inspecting the host:

```bash
BENCHMARKOOR_SRC=/absolute/path/to/benchmarkoor
TESTS_DIR=/absolute/path/to/benchmarkoor-tests
ERIGON_WT=/absolute/path/to/erigon-worktree
STATE_ROOT=/absolute/path/to/state-actor-v1-pristine
PRISTINE_DIR="$STATE_ROOT/erigon"
LOWER_ROOT=/absolute/path/to/state-actor-v1-lower-ro
OVERLAY_TMP=/absolute/path/to/overlay-runtime
BENCHMARKOOR_BIN="$BENCHMARKOOR_SRC/bin/benchmarkoor"
INSTANCE_ID=erigon-bal-full
IMAGE_TAG=erigon-local:glamsterdam-devnet-7-<short-commit>
```

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
and fixture URL because the repository can advance without changing the suite that ran. Do not
silently move the worktree or relabel an already-produced result.

## Build benchmarkoor

```bash
cd "$BENCHMARKOOR_SRC"
make build-core
"$BENCHMARKOOR_BIN" version
```

Use the source build because datadir handling, result formats, and State Actor orchestration change
across benchmarkoor revisions.

Before a long suite whose rollback strategy is `container-recreate`, verify the selected
benchmarkoor revision eagerly releases the previous datadir after removing its container. At
benchmarkoor `709ad43`, cleanup stayed deferred until process teardown, so a stateful run retained
one OverlayFS mount and upper layer per completed fixture. Local patch `ea92a86` was validated to
release each previous mount before preparing the next; use an upstream equivalent when available.
Do not infer safety from a successful short command alone: smoke-test the actual binary and assert
the live overlay count never exceeds one.

## Generate a separate pristine State Actor datadir

Stop if `$PRISTINE_DIR` already exists and its ownership is unclear. Choose a new path rather than
using `--force` against a valuable snapshot.

Append a local override after all upstream configs:

```yaml
global:
  env:
    STATE_DIR: /absolute/path/to/state-actor-v1-pristine

runner:
  container_runtime: docker
  live_reporting:
    enabled: false
```

Do not put `runner.client.datadirs` in this build-only override. Datadir isolation is a run-time
concern, and the later run-only override supplies the protected OverlayFS source.

From the benchmarkoor-tests repository, build only Erigon:

```bash
cd "$TESTS_DIR"
"$BENCHMARKOOR_BIN" build \
  --config configs/global.yaml \
  --config configs/datadirs/state-actor/v1/global.yaml \
  --config configs/datadirs/state-actor/v1/builder.yaml \
  --config local-overrides.yaml \
  --limit-state-actor-target=erigon \
  --rebuild-on-diff
```

State Actor can exceed its final-size projection while creating `streamsort-*` intermediates and
explicit templates. Reserve peak working headroom and trust only the completed filesystem
measurement after temporary data is cleaned up. One completed v1 build peaked near 1,056.5 GiB and
settled at 516.83 GiB; treat that 2.04x ratio as capacity evidence, not a bound. Wait for a successful
exit and inspect CPU and disk activity instead of treating a quiet finalization phase or ETA as a
failure.

After success, inspect:

```bash
jq . "$PRISTINE_DIR/state-actor-manifest.json"
jq . "$PRISTINE_DIR/.benchmarkoor-build.json"
find "$PRISTINE_DIR" -maxdepth 1 -type f -printf '%f %s bytes\n' | sort
```

The manifest records State Actor metadata and the resolved builder-image digest; the sidecar records
benchmarkoor's rebuild fingerprint. Do not rerun State Actor against this path after declaring it
pristine.

## Measure pristine and overlay capacity

Measure allocated and apparent sizes because large database files may be sparse, then check the
writable filesystem that will hold the overlay upper layer:

```bash
du -sx --block-size=1 "$PRISTINE_DIR"
du -sx --apparent-size --block-size=1 "$PRISTINE_DIR"
df -B1 --output=size,used,avail,target "$PRISTINE_DIR" "$OVERLAY_TMP"
findmnt -T "$PRISTINE_DIR"
findmnt -T "$OVERLAY_TMP"
```

Do not assume OverlayFS needs a second full copy for this workload, but do not assume the upper
will stay small either. Before the first run, treat the pristine datadir's allocated size as a
conservative copy-up budget and require operational headroom beyond it. During the run, measure the
actual upper layer and stop before the filesystem becomes critically full. A completed 4,773-test
compute run observed about 1.8 GiB of upper-layer data over a 516.83 GiB lower; this is evidence that
2x was unnecessary for that run, not a future storage bound.

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
FROM ethpandaops/erigon:glamsterdam-devnet-7@sha256:<resolved-base-digest>
COPY --chown=0:0 erigon /usr/local/bin/erigon
```

Build and verify that the image contains the byte-identical local binary:

```bash
docker build -t "$IMAGE_TAG" /absolute/path/to/overlay-context
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

For Glamsterdam devnet 7, the override should have this shape:

```yaml
global:
  env:
    STATE_DIR: /absolute/path/to/state-actor-v1-lower-ro

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
        source_dir: ${STATE_DIR}/erigon
        method: overlayfs
  instances:
    - id: erigon-bal-full
      client: erigon
      image: erigon-local:glamsterdam-devnet-7-<short-commit>
      pull_policy: never
      metadata:
        labels:
          bal-mode: full
      extra_args:
        - --override.amsterdam=1
        - --fcu.background.commit=false
        - --exec.no-merge=true
        - --exec.no-prune=true
        - --exec.no-background-maintenance
        - --experimental.parallel-commitment
      bootstrap_fcu:
        enabled: true
        max_retries: 120
        backoff: 30s
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
sudo mkdir -p "$LOWER_ROOT/erigon" "$OVERLAY_TMP"
sudo mount --bind "$PRISTINE_DIR" "$LOWER_ROOT/erigon"
sudo mount -o remount,bind,ro "$LOWER_ROOT/erigon"
findmnt -n -o SOURCE,OPTIONS "$LOWER_ROOT/erigon"
```

Require `ro` as a distinct mount option before continuing. Record allocated/apparent size, critical
file hashes, and a metadata fingerprint that covers every path's type, size, mtime, ctime, mode,
owner, group, and symlink target:

```bash
pristine_fingerprint() {
  find "$PRISTINE_DIR" -xdev \
    -printf '%P\0%y\0%s\0%T@\0%C@\0%m\0%u\0%g\0%l\0' |
    LC_ALL=C sort -z |
    sha256sum
}

du -sx --block-size=1 "$PRISTINE_DIR"
du -sx --apparent-size --block-size=1 "$PRISTINE_DIR"
sha256sum \
  "$PRISTINE_DIR/state-actor-manifest.json" \
  "$PRISTINE_DIR/.benchmarkoor-build.json" \
  "$PRISTINE_DIR/chainspec.json" \
  "$PRISTINE_DIR/genesis.json" \
  "$PRISTINE_DIR/nodekey"
pristine_fingerprint
```

Before the real run, manually mount a small probe overlay with this read-only bind as `lowerdir`.
Create a canary through the merged path and require it to appear in `upperdir` but never in either
the bind mount or pristine directory:

```bash
PROBE_ROOT=$(mktemp -d "$OVERLAY_TMP/benchmarkoor-overlay-probe.XXXXXX")
CANARY=.benchmarkoor-overlay-canary
mkdir "$PROBE_ROOT/upper" "$PROBE_ROOT/work" "$PROBE_ROOT/merged"

(
  set -euo pipefail
  trap 'sudo umount "$PROBE_ROOT/merged" 2>/dev/null || true' EXIT
  sudo mount -t overlay overlay \
    -o "lowerdir=$LOWER_ROOT/erigon,upperdir=$PROBE_ROOT/upper,workdir=$PROBE_ROOT/work" \
    "$PROBE_ROOT/merged"
  sudo touch "$PROBE_ROOT/merged/$CANARY"
  test -e "$PROBE_ROOT/upper/$CANARY"
  test ! -e "$LOWER_ROOT/erigon/$CANARY"
  test ! -e "$PRISTINE_DIR/$CANARY"
  sudo umount "$PROBE_ROOT/merged"
  trap - EXIT
)
```

Inspect and remove only this verified temporary probe tree afterward. Keep `upperdir` and `workdir`
together on the same writable filesystem.

Use the run-only override above so benchmarkoor writes its disposable overlay under `OVERLAY_TMP`
and reads from the protected bind. Keep the explicit `data-disk-type: overlayfs` label because an
upstream runner config may otherwise misclassify published results.

Run a 5-10-fixture smoke subset with the exact binary, config stack, image, and State Actor lower
before either full suite. During smoke and production runs, periodically require the lower
mount to remain `ro`, monitor free space, measure the current `upper` directory, and count matching
OverlayFS mounts. Zero is valid during the handoff between fixtures; more than one means cleanup is
lagging and the run must be stopped before mounts and copied-up data accumulate. For a long run,
capture the exact benchmarkoor PID and use a separate free-space watchdog that sends `SIGINT` only
when a predeclared floor is crossed. Never use a broad process match as the kill target.

After benchmarkoor exits, require zero matching overlay mounts, temporary overlay directories,
containers, and benchmarkoor processes. Eager cleanup can leave old deferred container callbacks;
`No such container` warnings are harmless only when the run exited successfully and all of those
postconditions pass. Compare the full-tree metadata fingerprint, sizes, and critical hashes before
and after while the read-only guard is still mounted. Unmount only the guard after those checks
pass. Never unmount or remove the pristine directory itself.

## Choose and run compute or stateful

Read [Compute and stateful suites](references/compute-stateful-suites.md) before selecting or
filtering a suite. It defines how to discover the current runner files, distinguish the adjacent
`glamsterdam-devnet-7-full` context, validate fixture provenance, assemble the ordered config stack,
and check the completed result.

The essential runtime distinction is:

- Compute uses `test-source.compute.runner.yaml` and normally `rollback_strategy: none`; one
  disposable overlay serves the run.
- Stateful uses `test-source.stateful.runner.yaml` and normally
  `rollback_strategy: container-recreate`; each fixture must start from the protected lower and the
  previous container and overlay must be released before the next one.

Never infer the suite from `fixtures_subdir`: both can say `blockchain_tests_stateful_engine`.
Require the selected file's `test-type`, suite name, fixture URL, and rollback strategy to agree.
Run benchmarkoor as root or through an approved privileged wrapper because native OverlayFS needs
mount operations; Docker-group membership alone is insufficient.

## Validate and summarize results

Find the newest run and confirm counts:

```bash
find "$TESTS_DIR/results/runs" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %f\n' |
  sort -nr | head
RUN_DIR="$TESTS_DIR/results/runs/<run-id>"
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
jq '.metadata.labels' "$TESTS_DIR/results/suites/$SUITE_HASH/summary.json"
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
- selected compute/stateful runner, fixture URL, suite filter/test count, run ID, pass/fail count,
  and results path;
- effective datadir method and published `data-disk-type` label;
- effective memory bytes, swap policy, CPU IDs, physical-core mapping, and frequency-control result;
- whether an authoritative branch advanced after launch and, if so, both the run and handoff tips;
- any host contention or fidelity caveat.
