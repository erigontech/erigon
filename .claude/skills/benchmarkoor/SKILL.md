---
name: benchmarkoor
description: Run benchmarkoor performance benchmarks against a locally-built Erigon binary and produce per-test MGas/s comparison tables. Covers image build, dataset reset, run invocation, result parsing, and before/after comparisons.
allowed-tools: Bash, Read, Write, Edit, Glob, Monitor
---

# Benchmarkoor: per-test throughput benchmarks

Benchmarkoor (`ethpandaops/benchmarkoor`) drives an execution client through the Engine API and measures `engine_newPayloadV<N>` throughput in MGas/s per test (the `V<N>` payload version depends on the dataset's hardfork — V5 for Amsterdam, V4 for Prague, etc.). This skill teaches you to run it against a locally-built Erigon, parse results, and compare two runs.

The skill assumes the host already has the benchmarkoor binary, a config YAML, a snapshot/working datadir pair, and Docker installed — it focuses on the agent workflow, not first-time setup.

## References

(All `$VAR` placeholders used below — `$BENCH_DIR`, `$CONFIG`, `$INSTANCE_ID`, etc. — are defined in the next section, "Adapt to the user's environment first".)

When something in this skill is ambiguous, or for newer/unfamiliar config options, consult these upstream sources before guessing:

- **Skylenet's BAL benchmark runs notes**: <https://notes.ethereum.org/@skylenet/bal-benchmark-runs> — narrative writeup of how the BAL devnet benchmarks were set up; useful for the *intent* behind the suite (which precompiles, why 120M gas, etc.) and for the dataset preparation steps that aren't visible from the YAML alone.
- **Benchmarkoor docs**: <https://github.com/ethpandaops/benchmarkoor/tree/master/docs> — authoritative reference for config schema, CLI flags, and the datadir-method matrix.
- **Example configs**: <https://github.com/ethpandaops/benchmarkoor/tree/master/examples/configuration> — working YAML samples covering different clients, network configs, and datadir methods. When the user wants a config knob you haven't seen before, find the closest example here and adapt rather than guessing.

If you change anything in `$CONFIG` that isn't already in this skill, cross-check it against the docs or examples links above before running.

## Adapt to the user's environment first

Before running anything, identify the host-specific values. Don't hard-code these — different hosts and different datasets use different names. Ask the user if anything below is ambiguous.

| Placeholder | What it is | How to discover |
|---|---|---|
| `$BENCH_DIR` | benchmarkoor host root (contains the binary, config, snapshot dirs) | The binary usually isn't in `$PATH`. Find it with `find ~ /opt /srv -maxdepth 4 -name benchmarkoor -type f -executable 2>/dev/null` and take the parent dir, or ask the user. Common pattern: `~/<dataset-name>/` |
| `$CONFIG` | YAML config file passed to `benchmarkoor run --config <…>` | `ls $BENCH_DIR/*.yaml` — there may be more than one (one per network/dataset). Ask which to use. |
| `$INSTANCE_ID` | The `instances[].id` to benchmark | `grep -E '^\s*-\s+id:' $CONFIG` |
| `$IMAGE_TAG` | Docker image tag the Erigon instance references | Look at `instances[].image` for the chosen `$INSTANCE_ID` (e.g. `erigon-local:traced`) |
| `$SNAPSHOT_DIR` | Read-only pristine dataset; never mutated | Look at `client.datadirs.erigon.source_dir` in the config, or the `PRISTINE=...` line in the reset script |
| `$WORKING_DIR` | Writable copy that benchmarkoor mutates | The `--datadir` benchmarkoor's docker mount uses; see `HYBRID=...` in the reset script or the `source_dir` for `method: direct` datadirs |
| `$RESET_SCRIPT` | Script that re-syncs $SNAPSHOT_DIR → $WORKING_DIR | Typically `reset-hybrid.sh` or `reset-<dataset>.sh` in `$BENCH_DIR`. May not exist if the snapshot/working dirs are the same (then state simply persists across runs). |

Concrete example for one host (perf-devnet-3):
```
$BENCH_DIR     = /home/erigon/perf-devnet-3-erigon-snapshot
$CONFIG        = benchmarkoor.interop.bal.yaml
$INSTANCE_ID   = erigon-bal-full
$IMAGE_TAG     = erigon-local:traced
$SNAPSHOT_DIR  = /home/erigon/perf-devnet-3-erigon-snapshot/erigon_snapshot
$WORKING_DIR   = /home/erigon/perf-devnet-3-erigon-snapshot/erigon_hybrid
$RESET_SCRIPT  = reset-hybrid.sh
```

Don't assume these names elsewhere — discover them per session.

## Datadir setup approaches

Benchmarkoor's source tree (`pkg/datadir/`) exposes five `client.datadirs.<client>.method` values: `copy`, `overlayfs`, `fuse-overlayfs`, `zfs`, `direct`. Only the first four are listed in upstream's `docs/configuration.md`; `direct` is present in the code (`pkg/datadir/direct.go`) but documented in its source as *"not suitable for normal benchmarking … intended for inspection / resume workflows."* Choose based on host filesystem and how clean you need isolation between runs.

### 1. "Hybrid" (= `method: direct` + external reset script) — what we use today

Strictly speaking this is `method: direct` pointing at a pre-prepared writable copy (`erigon_hybrid/`) of a read-only pristine snapshot (`erigon_snapshot/`). A user-owned script (`reset-hybrid.sh`) rsyncs snapshot → working copy between runs, and the read-only `snapshots/` subdir is bind-mounted from the pristine source to avoid duplicating immutable segment files.

- **Pros:** works on any filesystem; no kernel/ZFS features needed; no per-test copy-up cost during execution; full reset is one rsync (~tens of seconds).
- **Cons:** user-managed, not built into benchmarkoor; requires sudo for the reset (containers leave root-owned files in the working dir).
- **Use when:** the host has no ZFS, and you want fast, repeatable, isolated runs without paying overlayfs copy-up at runtime.

Config side: `method: direct` pointing at `$WORKING_DIR`. Reset side: external script before each run.

### 2. `method: overlayfs` — **doesn't work for our Erigon dataset**

Native Linux overlayfs with `$SNAPSHOT_DIR` as the read-only lower layer and a `/tmp/benchmarkoor-overlay-*` upper. Mount itself is instant. The problem is that MDBX's open path touches enough chaindata pages during recovery/steady-sync that the kernel copies up substantial parts of the file to the upper layer, taking minutes. Benchmarkoor's RPC-readiness probe (observed at ~2 minutes; check `pkg/runner/` for the current value) trips, and Erigon gets killed mid-open with `Got interrupt, shutting down...`.

Past evidence in our results dir: runs `1779001673_b1eeb60b_…` and `1779002095_ae096a8b_…` (May 17) — both timed out around the `Opening Database` step.

- **Use when:** working with clients whose datadirs don't trigger heavy copy-up (smaller / less-touched files). For Erigon with the current dataset size, skip it.
- **Possible workaround if you must:** increase whatever timeout benchmarkoor exposes for RPC readiness; we didn't pursue this.

### 3. `method: zfs` — promising once the host has ZFS

ZFS snapshot + clone provides copy-on-write isolation that should avoid the overlayfs copy-up problem because COW is the native operation, not a degraded fallback.

- **Requires:** `$SNAPSHOT_DIR` must live on a ZFS dataset; root or appropriate ZFS delegations.
- **What benchmarkoor does:** snapshots the source dataset, clones the snapshot to a working dataset, mounts that into the container; cleans up the clone after the run.
- **Not yet tested here:** the current host's root FS is ext4, so we never exercised it. When migrating to a ZFS host, this is the path to try first — it should subsume the "hybrid" workflow with no external reset script needed.

### 4. `method: direct` (raw) — **not for benchmarking, and dangerous if pointed at the snapshot**

Mounts `source_dir` directly into the container with no isolation. Whatever Erigon writes persists in `source_dir`. From benchmarkoor's own code comment: *"not suitable for normal benchmarking … intended for inspection / resume workflows."*

- **⚠️ If `source_dir` is the pristine snapshot, it will be irreversibly mutated.** The snapshot is ~2 TB on this host, and re-downloading it takes many hours. There is no automatic backup. Double-check `client.datadirs.erigon.source_dir` in the YAML *before* running with `method: direct` — if it points at `$SNAPSHOT_DIR`, change it or abort.
- **Use only when:** you specifically want to inspect / iterate on the chain state left behind by a prior run, or you're debugging.
- The "hybrid" approach above uses `method: direct` *correctly* because it points at a disposable working copy (`$WORKING_DIR`), not the pristine snapshot. That's the safe pattern.

### (Bonus) `method: copy` and `method: fuse-overlayfs`

Two more methods exist but weren't explored:
- `copy` — parallel file copy of `source_dir` to a fresh working dir each run. Universal but slow for large datadirs.
- `fuse-overlayfs` — userspace overlayfs; documented as ~3× slower than native overlayfs. Fallback when native isn't available.

## Layout convention (typical)

```
$BENCH_DIR/                                            # benchmarkoor host root
├── benchmarkoor                                       # binary (root-owned, requires sudo)
├── $CONFIG                                            # main config (one of possibly several)
├── $RESET_SCRIPT                                      # resets datadir (sudo-only); may not exist
├── $SNAPSHOT_DIR/                                     # read-only pristine dataset (never touched)
├── $WORKING_DIR/                                      # writable copy that benchmarkoor mutates
└── results/runs/                                      # per-run output dirs
    ├── index.json                                     # generated run index
    └── <unix_ts>_<short_hash>_<instance-id>/          # one dir per run

# The Erigon clone (containing build/bin/erigon) typically lives elsewhere on
# the host — sibling of $BENCH_DIR or a separate workspace — and is referenced
# via cp/COPY in Step 1. Don't assume it's under $BENCH_DIR.
```

## Prerequisites (verify before starting)

1. **Erigon binary** built at `$BENCH_DIR/erigon/build/bin/erigon` (or wherever the user's clone lives). If missing, build with `make erigon` from the erigon repo.
2. **Docker image** `$IMAGE_TAG` exists. Confirm with `sudo -n docker images | grep <image-name>`.
3. **Benchmarkoor binary** at `$BENCH_DIR/benchmarkoor`. Requires `sudo -n` to invoke (controls docker, cpuset, cpufreq).
4. **Dataset snapshot** at `$SNAPSHOT_DIR` (untouched) and `$WORKING_DIR` (working copy). `$RESET_SCRIPT` rsyncs the former into the latter.
5. **No conflicting Erigon process** using the benchmark datadir. Check with `pgrep -af "build/bin/erigon"` and stop any local node that has `$WORKING_DIR` as its `--datadir` (many setups have a `stop.sh` next to the datadir; otherwise `pkill -f "datadir.*$WORKING_DIR"`).
6. **No stale benchmarkoor containers** — they don't always get cleaned up on aborted runs. If you see `benchmarkoor-<oldhash>-*` containers still up, run `sudo -n ./benchmarkoor cleanup` (or `sudo -n docker rm -f <container>`) before starting.

## Workflow

### Step 1 — Rebuild Docker image with the new binary

If you've changed Erigon source since the last image was built, you need a fresh image. A full Dockerfile build is slow; use a quick overlay instead:

```bash
mkdir -p /tmp/erigon-img-overlay
cp "$BENCH_DIR/erigon/build/bin/erigon" /tmp/erigon-img-overlay/erigon
# Unquoted EOF on purpose — $IMAGE_TAG must expand into the heredoc.
cat > /tmp/erigon-img-overlay/Dockerfile <<EOF
FROM $IMAGE_TAG
COPY --chown=erigon:erigon erigon /usr/local/bin/erigon
EOF
cd /tmp/erigon-img-overlay && sudo -n docker build -t "$IMAGE_TAG" .
```

This re-tags `$IMAGE_TAG` in under a second. The original Dockerfile multi-stage build is only needed if base layers (OS, deps) changed.

If you've never built the base image, fall back to:
```bash
cd "$BENCH_DIR/erigon" && sudo -n docker build -t "$IMAGE_TAG" .
```

Caveat: the stock Erigon `Dockerfile` may not reproduce the original `$IMAGE_TAG`'s build flags (e.g. tracing builds use extra args). If the original image was built with non-default args, ask the user how it was first built before falling back.

### Step 2 — Reset the working datadir

`$RESET_SCRIPT` (typically) rsyncs `$SNAPSHOT_DIR/` → `$WORKING_DIR/` (excluding the read-only `snapshots/` bind mount). Requires sudo because containers leave root-owned files behind.

```bash
sudo -n bash "$BENCH_DIR/$RESET_SCRIPT"
```

**Always run this before every benchmark run.** Skipping it means leftover state from the previous run pollutes results.

If no reset script exists for this dataset, the user has chosen a setup where state persists across runs — in that case ask whether they want a manual rsync from `$SNAPSHOT_DIR` to `$WORKING_DIR` before starting, or to deliberately run on the prior state.

### Step 3 — Inspect/edit the config

Open `$BENCH_DIR/$CONFIG`. Don't construct YAML from scratch — read the existing file and edit it in place; the snippet below shows the *knobs* you'll likely touch, not the full schema. (Full schema in the upstream `examples/configuration/` reference.) Key knobs:

```yaml
runner:
  benchmark:
    tests:
      # The filter regex picks which tests run. Edit alternations to add/remove
      # tests; edit the size suffix (e.g. 120M / 60M) to change gas budgets per test.
      filter: 'regex:__test_(<test_name_1>|<test_name_2>|...)\[.*benchmark_<size>M\]'

  client:
    config:
      resource_limits:
        # Prefer explicit `cpuset:` over `cpuset_count:` for reproducibility.
        # Pin to exactly 6 distinct physical cores to match ethpandaops's upstream
        # reference runs (which also use 6) so results are comparable.
        # See "CPU pinning" notes below for how to pick the actual ids per host.
        cpuset: [<6 logical CPU ids, one per distinct physical core>]
        # cpuset_count: 6         # alternative: random 6 CPUs each run — adds variance
        cpu_freq: "3600MHz"
        cpu_turboboost: false
        cpu_freq_governor: performance
        memory: "32g"

instances:
  - id: <instance-id>
    client: erigon
    image: <image-tag>           # must match the image tag from Step 1
    pull_policy: never            # critical — local image only
    extra_args:
      # fork overrides for the snapshot's chain state — values are dataset-specific
```

### CPU pinning

`cpuset:` (explicit logical-CPU list) and `cpuset_count:` (random N CPUs each run) are mutually exclusive. **Prefer `cpuset:`** — it's deterministic across runs and lets you pick topology-aware values. `cpuset_count` picks N logical CPUs *at random* each run, which on SMT hosts produces different physical-core counts each time (the random selection often double-books some physical cores via their SMT siblings), baking noise into A/B comparisons.

**Pin to exactly 6 logical CPUs**, one per distinct physical core, avoiding SMT sibling pairs. The "6" matches what ethpandaops's upstream reference runs use (e.g. `cpuset: [6,7,8,9,10,11]` at <https://benchmarkoor.core.ethpandaops.io/runs/>), so any A/B against published reference numbers is core-count-comparable. If the host has more than 6 physical cores, leave the extras unpinned so the docker daemon, benchmarkoor itself, and the host kernel don't compete with the bench workload. If the host has fewer than 6 physical cores, that's a deeper problem — note it and ask the user.

Discover the host's topology:

```bash
lscpu | grep -E "^CPU|^Thread|^Core|^Socket|Model name"
for c in $(seq 0 $(($(nproc)-1))); do
  printf 'cpu%s: core=%s siblings=%s\n' \
    "$c" \
    "$(cat /sys/devices/system/cpu/cpu$c/topology/core_id)" \
    "$(cat /sys/devices/system/cpu/cpu$c/topology/thread_siblings_list)"
done
```

Read off **6 logical CPUs whose `core_id`s are distinct** (i.e. skip SMT siblings). On a typical Linux topology where logical CPUs 0..N-1 are physical and N..2N-1 are SMT siblings of cores 0..N-1, pick any 6 from the lower half.

The literal cpuset numbers in the reference run (`6,7,8,9,10,11`) are specific to *that* host — don't copy them; replicate the *intent* (deterministic + physical-only + count=6).

### Sanity-check the filter

To know how many tests will actually run, dry-run the filter against the extracted test fixtures. Strip the `regex:` prefix from the YAML filter value and feed the rest to `grep -E`:
```bash
# e.g. for filter 'regex:__test_(blake2f_benchmark|ecrecover)\[.*benchmark_120M\]'
ls "$BENCH_DIR"/.cache/opcode-archive-extract-*/eest_bal/testing/ 2>/dev/null \
  | grep -cE '__test_(blake2f_benchmark|ecrecover)\[.*benchmark_120M\]'
```

### Step 4 — Run benchmarkoor

```bash
cd "$BENCH_DIR" && sudo -n ./benchmarkoor run \
  --config "$CONFIG" \
  --limit-instance-id "$INSTANCE_ID" \
  2>&1 | tee /tmp/benchmarkoor.log
```

Notes:
- `benchmarkoor run --help` shows both `--limit-instance-id` (specific instance ids; what we use) and `--limit-instance-client` (any instance for a given client name). They coexist; pick the one matching how you've keyed your instances. Without either flag, benchmarkoor runs every instance in the config.
- Each test runs as its own freshly-recreated container; the suite wall-clock scales linearly with `<number of tests matched by the filter>`. Pre-test orchestration (container start, gas-bump, funding) dominates over the actual test on this setup, so expect tens of seconds per test even for tiny test payloads.
- Run in background and watch progress: either tail+grep `tail -F /tmp/benchmarkoor.log | grep -E "index=[0-9]+/|Error|FAIL|panic"`, or use the `Monitor` tool with the same filter to get notifications.
- While running, `sudo -n docker ps` shows the active container (`benchmarkoor-<runid>-<instance>-<index>`).

### Step 5 — Locate results

```bash
ls -t "$BENCH_DIR"/results/runs/ | head -3
```

Most recent dir matches `<unix_timestamp>_<short_hash>_<instance-id>/`. Inside, each test has its own dir:

```
$BENCH_DIR/results/runs/<run-id>/
├── config.json                                        # snapshot of the YAML
├── result.json                                        # aggregated run-level stats
├── benchmarkoor.log
├── test_<name>.py__test_<func>[<params>].txt/
│   ├── setup.result-aggregated.json
│   ├── setup.result-details.json
│   ├── test.result-aggregated.json                    # ← per-test MGas/s lives here
│   └── test.result-details.json
└── ...                                                # one dir per test that ran
```

To confirm the run completed all matched tests, check `result.json`'s `tests_total` / `tests_passed` (or grep `Run result written ... tests_count=<N>` in `benchmarkoor.log`).

The MGas/s value for each test is at:
```
.method_stats.mgas_s.engine_newPayloadV<N>.last
```

The `V<N>` suffix depends on the dataset's hardfork (V5 for Amsterdam, V4 for Prague, V3 for Cancun, etc. — matching line at top of skill). **Don't guess the key — inspect one `test.result-aggregated.json` first**, e.g.:
```bash
jq -r '.method_stats.mgas_s | keys[]' \
  "$BENCH_DIR"/results/runs/<run-id>/test_*/test.result-aggregated.json | sort -u | head
```

A `.last` value (instead of `.mean`) is fine because each test runs exactly one such call against its payload. If you're A/B-comparing runs across hardforks, the keys differ — the comparator below will show `n/a` for any mismatched key.

### Step 6 — Build a comparison table

Use a Python one-liner that reads two run dirs and produces a per-test speedup table sorted by ratio. The script auto-handles different test counts and naming patterns:

```python
# Substitute <bench-dir>, <old-run-dirname>, <new-run-dirname> below before running.
# Python does NOT expand shell variables; use literal paths or os.environ.
import json, glob, os, re

RUNS_DIR  = '<bench-dir>/results/runs'   # or: os.environ['BENCH_DIR'] + '/results/runs'
before_id = '<old-run-dirname>'
after_id  = '<new-run-dirname>'

def shorten(name):
    m = re.search(r'test_(\w+)\.py__(test_\w+)\[(.+)\]', name)
    if not m: return name
    tname, params = m.group(2), m.group(3)
    parts = [tname]
    for label, pat in [('', r'opcode_(\w+)'), ('mod', r'mod_bits_(\d+)'),
                       ('rounds', r'rounds_(\d+)'), ('', r'benchmark_(\d+M)')]:
        mm = re.search(pat, params)
        if mm: parts.append(f'{label}{mm.group(1)}')
    return '-'.join(parts)

def mgas(run_id):
    """Read MGas/s from each test in a run. Auto-detects the engine_newPayloadV<N> key."""
    out = {}
    for f in sorted(glob.glob(os.path.join(RUNS_DIR, run_id, 'test_*/test.result-aggregated.json'))):
        with open(f) as fp: d = json.load(fp)
        name = os.path.basename(os.path.dirname(f)).removesuffix('.txt')
        stats = d.get('method_stats', {}).get('mgas_s', {})
        # Pick the first engine_newPayloadV* entry — same hardfork ⇒ same key across tests.
        key = next((k for k in stats if k.startswith('engine_newPayloadV')), None)
        if key and 'last' in stats[key]:
            out[name] = stats[key]['last']
    return out

b, a = mgas(before_id), mgas(after_id)
rows = [(shorten(n), b.get(n), a.get(n)) for n in sorted(set(b)|set(a))]
rows = [(n, bv, av,
         (av / bv) if (bv is not None and av is not None and bv > 0) else None)
        for n, bv, av in rows]
rows.sort(key=lambda r: r[3] if r[3] is not None else 0, reverse=True)

def fmt_num(x): return f'{x:>14.1f}' if x is not None else f'{"n/a":>14}'
def fmt_sp(s):  return f'{s:>8.2f}x' if s is not None else f'{"n/a":>9}'

print(f'{"Test":<55} {"Before":>14} {"After":>14} {"Speedup":>9}')
for n, bv, av, sp in rows:
    print(f'{n:<55} {fmt_num(bv)} {fmt_num(av)} {fmt_sp(sp)}')
if b and a:
    print(f'\navg: {sum(b.values())/len(b):.1f} → {sum(a.values())/len(a):.1f} ({sum(a.values())/sum(b.values()):.2f}x)')
```

Render as a markdown table for inclusion in the response; write to `/tmp/benchmark_comparison.md` if the user wants to copy-paste. **Important**: the average is over the test set actually present in both runs. If the filter changed between runs, the averages aren't directly comparable — call that out explicitly.

## Failure modes & gotchas

| Symptom | Cause | Fix |
|---|---|---|
| `Failed to set turbo boost` warning | CPU governor not user-controllable | Harmless; ignore. |
| `HEAD failed; using cached file` | GitHub Actions artifact HEAD requires auth | Harmless if cache is present at `$BENCH_DIR/.cache/`. |
| `Container stopped for recreate` count = N–1 (not N) at end | Last container's "stopped" log fires after the suite completion log | Verify with `Run result written ... tests_count=<N>` in the log. |
| Big variance between identical runs | CPU governor not pinned, or other heavy workload | Always set `cpu_freq_governor: performance`; don't run other CPU-heavy tasks (full make test-all, syncing nodes) simultaneously. |
| Image-tag mismatch (the right `IMAGE_TAG` not used) | Docker cached an older layer | Rebuild the image (Step 1) explicitly; confirm `docker images` shows a recent `CREATED` time. |
| Old `benchmarkoor-<hash>-*` container lingering | Previous run aborted before cleanup | `sudo -n docker rm -f <container>` + `sudo -n ./benchmarkoor cleanup`. |
| Run "completes" instantly with 0 tests | Wrong cwd, or filter regex matches 0 tests | Confirm cwd is `$BENCH_DIR`; sanity-check the filter against `$BENCH_DIR/.cache/opcode-archive-extract-*/eest_bal/testing/*.txt`. |

## A few "what to remember" rules

- **Discover the host-specific names per session.** Don't hard-code `erigon_snapshot`/`erigon_hybrid`/`benchmarkoor.interop.bal.yaml`/`erigon-local:traced`/`erigon-bal-full` — they vary between datasets and hosts.
- **Always run the reset script (if it exists) before each run.** State leakage between runs is real and silently skews numbers.
- **Always pass `--limit-instance-id`** when comparing just one client — otherwise the run also exercises every other client in the config (geth/besu/reth/nethermind/…) which adds tens of minutes and clutters `results/runs/`.
- **The `image` tag in the YAML must match `pull_policy: never`** — benchmarkoor will refuse to pull, so a missing local image fails immediately rather than silently downloading a stale upstream one.
- **`results/runs/index.json` is regenerated by `generate-index-file`**; don't hand-edit. After a successful run it's auto-generated when `generate_results_index: true` in the config. If it's stale or missing (e.g. an aborted previous run, or comparing against an older directory the index doesn't list), regenerate explicitly: `sudo -n ./benchmarkoor generate-index-file --config "$CONFIG"`.
- **A run dir is keyed by suite-hash** (under `results/suites/`). Two runs with the same filter regex have the same suite hash, so comparing them is direct. A different filter ⇒ different suite hash ⇒ different test set, and shortened-name matching is the only sane cross-suite comparison — flag this to the user.
- **For PR-style before/after testing:** stash the change, rebuild image (Step 1), reset working dir (Step 2), run baseline; unstash, rebuild image, reset, run again. Compare the two newest run dirs. Don't compare a fresh run against an old result captured before unrelated config changes — too many variables.
