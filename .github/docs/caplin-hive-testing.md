# Caplin + Erigon: Hive & Kurtosis Testing Setup

## Overview

This documents how to run hive and kurtosis tests with erigon as the EL and caplin
as the CL. There are two test axes:

1. **Hive** — tests the EL Engine API (NewPayload, FCU, withdrawals, cancun, auth).
   Caplin is not involved; hive drives the Engine API directly.
2. **Kurtosis + Assertoor** — tests the full CL+EL stack: caplin as standalone CL,
   erigon as EL, lighthouse as VC. Validates block production, sync, finalization.

## Hive Setup

### Current State

Hive is cloned from `ethereum/hive` at commit `bf5e74fa` with local modifications
to the `clients/erigon/` directory.

Location: `/erigon/mark/hive/`

### Files Modified from Upstream

| File | Change | Why |
|------|--------|-----|
| `clients/erigon/Dockerfile` | Replaced pre-built image with local build from source (alpine-based) | Build from working tree instead of pulling from Docker Hub |
| `clients/erigon/Dockerfile.local` | Added `caplin` binary build, `EXTRA_BUILD_TAGS=nosilkworm`, extra ports | Local development builds need caplin + silkworm disabled |
| `clients/erigon/erigon.sh` | Added `ERIGON_EXEC3_PARALLEL=true` for Amsterdam/BAL blocks | BAL validation only runs in parallel execution path |

### Key Files

```
/erigon/mark/hive/
├── hive                          # Pre-built hive binary
├── erigon-local.yaml             # Client config: "- client: erigon\n  dockerfile: local"
├── clients/erigon/
│   ├── Dockerfile                # Modified: builds from local source (alpine)
│   ├── Dockerfile.local          # Modified: builds erigon+caplin from local source (debian)
│   ├── erigon/                   # Erigon source (copy/symlink of working tree)
│   ├── erigon.sh                 # Modified: +BAL parallel execution env var
│   ├── enode.sh                  # Upstream (unchanged)
│   ├── genesis.json              # Upstream (unchanged)
│   └── mapper.jq                 # Upstream (unchanged)
└── workspace/logs/               # Test results (JSON) after runs
```

### Dockerfile.local (full)

```dockerfile
FROM golang:1.25.0-trixie as builder
ARG local_path=erigon
COPY $local_path erigon
RUN apt-get update && apt-get install -y bash build-essential ca-certificates git \
    && cd erigon && make EXTRA_BUILD_TAGS=nosilkworm erigon caplin \
    && cp build/bin/erigon /usr/local/bin/erigon \
    && cp build/bin/caplin /usr/local/bin/caplin

FROM debian:13-slim
COPY --from=builder /usr/local/bin/erigon /usr/local/bin/
COPY --from=builder /usr/local/bin/caplin /usr/local/bin/
RUN apt-get update && apt-get install -y bash curl jq libstdc++6 libgcc-s1 && rm -rf /var/lib/apt/lists/*
RUN erigon --version | sed -e 's/erigon version \(.*\)/\1/' > /version.txt
COPY genesis.json /genesis.json
COPY mapper.jq /mapper.jq
COPY erigon.sh /erigon.sh
COPY enode.sh /hive-bin/enode.sh
RUN chmod +x /erigon.sh /hive-bin/enode.sh
EXPOSE 8545 8546 8551 30303 30303/udp 30304 30304/udp
ENTRYPOINT ["/erigon.sh"]
```

### erigon.sh Modifications

Only one addition to upstream — Amsterdam/BAL parallel execution:

```bash
# Enable parallel execution for Amsterdam (BAL) blocks.
# BAL validation only runs in the parallel execution path.
if [ "$HIVE_AMSTERDAM_TIMESTAMP" != "" ]; then
    export ERIGON_EXEC3_PARALLEL=true
fi
```

### Running Hive Tests Locally

```bash
cd /erigon/mark/hive

# Copy/update erigon source into hive client directory
rsync -a --exclude='.git' --exclude='build/' /path/to/erigon/ clients/erigon/erigon/

# Run engine tests (all suites)
./hive --client-file erigon-local.yaml \
  --sim ethereum/engine \
  --sim.limit "exchange-capabilities|withdrawals|cancun|api|auth" \
  --sim.parallelism 12 \
  --sim.timelimit 30m

# Run RPC compat tests
./hive --client-file erigon-local.yaml \
  --sim ethereum/rpc \
  --sim.limit "compat" \
  --sim.parallelism 12 \
  --sim.timelimit 30m
```

### Expected Results

| Suite | Max Failures |
|-------|-------------|
| exchange-capabilities | 0 |
| withdrawals | 0 |
| cancun | 3 (known: "Invalid Missing Ancestor Syncing ReOrg") |
| api | 0 |
| auth | 0 |
| rpc-compat | 23 |

### Forking Hive

If we fork `ethereum/hive` to `erigontech/hive`:

1. Fork from commit `bf5e74fa` (current tested baseline)
2. Apply the 3 file modifications above (Dockerfile, Dockerfile.local, erigon.sh)
3. The CI workflow (`test-hive.yml`) currently uses `Dockerfile.git` which clones
   from GitHub. For a fork, change the workflow to point to `erigontech/hive` or
   switch to using `Dockerfile.local` with rsync
4. The erigon source in `clients/erigon/erigon/` is NOT committed — it's copied
   at test time. The fork only needs the Dockerfile and scripts.

### CI Integration (test-hive.yml)

The existing workflow at `.github/workflows/test-hive.yml`:
- Clones `ethereum/hive` at a pinned commit
- Replaces `clients/erigon/Dockerfile` with `Dockerfile.git`
- Patches in the erigon branch/repo being tested
- Runs hive with `--client erigon`

To use a fork, change the checkout step to point to `erigontech/hive` and
remove the Dockerfile replacement (since the fork would have the right Dockerfile).

---

## Kurtosis + Assertoor Setup

### Architecture

```
┌─────────────────────┐    ┌──────────────────────┐
│  el-1-erigon-caplin │    │   cl-1-caplin-erigon  │
│  (erigon EL)        │◄──►│   (caplin CL)         │
│  ports: 8545, 8551  │    │   ports: 4000, 9000   │
└─────────────────────┘    └──────────┬─────────────┘
                                      │ Beacon API
                           ┌──────────▼─────────────┐
                           │ vc-1-caplin-lighthouse  │
                           │ (lighthouse VC)         │
                           └─────────────────────────┘
                           ┌─────────────────────────┐
                           │      assertoor          │
                           │  (test orchestrator)    │
                           └─────────────────────────┘
```

### Docker Image Requirements

The Docker image (`test/erigon:current`) must contain BOTH binaries:

```bash
docker build --build-arg BINARIES="erigon caplin" -t test/erigon:current -f Dockerfile .
```

The default `Dockerfile` only builds `erigon`. Without `caplin`, kurtosis fails:
`sh: 1: exec: caplin: not found`

### Kurtosis Config (caplin-assertoor.io)

```yaml
participants:
  - el_type: erigon
    el_image: test/erigon:current
    el_log_level: "debug"
    cl_type: caplin
    cl_image: test/erigon:current
    cl_log_level: "debug"
    use_separate_vc: true
    vc_type: lighthouse
    vc_image: sigp/lighthouse:v7.0.1
network_params:
  seconds_per_slot: 12
  deneb_fork_epoch: 0
additional_services:
  - assertoor
assertoor_params:
  run_stability_check: true
  run_block_proposal_check: true
  image: ethpandaops/assertoor:v0.0.17
  tests:
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/synchronized-check.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/block-proposal-check.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/all-opcodes-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/blob-transactions-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/eoa-transactions-test.yaml
    - https://raw.githubusercontent.com/ethpandaops/assertoor-test/master/assertoor-tests/stability-check.yaml
```

### ethereum-package Branch Requirement

The caplin launcher (`src/cl/caplin/caplin_launcher.star`) only exists on the
`main` branch of `ethpandaops/ethereum-package`. The tagged branch `5.0.1`
(used by existing CI suites) does NOT have caplin support.

For CI, the caplin suite must use `ethereum_package_branch: "main"`.
Consider pinning to a specific commit once caplin support is stable upstream.

### Running Locally

```bash
# Build Docker image with both binaries
docker build --build-arg BINARIES="erigon caplin" -t test/erigon:current -f Dockerfile .

# Run kurtosis
kurtosis run github.com/ethpandaops/ethereum-package \
  --args-file caplin-assertoor.io \
  --enclave caplin-test

# Or with local ethereum-package clone
kurtosis run /path/to/ethereum-package \
  --args-file caplin-assertoor.io \
  --enclave caplin-test

# Check assertoor results
curl -s http://127.0.0.1:<assertoor-port>/api/v1/test_runs | python3 -m json.tool

# Check chain health
kurtosis service logs caplin-test cl-1-caplin-erigon 2>&1 | grep "finalizedEpoch" | tail -1
```

### CI Integration (test-kurtosis-assertoor.yml)

Added `caplin` to the matrix in `.github/workflows/test-kurtosis-assertoor.yml`:

```yaml
matrix:
  include:
    - suite: regular
      package_args: .github/workflows/kurtosis/regular-assertoor.io
      docker_binaries: "erigon"
      ethereum_package_branch: "5.0.1"
    - suite: pectra
      package_args: .github/workflows/kurtosis/pectra.io
      docker_binaries: "erigon"
      ethereum_package_branch: "5.0.1"
    - suite: caplin
      package_args: .github/workflows/kurtosis/caplin-assertoor.io
      docker_binaries: "erigon caplin"
      ethereum_package_branch: "main"
```

Key changes to the workflow:
- `docker_binaries` matrix variable → passed as `BINARIES` build-arg to Dockerfile
- `ethereum_package_branch` matrix variable → per-suite branch selection
- Per-suite cache scopes to avoid cache conflicts between different BINARIES

### Verified Test Results (2026-03-12)

| Test | Status |
|------|--------|
| Assertoor `synchronized-check` | PASS |
| Assertoor `block-proposal-check` | PASS |
| Chain finalization | epoch 7+, zero leaf rejections |
| Hive engine (401/403) | PASS (2 known Cancun failures) |

---

## Embedded Engine API Mode

The `--caplin.use-engine-api` flag runs Caplin embedded within the erigon binary,
routing through the EngineServer in-process instead of direct execution.

Three execution modes:

| Mode | Flag | Binary | Transport |
|------|------|--------|-----------|
| Embedded Direct | (default) | `erigon` | In-process gRPC |
| Embedded Engine API | `--caplin.use-engine-api` | `erigon` | In-process Engine API |
| Standalone Remote | `caplin --engine.api` | `caplin` + `erigon` | HTTP+JWT |

The kurtosis tests validate **Standalone Remote** mode. The hive tests validate
the **Engine API interface** (shared by all three modes).

### Testing Embedded Mode

Sync a Hoodi node with the flag and verify it reaches the chain tip:

```bash
./build/bin/erigon \
  --datadir=/path/to/hoodi-data \
  --chain=hoodi \
  --caplin.use-engine-api \
  --http --http.api=eth,erigon,engine,web3,net
```

Pass condition: node syncs to the chain tip and stays at head. This exercises
the full Engine API path (NewPayload, FCU) through the in-process EngineServer
rather than the direct execution shortcut.
