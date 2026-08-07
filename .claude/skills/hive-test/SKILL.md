---
name: hive-test
description: Run Ethereum Hive integration tests against a local Erigon build, including engine, RPC compatibility, stable and devnet EEST EngineX, and RLP suites. Use when setting up an ephemeral Hive environment, selecting or running Hive suites, interpreting Hive failures, or cleaning up Hive test resources.
---

# Skill: hive-test

Run Ethereum Hive integration tests against a local Erigon build. Works from a clean
environment -- no pre-existing hive installation required.

## Metadata
- user-invocable: true
- description: Run Hive integration tests (engine, rpc, eest) against local Erigon
- allowed-tools: Bash, Read, Write, Edit, Glob, Grep

## Overview

This skill sets up an ephemeral Hive test environment, builds a local Erigon Docker
image, runs the requested test suites, reports results, and cleans up containers.

## Arguments

The user may specify one or more test suites in any combination:

### Individual suites
| Suite name | Simulator | Description |
|-----------|-----------|-------------|
| `exchange-capabilities` | ethereum/engine | Engine exchange-capabilities |
| `withdrawals` | ethereum/engine | Engine withdrawals |
| `cancun` | ethereum/engine | Engine cancun |
| `api` | ethereum/engine | Engine API |
| `auth` | ethereum/engine | Engine auth |
| `rpc-compat` | ethereum/rpc | RPC compatibility |
| `eest` | ethereum/eels/consume-enginex | Stable EEST EngineX fixtures |
| `eest-devnet` | ethereum/eels/consume-enginex | Fork-partitioned devnet EEST EngineX fixtures |
| `eest-rlp` | ethereum/eels/consume-rlp | EEST RLP block import (BlockchainTest, all forks) |

### Groups
| Group name | Expands to |
|-----------|------------|
| `engine` | exchange-capabilities, withdrawals, cancun, api, auth |
| `all` | Every suite listed above |

### Examples
- `/hive-test api` - Run just the engine API suite
- `/hive-test withdrawals api` - Run withdrawals and API suites
- `/hive-test engine` - Run all engine suites
- `/hive-test engine rpc-compat` - Run all engine suites plus rpc-compat
- `/hive-test eest-devnet` - Run devnet EEST tests (BAL/glamsterdam)
- `/hive-test eest-rlp` - Run EEST RLP block-import tests
- `/hive-test all` - Run everything

### Options
- **erigon-path** - Path to local erigon source (default: current working directory)
- **branch=BRANCH** - Clone erigon from a remote branch instead of using the local
  working directory. The branch is cloned from `https://github.com/erigontech/erigon.git`
  into the hive client directory. Example: `/hive-test api branch=fix/my-feature`
- **eest-version=VERSION** - Override the stable fixture release. Default: use `eest_stable` from `test-fixtures.json`.
- **devnet-version=VERSION** - Override the devnet fixture release. Default: use `eest_devnet` from `test-fixtures.json`.

## Expected Failures (CI thresholds)

Sources of truth: `.github/workflows/test-hive.yml` (`max-allowed-failures` per matrix
entry) for engine + rpc-compat suites, `.github/workflows/test-hive-eest.yml`
(`max-failures` per matrix entry) for eest shards.

| Suite | Max Allowed Failures |
|-------|---------------------|
| exchange-capabilities | 0 |
| withdrawals | 0 |
| cancun | 3 (2 known Hive/Geth secondary-client failures + 1 known parallelism flake) |
| api | 0 |
| auth | 0 |
| rpc-compat | 0 |
| eest (consume-enginex) | 0 |
| eest-rlp | 0 |
| pre-Amsterdam eest-devnet consume-enginex shards | 0 |
| glamsterdam-devnet consume-enginex | 4 |

Note: Failure counts are version-dependent and may change with newer fixtures.
The CI devnet rows split every EngineX fork exactly once and run only with parallel
execution on GitHub-hosted runners at simulator parallelism 4. Read the current
filters and client flags from `.github/workflows/test-hive-eest.yml`; the Amsterdam
row enables `--experimental.bal`.

## Procedure

### Phase 0: Resolve Fixtures

Use the repository pins by default so local results reproduce CI:

```bash
EEST_FIXTURES_URL=$(jq -r '.eest_stable.url' test-fixtures.json)
DEVNET_FIXTURES_URL=$(jq -r '.eest_devnet.url' test-fixtures.json)
DEVNET_BRANCH=$(jq -r '.eest_devnet.branch' test-fixtures.json)
```

Only replace these values when the user explicitly requests another release.

### Phase 1: Setup

1. **Determine erigon source path.** Default is the current git working directory.
   Verify it contains a `Makefile` and `go.mod` with `erigontech/erigon`.

2. **Choose a work directory.** Use `mktemp -d /tmp/hive-test-XXXXXX` for isolation.

3. **Clone hive:**
   ```bash
   WORKDIR=$(mktemp -d /tmp/hive-test-XXXXXX)
   cd "$WORKDIR"
   git clone --depth 1 https://github.com/ethereum/hive.git
   cd hive
   ```

4. **Copy or clone the erigon source into hive:**

   If `branch=BRANCH` was specified, clone that branch:
   ```bash
   git clone --depth 1 --branch "$BRANCH" \
     https://github.com/erigontech/erigon.git clients/erigon/erigon
   ```

   Otherwise, copy the local source:
   ```bash
   # Use rsync to copy, excluding build artifacts and .git
   rsync -a --exclude='.git' --exclude='build/' --exclude='temp/' \
     "$ERIGON_PATH/" clients/erigon/erigon/
   ```

5. **Install Dockerfile.local** for local builds:
   Ensure `clients/erigon/Dockerfile.local` exists with the correct content.
   Key requirements:
   - Base image: `golang:1.25.7-trixie` (Debian, not Alpine)
   - Build command: `make erigon`
   - Runtime: `debian:13-slim` with `bash curl jq libstdc++6 libgcc-s1`

   If `clients/erigon/Dockerfile.local` doesn't already exist, write the correct version:
   ```dockerfile
   FROM golang:1.25.7-trixie AS builder
   ARG local_path=erigon
   COPY $local_path erigon
   RUN apt-get update && apt-get install -y bash build-essential ca-certificates git \
       && cd erigon && make erigon \
       && cp build/bin/erigon /usr/local/bin/erigon

   FROM debian:13-slim
   COPY --from=builder /usr/local/bin/erigon /usr/local/bin/
   RUN apt-get update && apt-get install -y bash curl jq libstdc++6 libgcc-s1 && rm -rf /var/lib/apt/lists/*
   RUN erigon --version | sed -e 's/erigon version \(.*\)/\1/' > /version.txt
   COPY genesis.json /genesis.json
   COPY mapper.jq /mapper.jq
   COPY erigon.sh /erigon.sh
   COPY enode.sh /hive-bin/enode.sh
   RUN chmod +x /erigon.sh /hive-bin/enode.sh
   EXPOSE 8545 8546 8551 30303 30303/udp
   ENTRYPOINT ["/erigon.sh"]
   ```

6. **P2P protocol configuration:**
   Do NOT add `--p2p.protocol` flags to erigon.sh. Let erigon use its default
   protocol negotiation.

6b. **EngineX client configuration:**
   Mirror the selected row from `.github/workflows/test-hive-eest.yml`. For every
   stable or devnet EngineX run, patch `clients/erigon/erigon.sh` with
   `--fcu.background.prune=false --fcu.timeout=0`. Devnet runs also bake
   `ERIGON_EXEC3_PARALLEL=true` into the client image. Add `--experimental.bal`
   only for the Amsterdam family; use a separate client image when running it
   alongside other EngineX families.

7. **Create client config file:**
   ```bash
   cat > erigon-local.yaml <<'EOF'
   - client: erigon
     dockerfile: local
   EOF
   ```

8. **Build hive binary:**
   ```bash
   go build .
   ```

### Phase 2: Run Tests

Use `--sim.parallelism 4` for EEST EngineX runs to match the GitHub-hosted CI
runners and avoid simulator connection exhaustion on the larger devnet shards.
Use up to 12 for the smaller engine and RPC suites when local resources allow it.

When running multiple suites, launch **separate hive sessions in parallel** (as
background shell commands) whenever the suites use different simulators. This gives
`runs × parallelism` total concurrency. Suites using the same simulator can be combined
with `--sim.limit "suite1|suite2|..."`.

**Engine suites** (sim: `ethereum/engine`) — combine all with `|`:
```bash
./hive --client-file erigon-local.yaml --sim ethereum/engine \
  --sim.limit "exchange-capabilities|withdrawals|cancun|api|auth" \
  --sim.parallelism 12 --sim.timelimit 30m
```

**RPC compat** (sim: `ethereum/rpc`, limit: `compat`):
```bash
./hive --client-file erigon-local.yaml --sim ethereum/rpc \
  --sim.limit "compat" --sim.parallelism 12 --sim.timelimit 30m
```

**Stable EEST EngineX** (sim: `ethereum/eels/consume-enginex`):
```bash
./hive --client-file erigon-local.yaml \
  --sim ethereum/eels/consume-enginex \
  --sim.parallelism=4 --docker.nocache=true \
  --sim.buildarg fixtures=${EEST_FIXTURES_URL} \
  --sim.timelimit 60m
```

**Devnet EEST EngineX** (sim: `ethereum/eels/consume-enginex`):
```bash
# Use one devnet sim-limit from .github/workflows/test-hive-eest.yml at a time.
# This example runs the Amsterdam family.
./hive --client-file erigon-local.yaml \
  --sim ethereum/eels/consume-enginex \
  --sim.limit=".*/.*fork_(Amsterdam|BPO2ToAmsterdam)" \
  --sim.parallelism=4 --docker.nocache=true \
  --sim.buildarg branch=${DEVNET_BRANCH} \
  --sim.buildarg fixtures=${DEVNET_FIXTURES_URL} \
  --sim.timelimit 60m
```

**EEST RLP** (sim: `ethereum/eels/consume-rlp`):

Tests block import via RLP-encoded blocks loaded at client startup (the historical
sync code path). Uses the `BlockchainTest` fixture format and covers all forks
including pre-merge, complementary to consume-enginex which only covers Paris+.
See https://eest.ethereum.org/main/running_tests/running/#engine-vs-rlp-simulator.

The full RLP test set is too large to run end-to-end in CI — always pass a
`--sim.limit` regex narrowing the scope. CI mirrors this by running only
`.*eip2930_access_list.*`. For local debugging, target a single EIP / opcode
group similarly. Fixtures come from the same `fixtures_develop.tar.gz` archive
as consume-enginex.

```bash
# Replace the sim.limit regex to scope to the area under test.
./hive --client-file erigon-local.yaml \
  --sim ethereum/eels/consume-rlp \
  --sim.limit=".*eip2930_access_list.*" \
  --sim.parallelism=12 --docker.nocache=true \
  --sim.buildarg fixtures=${EEST_FIXTURES_URL} \
  --sim.timelimit 60m
```

### Phase 3: Parse Results

After each suite, parse the output to extract results:
```bash
status_line=$(tail -2 output.log | head -1 | sed -r "s/\x1B\[[0-9;]*[a-zA-Z]//g")
suites=$(echo "$status_line" | sed -n 's/.*suites=\([0-9]*\).*/\1/p')
tests=$(echo "$status_line" | sed -n 's/.*tests=\([0-9]*\).*/\1/p')
failed=$(echo "$status_line" | sed -n 's/.*failed=\([0-9]*\).*/\1/p')
```

Also check the JSON result files in `workspace/logs/*.json` for detailed per-test
results.  Report pass/fail counts and list any failing test names.

### Phase 4: Cleanup

**Always run cleanup**, even if tests fail:

```bash
# Clean up hive containers
./hive --cleanup

# Optionally remove the work directory
rm -rf "$WORKDIR"

# Prune dangling docker images from the test run
docker image prune -f
```

## Troubleshooting

### Timeout failures
Run suites separately instead of combining them. Increase `--sim.timelimit` if needed.

### Leftover containers
Run `./hive --cleanup` or `docker rm -f $(docker ps -aq)` to remove stale containers.
The hive binary has built-in cleanup: `./hive --cleanup --cleanup.older-than 1h`.
