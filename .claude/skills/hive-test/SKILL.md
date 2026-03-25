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
| `eest` | ethereum/eels/consume-engine | Execution Spec Tests (version auto-discovered) |
| `eest-bal` | ethereum/eels/consume-engine | EEST BAL amsterdam fixtures (version auto-discovered) |

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
- `/hive-test eest-bal` - Run BAL-specific EEST tests
- `/hive-test all` - Run everything

### Options
- **erigon-path** - Path to local erigon source (default: current working directory)
- **branch=BRANCH** - Clone erigon from a remote branch instead of using the local
  working directory. The branch is cloned from `https://github.com/erigontech/erigon.git`
  into the hive client directory. Example: `/hive-test api branch=fix/my-feature`
- **eest-version=VERSION** - Pin EEST fixtures version (e.g. `v5.3.0`). Default: auto-discover latest.
- **bal-version=VERSION** - Pin BAL fixtures version (e.g. `bal@v5.1.0`). Default: auto-discover latest.

## Expected Failures (CI thresholds)

| Suite | Max Allowed Failures |
|-------|---------------------|
| exchange-capabilities | 0 |
| withdrawals | 0 |
| cancun | 3 |
| api | 0 |
| auth | 0 |
| rpc-compat | 23 |
| eest (consume-engine) | 0 |
| eest-bal | 4 (upstream BPO2ToAmsterdamAtTime15k fork transition) |

Note: Failure counts are version-dependent and may change with newer fixtures.
The eest-bal fork transition failures are a known upstream issue, not Erigon bugs.

## Procedure

### Phase 0: Discover Versions

Before setup, discover the latest EEST fixture versions from GitHub. Skip this phase
if the user provided explicit version overrides (`eest-version=`, `bal-version=`).

```bash
# Latest standard EEST fixtures (tag matching v*.*.*)
EEST_VERSION=$(curl -s https://api.github.com/repos/ethereum/execution-spec-tests/releases \
  | jq -r '[.[] | select(.tag_name | test("^v[0-9]+\\.[0-9]+\\.[0-9]+$"))][0].tag_name')

# Latest BAL fixtures (tag matching bal@v*.*.*)
BAL_TAG=$(curl -s https://api.github.com/repos/ethereum/execution-spec-tests/releases \
  | jq -r '[.[] | select(.tag_name | startswith("bal@"))][0].tag_name')
BAL_BRANCH="tests-${BAL_TAG}"
BAL_TAG_URLENC=$(echo "$BAL_TAG" | sed 's/@/%40/')
BAL_FIXTURES_URL="https://github.com/ethereum/execution-spec-tests/releases/download/${BAL_TAG_URLENC}/fixtures_bal.tar.gz"
```

Then check whether the EEST mapper at the discovered tag already has the exception
entries Erigon needs. If not, the `disable_strict_exception_matching` workaround is
required:

```bash
MAPPER_URL="https://raw.githubusercontent.com/ethereum/execution-specs/refs/tags/${BAL_BRANCH}/packages/testing/src/execution_testing/client_clis/clis/erigon.py"
if curl -sf "$MAPPER_URL" | grep -q "GAS_USED_OVERFLOW"; then
    DISABLE_STRICT=""
    echo "Mapper has BAL exception entries — strict matching enabled"
else
    DISABLE_STRICT='--sim.buildarg disable_strict_exception_matching=erigon'
    echo "Mapper missing BAL exception entries — strict matching disabled for erigon"
fi
```

Log the discovered versions:
```
EEST: $EEST_VERSION | BAL: $BAL_TAG (branch: $BAL_BRANCH) | Strict matching: enabled/disabled
```

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
   - P2P protocol: `erigon.sh` must include `--p2p.protocol 68,69`

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
   Do NOT add `--p2p.protocol` flags to erigon.sh. The hive devp2p simulator
   does not yet support eth/69, so advertising it causes Fork ID test failures
   (`rlp: expected input list for devp2p.Disconnect`). Let erigon use its default
   protocol negotiation.

6b. **Parallel execution for Amsterdam (BAL) blocks:**
   Verify that `erigon.sh` enables parallel execution when Amsterdam is configured.
   BAL validation only runs in the parallel execution path. If the following block
   is missing from `erigon.sh`, add it after the `--sync.parallel-state-flushing` line:
   ```bash
   # Enable parallel execution for Amsterdam (BAL) blocks.
   # BAL validation only runs in the parallel execution path.
   if [ "$HIVE_AMSTERDAM_TIMESTAMP" != "" ]; then
       export ERIGON_EXEC3_PARALLEL=true
   fi
   ```
   Without this, Amsterdam blocks run through serial execution which has no BAL
   validation, causing `test_bal_invalid` tests to incorrectly return VALID.

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

Always use `--sim.parallelism 12` for all suites to maximize throughput.

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

**EEST** (sim: `ethereum/eels/consume-engine`):
```bash
# $EEST_VERSION discovered in Phase 0 (e.g. v5.4.0), or user-provided via eest-version=
./hive --client-file erigon-local.yaml \
  --sim ethereum/eels/consume-engine \
  --sim.parallelism=12 --docker.nocache=true \
  --sim.buildarg fixtures=https://github.com/ethereum/execution-spec-tests/releases/download/${EEST_VERSION}/fixtures_develop.tar.gz \
  --sim.timelimit 60m
```

**EEST BAL** (sim: `ethereum/eels/consume-engine`, amsterdam filter):
```bash
# $BAL_BRANCH, $BAL_FIXTURES_URL, $DISABLE_STRICT discovered in Phase 0
# (e.g. BAL_BRANCH=tests-bal@v5.2.0), or user-provided via bal-version=
./hive --client-file erigon-local.yaml \
  --sim ethereum/eels/consume-engine \
  --sim.limit=".*amsterdam.*" \
  --sim.parallelism=12 --docker.nocache=true \
  --sim.buildarg branch=${BAL_BRANCH} \
  --sim.buildarg fixtures=${BAL_FIXTURES_URL} \
  ${DISABLE_STRICT} \
  --sim.timelimit 60m
```

Note: The `disable_strict_exception_matching` flag is only added when Phase 0 detects
that the EEST `ErigonExceptionMapper` at the discovered tag is missing required entries
(e.g. `BlockException.GAS_USED_OVERFLOW`, BAL exception types). Once upstream updates
their tagged releases with the mapper fixes (already on `forks/amsterdam` HEAD), this
flag will no longer be needed automatically.

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

### Fork ID test failures: "rlp: expected input list for devp2p.Disconnect"
The erigon.sh is missing eth/69 support. Ensure `--p2p.protocol 68,69`.

### Timeout failures
Run suites separately instead of combining them. Increase `--sim.timelimit` if needed.

### Leftover containers
Run `./hive --cleanup` or `docker rm -f $(docker ps -aq)` to remove stale containers.
The hive binary has built-in cleanup: `./hive --cleanup --cleanup.older-than 1h`.
