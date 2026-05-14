---
name: erigon-test-rpc
description: Run QA RPC Integration Tests locally against a synced Erigon datadir, or dispatch them remotely via GitHub Actions. Tests compare rpcdaemon JSON-RPC responses against expected results from erigontech/rpc-tests.
---

# QA RPC Integration Tests

Tests that run `rpcdaemon` against a pre-synced Erigon datadir and compare JSON-RPC responses against golden expected outputs from the [`erigontech/rpc-tests`](https://github.com/erigontech/rpc-tests) repository.

## Prerequisites

- A **synced Erigon mainnet datadir** (the tests need specific blocks to exist — tests at block ~24.3M require sync to at least that height)
- Python 3 (for the rpc-tests runner)
- `rpcdaemon` and `integration` binaries built (`make rpcdaemon integration`)

## How It Works

The CI workflow:
1. Backs up chaindata from the reference datadir
2. Runs `integration run_migrations` to apply pending migrations
3. Starts `rpcdaemon --datadir <reference-datadir>`
4. Runs `run_rpc_tests.sh` which clones `erigontech/rpc-tests`, sets up a Python venv, and runs `run_tests.py` against port 8545
5. Restores original chaindata from backup

The backup/restore pattern exists because `run_migrations` writes to the DB.

## Local Setup

### Step 1: Set your reference datadir

```bash
# Point to your synced mainnet Erigon node
export ERIGON_REFERENCE_DATA_DIR=/path/to/your/erigon/mainnet-datadir

# Optional: workspace for test output
export WORKSPACE=/tmp/rpc-test-run
mkdir -p $WORKSPACE
```

### Step 2: Back up chaindata (protects your DB from migration writes)

```bash
cp -r $ERIGON_REFERENCE_DATA_DIR/chaindata /tmp/chaindata-backup
```

### Step 3: Build binaries

```bash
make rpcdaemon integration
```

### Step 4: Run migrations

```bash
./build/bin/integration run_migrations --datadir $ERIGON_REFERENCE_DATA_DIR --chain mainnet
```

### Step 5: Start rpcdaemon

```bash
./build/bin/rpcdaemon --datadir $ERIGON_REFERENCE_DATA_DIR \
  --http.api admin,debug,eth,parity,erigon,trace,web3,txpool,ots,net \
  --ws > /tmp/rpcdaemon.log 2>&1 &

RPC_DAEMON_PID=$!

# Wait for port 8545
for i in {1..30}; do
  nc -z localhost 8545 && break || sleep 5
done
```

### Step 6: Run the tests

```bash
.github/workflows/scripts/run_rpc_tests_ethereum.sh $WORKSPACE /tmp/rpc-results
echo "Exit: $?"
```

### Step 7: Stop and restore

```bash
kill $RPC_DAEMON_PID

# Restore original chaindata
rm -rf $ERIGON_REFERENCE_DATA_DIR/chaindata
mv /tmp/chaindata-backup $ERIGON_REFERENCE_DATA_DIR/chaindata
```

## Run a Single Test Manually

Clone rpc-tests and run one test directly:

```bash
git clone --depth 1 --branch v1.121.0 https://github.com/erigontech/rpc-tests /tmp/rpc-tests
cd /tmp/rpc-tests
python3 -m venv .venv && source .venv/bin/activate
pip3 install -r requirements.txt

cd integration
# Run just one test
python3 run_tests.py --blockchain mainnet --port 8545 \
  --include-api-list eth_getBlockByNumber/test_1.json \
  --display-only-fail --json-diff
```

## When Tests Fail — Drill Down

The runner outputs diffs for failing tests. Each test is a `.json` file in the `integration/<chain>/` directory of `rpc-tests`. To investigate:

```bash
# Show which tests failed
grep "FAILED\|PASS\|SKIP" /tmp/rpc-results/output.log | grep FAIL

# Re-run just the failing API group
cd /tmp/rpc-tests/integration
python3 run_tests.py --blockchain mainnet --port 8545 \
  --include-api-list eth_getBlockByNumber \
  --display-only-fail --json-diff

# Run with full response dump to see what rpcdaemon returned
python3 run_tests.py --blockchain mainnet --port 8545 \
  --include-api-list eth_getBlockByNumber/test_1.json \
  --dump-response
```

## Disabled Tests

Some tests are intentionally disabled in the CI run (see `run_rpc_tests_ethereum.sh`). These include:
- Engine API tests (require active Erigon engine, not just rpcdaemon)
- Tests requiring specific peer state (`net_peerCount`, `admin_peers`, etc.)
- Known-broken tests with open issues

## CI Equivalents

| Variant | CI workflow | File |
|---------|-------------|------|
| Mainnet | QA - RPC Integration Tests | `qa-rpc-integration-tests.yml` |
| Latest blocks | QA - RPC Integration Tests Remote | `qa-rpc-integration-tests-remote.yml` |
| Gnosis | QA - RPC Integration Tests (Gnosis) | `qa-rpc-integration-tests-gnosis.yml` |

Remote dispatch:
```bash
gh workflow run "QA - RPC Integration Tests" --ref <branch>
gh workflow run "QA - RPC Integration Tests Remote" --ref <branch>
gh workflow run "QA - RPC Integration Tests (Gnosis)" --ref <branch>
```

## TODO: Reference Datadir Setup

The CI runner maintains a pre-synced mainnet datadir at `/opt/erigon-versions/reference-version/datadir`. To create a local equivalent:

1. Sync Erigon mainnet to at least block ~24.5M (covers all test cases)
2. Stop the node cleanly
3. Set `ERIGON_REFERENCE_DATA_DIR` to that datadir
4. Follow the workflow above

A full mainnet sync takes ~2-4 days on NVMe with `--prune.mode=minimal`. Alternatively, ask the team for a snapshot of the reference datadir.
