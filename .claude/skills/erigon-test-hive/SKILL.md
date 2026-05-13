---
name: erigon-test-hive
description: Run Erigon Hive simulator tests locally or via GitHub Actions. Tests EL/CL protocol interoperability (engine API, RPC compat, EEST fixtures). Requires Docker and either `act` (for CI simulation) or direct hive invocation.
---

# Erigon Hive Tests

Runs Hive simulator tests for EL/CL protocol interoperability. Two modes:
1. **`make test-hive`** — uses `act` (nektos) to simulate the GitHub Actions workflow locally
2. **`make hive-local`** — builds hive and runs suites directly (no `act` needed)
3. **`make eest-bal`** — runs the BAL/Amsterdam-specific EEST fixtures

## Prerequisites

- Docker running
- `GITHUB_TOKEN` environment variable set (for `make test-hive` via `act`)
- `act` installed (for `make test-hive`): https://nektosact.com/installation/index.html

## Docker Resource Warning

**Hive creates many Docker containers and images per test run.** Without cleanup, these accumulate and exhaust disk space and memory. Always run the cleanup steps below — even after a clean run.

Hive spawns per test:
- One container per EL client instance (erigon)
- One container per simulator (e.g., `ethereum/engine`, `eels/consume-engine`)
- Intermediate build images for each client Dockerfile

The CI workflow always runs `docker system prune -af --volumes` on exit (even on failure). Replicate this locally.

## Commands

### Via act (mirrors CI exactly)

```bash
export GITHUB_TOKEN=<your-token>

# Run standard hive suite (engine API, RPC compat, EEST)
make test-hive

# Run EEST-only suite
SUITE=eest make test-hive
```

### Direct hive run (no act required)

```bash
# Build erigon Docker image and run engine/rpc-compat suites
make hive-local
```

### BAL/Amsterdam-specific (EIP-7928 fixtures)

```bash
# Runs eels/consume-engine with amsterdam fixtures
make eest-bal
```

## Cleanup (ALWAYS run after hive tests)

Hive leaves behind stopped containers, dangling images, and build caches. Run this after every hive test run, including after failures or ctrl+c:

```bash
# Stop all running hive-related containers
docker ps --filter "name=hive" -q | xargs -r docker stop

# Full prune: removes all stopped containers, unused images, build cache, volumes
docker system prune -af --volumes
```

### Automated cleanup with trap (recommended for direct hive invocations)

Wrap hive invocations in a script with a trap so cleanup runs on exit/ctrl+c:

```bash
cleanup() {
  echo "Cleaning up Docker resources..."
  docker ps --filter "name=hive" -q | xargs -r docker stop 2>/dev/null || true
  docker system prune -af --volumes
  echo "Docker cleanup complete"
}
trap cleanup EXIT INT TERM

cd temp/<hive-dir>/hive
./hive --sim ethereum/engine --sim.limit=".*cancun.*" --client erigon
```

### Check what's accumulated

```bash
# Show disk usage by Docker
docker system df

# List dangling images
docker images --filter dangling=true

# List stopped containers from hive
docker ps -a --filter "name=hive" --format "table {{.Names}}\t{{.Status}}\t{{.Size}}"
```

## When a Suite Fails — Drill Down

Hive outputs test results to `temp/*/hive/workspace/logs/`. View results in the hiveview browser:

```bash
# hiveview serves results at http://localhost:3000
cd temp/<hive-dir>/hive
./hiveview --serve --logdir ./workspace/logs
```

Re-run a single suite by specifying the simulator and limit:
```bash
cd temp/<hive-dir>/hive

# Run only the failing simulator
./hive --sim ethereum/engine --sim.limit=exchange-capabilities --client erigon

# Run with a specific test pattern
./hive --sim ethereum/engine --sim.limit=".*cancun.*" --sim.parallelism=4 --client erigon

# Run EEST engine simulator
./hive --sim ethereum/eels/consume-engine --sim.limit="" --client erigon \
  --sim.buildarg fixtures=https://github.com/ethereum/execution-spec-tests/releases/download/v5.3.0/fixtures_develop.tar.gz
```

## Suites Covered

| Suite | Description |
|-------|-------------|
| `engine/exchange-capabilities` | Engine API capability negotiation |
| `engine/withdrawals` | Withdrawals support |
| `engine/cancun` | Cancun/EIP-4844 blob txs |
| `engine/api` | Engine API conformance |
| `engine/auth` | JWT authentication |
| `rpc-compat` | RPC compatibility with reference |
| `eels/consume-engine` | EEST execution spec tests |
| `eels/consume-engine` (amsterdam) | EIP-7928 BAL Amsterdam fixtures |

## Related Skill

For an **interactive, ephemeral hive workflow** (handles versioned EEST fixtures, Dockerfile setup, BAL workarounds, per-suite parallelism), use the `/hive-test` skill instead:
```
/hive-test eest-bal          # run BAL amsterdam tests
/hive-test engine rpc-compat # run engine + rpc suites
/hive-test all               # run everything
```
The `/hive-test` skill also covers detailed cleanup via `./hive --cleanup` + `docker image prune -f`.

This skill (`erigon-test-hive`) documents the Makefile targets for reference — use `/hive-test` when you need a full guided run.

## CI Equivalent

| Local command | CI workflow | File |
|---------------|-------------|------|
| `make test-hive` | Test Hive | `test-hive.yml` |

To dispatch remotely on a branch:
```bash
gh workflow run "Test Hive" --ref <branch>
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `act command not found` | Install from https://nektosact.com/installation/index.html |
| `GITHUB_TOKEN` not set | `export GITHUB_TOKEN=$(gh auth token)` |
| Docker build fails | Ensure `make erigon` succeeded first (builds binary used in Docker image) |
| Hive clone fails | Check network; hive is cloned fresh each run into `temp/` |
| EEST fixtures 404 | Check `EEST_VERSION` in Makefile; fixtures URL must match a released tag |
| Disk full / OOM | Run `docker system prune -af --volumes` — previous hive runs left dangling images/containers |
| Containers stuck running | `docker ps --filter "name=hive" -q \| xargs -r docker stop` |
