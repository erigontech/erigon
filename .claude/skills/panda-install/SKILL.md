---
name: panda-install
description: Install + configure EthPandaOps Panda CLI/MCP server on a fresh server. Use when the user wants Claude or panda CLI to query Xatu ClickHouse, Prometheus, Loki, or Eth nodes via the hosted ethpandaops proxy. The install requires Docker and an interactive GitHub OAuth flow.
allowed-tools: Bash, Read, Write
---

# Install + configure EthPandaOps Panda

[Panda](https://github.com/ethpandaops/panda) is a CLI + MCP server +
sandboxed Python runtime that gives a single authenticated entry point
to the EthPandaOps data ecosystem (Xatu ClickHouse, Prometheus, Loki,
Eth nodes). Install it once per server; both `panda` CLI and Claude's
MCP integration use the same local server.

## When to use this skill

- The user wants to query Xatu data, Prometheus metrics, Loki logs, or
  Ethereum-node data from their environment.
- They've asked to enable Claude's `ethpandaops-panda` MCP integration.
- They need this on a new server (it's per-host: each server runs its
  own local Panda server pointing at the shared hosted proxy).

## Pre-flight checks

```bash
docker --version            # must be 20+ with Compose plugin
docker compose version      # must succeed (Compose v2)
which curl                  # required for the install script
```

If Docker is missing, abort and tell the user to install Docker first
— Panda's runtime is Docker-based.

## Install

```bash
curl -sSfL https://raw.githubusercontent.com/ethpandaops/panda/master/scripts/install.sh | sh
```

This installs the `panda` CLI to `~/.local/bin/`. Make sure `~/.local/bin`
is on PATH:

```bash
echo $PATH | grep -q "$HOME/.local/bin" || echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
```

## Initialize (hosted proxy mode)

```bash
panda init
```

This is **interactive**:
1. Pulls the panda Docker images (~500 MB).
2. Writes a default config at `~/.config/panda/config.yaml`.
3. Opens a browser for GitHub OAuth against `panda-proxy.ethpandaops.io`.
4. Starts the local panda server on port 2480.

The OAuth flow needs:
- A GitHub account that's been allowlisted by EthPandaOps for the
  hosted proxy. If the user isn't on the allowlist, the auth step
  fails — they'll need to coordinate with EthPandaOps.
- A browser. If running on a headless server, the `panda init` flow
  prints a URL the user can open from any machine; the callback is
  device-flow style.

## Self-host the proxy (alternative — for orgs with their own credentials)

If the org has its own ClickHouse/Prometheus/Loki credentials and
prefers not to depend on the hosted proxy:

```bash
mkdir -p ~/panda-proxy && cat > ~/panda-proxy/proxy-config.yaml <<'EOF'
server:
  listen_addr: ":18081"
auth:
  mode: none
clickhouse:
  - name: my-cluster
    host: "<host>"
    port: 8443
    database: default
    username: "<user>"
    password: "<pass>"
    secure: true
EOF

docker run -d --name panda-proxy -p 18081:18081 \
  -v ~/panda-proxy/proxy-config.yaml:/config/proxy-config.yaml:ro \
  --entrypoint /app/panda-proxy \
  ethpandaops/panda:server-latest \
  --config /config/proxy-config.yaml
```

Point the panda CLI at it via `~/.config/panda/config.yaml` (set
`proxy.url` to `http://localhost:18081`). Then `panda init` skips the
OAuth flow.

See [proxy-config.example.yaml](https://github.com/ethpandaops/panda/blob/master/proxy-config.example.yaml)
for the full schema (Prometheus, Loki, ethnodes too).

## Verify

```bash
panda server status         # should report "running"
panda datasources           # lists available datasources via the proxy
panda execute --code 'print("hello from sandbox")'
```

If `panda datasources` returns nothing, the proxy auth or proxy URL is
wrong. Common issues:
- GitHub identity not on the allowlist → ask the user to coordinate
  with EthPandaOps.
- Stale token → `panda auth refresh` (check `panda --help` for the
  exact subcommand on the installed version).
- Docker container exited → `docker logs panda-server` for the trace.

## Wire into Claude Code (MCP)

Edit `~/.claude.json` to add the MCP server entry:

```json
{
  "mcpServers": {
    "ethpandaops-panda": {
      "type": "http",
      "url": "http://localhost:2480/mcp"
    }
  }
}
```

Restart Claude Code (or whatever MCP client). The tools `execute_python`,
`manage_session`, and `search` should appear under the `ethpandaops-panda`
namespace.

## What Panda exposes (datasources)

| datasource | what's there |
|---|---|
| ClickHouse `xatu` | Beacon-chain data lake (blocks, attestations, validators, etc.) |
| Prometheus `primary` | Time-series metrics from EthPandaOps infrastructure |
| Loki `primary` | Logs from EthPandaOps infrastructure |
| `ethnode` | Direct Ethereum node access (read-only RPC) |

**Benchmarkoor results** are not (as of 2026-05-10) a directly named
datasource. The benchmarkoor service stores output in S3
(`repricings/results`) gated by the same GitHub OAuth. Check via
Panda whether they've been ingested into Prometheus or ClickHouse:

```python
# Inside `panda execute --code '...'`:
import panda
# Try Prometheus
print(panda.prometheus("primary").query("benchmarkoor_test_duration_seconds"))
# Try ClickHouse table listing
print(panda.clickhouse("xatu").execute("SHOW TABLES LIKE '%benchmark%'"))
```

If neither returns results, file an EthPandaOps request to ingest
benchmarkoor results into one of Panda's datasources. Without that,
benchmarkoor data still requires the gated HTTP API.

## Troubleshooting

- **`panda init` hangs at OAuth**: the browser callback URL may not be
  reachable from the auth issuer. Use the device-flow alternative:
  `panda auth login --device`.
- **Docker pull fails behind proxy**: configure Docker daemon proxy
  settings before running `panda init`.
- **Multiple servers, single user**: the OAuth flow is per-server.
  Each server needs its own `panda init` against the same GitHub
  identity. Consider a self-hosted proxy if managing many servers
  becomes painful.
- **MCP tools don't appear in Claude**: confirm `panda server status`
  reports running; confirm port 2480 is reachable from where Claude
  runs (same machine OK; different machine = need to expose port).

## Cross-check before considering install successful

```bash
panda server status                    # running
panda datasources                      # at least 1 datasource listed
panda execute --code 'print("ok")'    # prints "ok"
```

If all three pass, the skill's job is done. Hand the user the list of
available datasources so they know what to query next.
