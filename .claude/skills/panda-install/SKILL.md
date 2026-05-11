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

## Step 1 — Choose install mode (REQUIRED PROMPT)

Before any install action, ask the user via `AskUserQuestion` which
mode they want — the answer drives every later step:

| option | when to choose |
|---|---|
| **Hosted proxy** | User has an EthPandaOps-allowlisted GitHub identity. Easiest path. |
| **Self-hosted proxy** | User's org has its own ClickHouse / Prometheus / Loki credentials and prefers not to depend on the EthPandaOps proxy. |
| **Skip the proxy step** | Just install the CLI for now; configure later. |

Question to ask:

```
header: "Panda mode"
question: "Which Panda install mode do you want?"
options:
  - Hosted proxy (recommended, requires EthPandaOps GitHub allowlist)
  - Self-hosted proxy (org has own ClickHouse/Prometheus/Loki credentials)
  - Skip proxy step (install CLI only)
```

If "Hosted proxy" → continue to step 2.
If "Self-hosted proxy" → ask the credential prompts in step 4.
If "Skip" → install the CLI only, skip `panda init`.

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

## Step 3 — Initialize (hosted proxy mode)

Run interactively in foreground (NOT via `run_in_background` — the
OAuth flow needs the user to copy a URL):

```bash
panda init
```

This:
1. Pulls the panda Docker images (~500 MB).
2. Writes a default config at `~/.config/panda/config.yaml`.
3. Prints a GitHub OAuth URL — opens browser if a display is
   available, else prints a device-flow URL the user opens from any
   browser.
4. Starts the local panda server on port 2480.

**REQUIRED OAUTH HANDOFF — two-step format**: the device-flow URL and
code MUST be presented as plain markdown FIRST so they render as a
clickable link / copyable code in the user's UI. Then ask a minimal
question with NO embedded URL/code (AskUserQuestion can't reliably
render long URLs or distinct copyable strings).

Step 1 — output the URL + code as a markdown text message (regular
output, not a tool call):

```
**URL:** <paste the dex URL from panda auth output>
**Code:** `<paste the code from panda auth output>`

Steps in browser:
1. Click the URL above
2. Sign in with the EthPandaOps-allowlisted GitHub identity
3. Paste the code when prompted
4. Approve the access

The `panda auth login` process is sitting waiting — it will complete
on its own when the browser side is done.
```

Step 2 — minimal `AskUserQuestion` for the next step:

```
header: "Status"
question: "OAuth done?"
options:
  - Done — verify now
  - Need a fresh code
  - Hit an error (tell me what the browser said)
```

Why two steps: AskUserQuestion option labels are short (≤5 words each)
and the question text doesn't render long URLs as clickable in most
clients. Putting the URL/code in a plain message first lets the user
click + copy with their normal terminal/UI affordances.

If OAuth fails (user not allowlisted, network issue, expired code),
fall back via:

```
header: "Allowlist?"
question: "OAuth failed — GitHub identity may not be allowlisted. What next?"
options:
  - Request allowlist (re-run skill when granted)
  - Switch to self-hosted proxy
  - Install CLI only
```

**Recognising the allowlist failure mode**: the symptom is GitHub
returning `bad_verification_code` during the code-exchange step.
That's NOT an expired code — it's GitHub refusing to issue a token
because the user can't access the upstream OAuth app
(`panda-proxy`). Distinguish from a genuinely expired code by
checking that the failure is immediate after device-flow approval,
not after a multi-minute wait.

When the user picks "Request allowlist", give them the request
template (don't make them assemble it themselves):

| field | value |
|---|---|
| GitHub identity | <ask the user for their GH username> |
| Service | panda-proxy (`panda-proxy.ethpandaops.io`) |
| Dex issuer | `https://dex.primary.production.platform.ethpandaops.io` |
| OAuth client ID | `panda-proxy` |
| Reason | <user's project + team> |
| Reproducer | `panda auth login --no-browser` returns `bad_verification_code` after entering the device code |

Where to send: EthPandaOps Slack/Discord, or GitHub issue on
`ethpandaops/panda`. Visible maintainers in the proxy config's
success-page rules: `samcm`, `mattevans`.

Also stop the local panda server while waiting — it will flap
because the proxy embedding backend is unreachable without auth:

```bash
panda server stop
```

## Step 4 — Self-host the proxy (alternative path)

For orgs with their own ClickHouse / Prometheus / Loki credentials.

**REQUIRED PROMPT — gather credentials structurally** via
`AskUserQuestion`. Multiple questions in one batch:

```
question: "Which datasources do you want to configure on the
self-hosted proxy?"
multiSelect: true
header: "Datasources"
options:
  - ClickHouse (e.g. Xatu)
  - Prometheus
  - Loki
  - Ethereum node (RPC)

question: "Provide the ClickHouse host:port (leave empty if N/A)"
header: "ClickHouse host"
options:
  - localhost:8443
  - clickhouse.internal:8443
  (user will likely select Other and supply their host)

question: "ClickHouse credentials — username only here; ask for the
password separately to keep it off the question prompt"
header: "ClickHouse user"
options:
  - default
  - readonly
```

For SECRETS (passwords, tokens), do NOT use `AskUserQuestion` (it logs
to telemetry). Instead, prompt the user to set environment variables
and reference them in the config:

```bash
echo 'export CH_PASSWORD="..."' >> ~/.bashrc       # user does this
echo 'export PROM_PASSWORD="..."' >> ~/.bashrc     # if using Prometheus
echo 'export LOKI_PASSWORD="..."' >> ~/.bashrc     # if using Loki
```

Then write the proxy config that references the env vars:

```bash
mkdir -p ~/panda-proxy && cat > ~/panda-proxy/proxy-config.yaml <<EOF
server:
  listen_addr: ":18081"
auth:
  mode: none
clickhouse:
  - name: my-cluster
    host: "${CH_HOST:-localhost}"
    port: ${CH_PORT:-8443}
    database: ${CH_DB:-default}
    username: "${CH_USER:-default}"
    password: "${CH_PASSWORD}"
    secure: true
EOF

docker run -d --name panda-proxy -p 18081:18081 \
  -e CH_PASSWORD -e PROM_PASSWORD -e LOKI_PASSWORD \
  -v ~/panda-proxy/proxy-config.yaml:/config/proxy-config.yaml:ro \
  --entrypoint /app/panda-proxy \
  ethpandaops/panda:server-latest \
  --config /config/proxy-config.yaml
```

Point the panda CLI at it via `~/.config/panda/config.yaml`:

```yaml
proxy:
  url: "http://localhost:18081"
```

Then `panda init` skips the OAuth flow.

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
