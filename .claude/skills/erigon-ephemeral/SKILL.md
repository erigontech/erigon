---
name: erigon-ephemeral
description: Run an ephemeral Erigon instance with a temporary datadir. Checks for port conflicts, applies port offsets, and supports safe cleanup. Use this when the user wants a temporary/throwaway Erigon instance.
allowed-tools: Bash, Read
allowed-prompts:
  - tool: Bash
    prompt: create, write, and delete files in the system default temp directory
---

# Ephemeral Erigon Instance

Spin up a temporary Erigon instance with its datadir in the system's default temp directory. Handles port conflicts automatically and supports safe cleanup.

## Workflow

Follow these steps in order:

### Step 1: Build Erigon

Check if `./build/bin/erigon` exists. If it does not exist or the user requests a fresh build, invoke the `/erigon-build` skill. Otherwise skip this step.

### Step 2: Create Datadir

There are two modes: **empty** (default) or **clone** (if the user provides a source datadir to clone from).

#### Mode A: Empty datadir (default)

**Always** use `mktemp -d` to create the datadir. This guarantees the directory does not already exist (mktemp creates a new unique directory atomically). Never construct the path manually or use `mkdir`.

```bash
mktemp -d -t erigon-ephemeral
```

This uses the system's default temp directory (e.g., `/var/folders/...` on macOS, `/tmp` on Linux). Capture the output — that is the datadir path. Report the full path to the user so they know where data is stored.

#### Mode B: Clone an existing datadir

If the user provides a source datadir path, clone it into an ephemeral directory using the `erigon-datadir` skill's **Duplicating a Datadir** procedure. Use `mktemp -u` to generate a unique path without creating it (so the `erigon-datadir` destination-not-exist precondition is satisfied):

```bash
mktemp -u -d -t erigon-ephemeral
```

Then follow the `erigon-datadir` skill with source=`<user-provided-path>` and destination=the generated path. It handles APFS detection, CoW cloning, precondition checks, and post-copy verification.

### Step 3: Port Conflict Detection

**Never use default ports.** Always start with offset +100 to avoid conflicts with any existing Erigon instance using defaults. Check the offset ports for conflicts:

```bash
lsof -nP -iTCP:<port> -sTCP:LISTEN 2>/dev/null
```

Also check UDP ports where applicable (torrent, devp2p, caplin discovery):

```bash
lsof -nP -iUDP:<port> 2>/dev/null
```

#### Port Table

| CLI Flag | Default | +100 | +200 | +300 |
|----------|---------|------|------|------|
| `--private.api.addr` | `127.0.0.1:9090` | `127.0.0.1:9190` | `127.0.0.1:9290` | `127.0.0.1:9390` |
| `--http.port` | `8545` | `8645` | `8745` | `8845` |
| `--authrpc.port` | `8551` | `8651` | `8751` | `8851` |
| `--ws.port` | `8546` | `8646` | `8746` | `8846` |
| `--torrent.port` | `42069` | `42169` | `42269` | `42369` |
| `--port` | `30303` | `30403` | `30503` | `30603` |
| `--p2p.allowed-ports` | `30303-30307` | `30403-30407` | `30503-30507` | `30603-30607` |
| `--caplin.discovery.port` | `4000` | `4100` | `4200` | `4300` |
| `--caplin.discovery.tcpport` | `4001` | `4101` | `4201` | `4301` |
| `--sentinel.port` | `7777` | `7877` | `7977` | `8077` |
| `--beacon.api.port` | `5555` | `5655` | `5755` | `5855` |
| `--mcp.port` | `8553` | `8653` | `8753` | `8853` |

#### Conditional Ports (only if `--pprof` or `--metrics` in user's extra flags)

| CLI Flag | Default | +100 | +200 | +300 |
|----------|---------|------|------|------|
| `--pprof.port` | `6060` | `6160` | `6260` | `6360` |
| `--metrics.port` | `6061` | `6161` | `6261` | `6361` |

#### Conflict Resolution Strategy

- Start with offset **+100**. Check all +100 ports for conflicts.
- If any +100 port is in use, try +200, then +300.
- Report the chosen port configuration to the user before starting.

#### Port Flag Construction

When an offset is needed, construct the flags as follows (example for offset +100):

```bash
--private.api.addr=127.0.0.1:9190 \
--http.port=8645 \
--authrpc.port=8651 \
--ws.port=8646 \
--torrent.port=42169 \
--port=30403 \
--p2p.allowed-ports=30403,30404,30405,30406,30407 \
--caplin.discovery.port=4100 \
--caplin.discovery.tcpport=4101 \
--sentinel.port=7877 \
--beacon.api.port=5655 \
--mcp.port=8653
```

Note: `--private.api.addr` takes a full `host:port` value. `--p2p.allowed-ports` must list 5 consecutive ports starting from the base `--port` value.

### Step 4: Start Erigon

Run Erigon in the background:

```bash
./build/bin/erigon --datadir=<path> [port flags if needed] [user extra flags] &
```

- The user can pass extra flags (e.g., `--chain=dev --mine`, `--log.console.verbosity=4`) which get appended to the command.
- **Always** print the full CLI command to the user in a formatted code block (one flag per line with `\` continuation) before launching, so they can see exactly what is being run.
- Report the PID to the user after launching.
- Use `run_in_background: true` on the Bash tool so the process survives.

### Step 5: Cleanup

When the user asks to stop/clean up:

1. Ask the user if they want to stop the ephemeral Erigon instance and delete the datadir.
2. If confirmed:
   - Kill the Erigon process by PID (or find it via `pgrep -f <datadir-path>`).
   - Delete the datadir: `rm -rf <datadir-path>`.
   - Report cleanup complete.
3. If declined:
   - Remind the user of the PID and datadir path so they can clean up manually later.

**Note:** Claude Code cannot automatically clean up on session exit. If the user closes the session without cleaning up, the datadir and process will remain. Use Step 6 to find and clean them up.

### Step 6: Leftover Detection

This step can be run independently at any time (the user does not need to be launching a new instance). Check for leftover ephemeral datadirs from previous sessions:

```bash
ls -d "$TMPDIR"erigon-ephemeral.* 2>/dev/null
```

If any are found:
1. List them with their sizes (`du -sh`).
2. For each, check if an Erigon process is still using it (`pgrep -f <dir-name>`).
3. Report findings to the user and ask which to clean up (kill process if still running, then delete).
4. If the user declines, leave them alone.

If none are found, tell the user there are no leftovers.
