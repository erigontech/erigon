---
name: deploy-epbs-devnet
description: "Build and redeploy erigon to an epbs devnet. Usage: /deploy-epbs-devnet <N> (e.g. /deploy-epbs-devnet 0). Handles build, stop, start, verify."
allowed-tools: Bash, Read, Glob, Grep
allowed-prompts:
  - tool: Bash
    prompt: build, deploy, and restart erigon on epbs devnets
---

# Deploy ePBS Devnet

Build erigon and redeploy to an ePBS devnet. One-command deploy cycle.

## Usage

```
/deploy-epbs-devnet 0     # deploy to epbs-devnet-0
/deploy-epbs-devnet 1     # deploy to epbs-devnet-1 (when available)
```

The argument is the devnet number N. If omitted, default to 0.

## Step 0: Load Config

Read the config file at:
```
/root/work/erigon/.claude/skills/devnet/configs/epbs-devnet-<N>.yaml
```

Extract: `name`, `workdir`, `branch`, `el_http`, `cl_beacon_api`, `stop_command`.

## Step 1: Pre-flight checks

```bash
git branch --show-current
```

Warn if not on expected `branch`. Ask user to confirm if different.

Verify start script exists:
```bash
ls $WORKDIR/start-erigon.sh
```

If not, tell user to run `/launch-epbs-devnet <N>` first.

## Step 2: Build

```bash
cd /root/work/erigon && make erigon
```

If build fails, stop and report. Do NOT stop the old process.

## Step 3: Stop old process

Only after successful build:

```bash
pkill -f "datadir.*${NAME}/erigon-data" 2>/dev/null
sleep 2
pgrep -f "datadir.*${NAME}/erigon-data" && echo "STILL RUNNING - force kill" && pkill -9 -f "datadir.*${NAME}/erigon-data" || echo "Stopped"
```

## Step 4: Start new process

```bash
cd $WORKDIR && nohup bash start-erigon.sh > erigon-console.log 2>&1 &
```

## Step 5: Verify startup (wait 5 seconds)

```bash
sleep 5
```

Then check:

1. **Process running**: `pgrep -fa "datadir.*${NAME}/erigon-data"`
2. **Port binding**: `ss -tlnp | grep -E '${el_http}|${cl_beacon_api}'`
3. **Initial logs**: `tail -30 $WORKDIR/erigon-console.log`
4. **No panic**: `grep -i "panic" $WORKDIR/erigon-console.log | tail -5`

## Step 6: Report

```
== Deploy Result ==
Devnet:    $NAME
Build:     SUCCESS
Old proc:  STOPPED
New proc:  RUNNING (PID xxxxx)
Ports:     ${el_http} (EL RPC) OK, ${cl_beacon_api} (Beacon API) OK
Panic:     none
Status:    DEPLOYED
```

## Notes

- Does NOT run `make lint`. Run lint separately before committing.
- Does NOT wipe datadir. Use `bash $WORKDIR/clean.sh` for clean start.
- Build takes ~1-2 minutes.
