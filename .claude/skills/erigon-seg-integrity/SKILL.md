---
name: erigon-seg-integrity
description: Run integrity checks on Erigon datadirs using the 'erigon seg integrity' command. Use this when the user wants to verify snapshot/segment file integrity or during the snapshot publishing process.
allowed-tools: Bash, Read
---

# Erigon Segment Integrity Check

The `erigon seg integrity` command runs integrity checks on an Erigon datadir to verify that snapshot and segment files are valid and consistent.

## Prerequisites

1. **Erigon must be stopped** - The command requires exclusive access to the datadir
2. **Binary must be built** - Run `/erigon-build` first if the binary doesn't exist
3. **Datadir must exist** - A synced Erigon datadir with blockchain data

## Command Syntax

```bash
./build/bin/erigon seg integrity --datadir=<path>
```

Replace `<path>` with the actual path to your Erigon data directory.

## Available Flags

- `--check` — Comma-separated list of specific checks to run (default: all default checks)
- `--skip-check` — Comma-separated list of checks to exclude from the run
- `--failFast` — Stop after the first problem is found (default: `true`). Set to `false` to continue and collect all warnings

## Discovering Available Checks

The list of available checks is **dynamic and changes over time** as new checks are added or removed. Do NOT assume a fixed list. Instead, discover the current checks by running:

```bash
./build/bin/erigon seg integrity --help
```

Look at the `--check` flag description in the output — it lists all currently available check names.

## Workflow

When the user wants to run integrity checks:

1. **Ask for the datadir path** if not already provided
2. **Verify prerequisites**
   - Check the datadir exists
   - Ensure Erigon is not running (file lock will prevent concurrent access)
   - Build the binary if needed
3. **Discover available checks** by running `--help` if the user wants to run or skip specific checks
4. **Execute the command** with the appropriate flags
5. **Interpret the output** — report any errors or warnings found

## Examples

### Run all default checks
```bash
./build/bin/erigon seg integrity --datadir=/data/erigon
```

### Run a specific check only
First discover available checks via `--help`, then:
```bash
./build/bin/erigon seg integrity --datadir=/data/erigon --check=<check_name>
```

### Skip a specific check
```bash
./build/bin/erigon seg integrity --datadir=/data/erigon --skip-check=<check_name>
```

### Continue past failures instead of stopping
```bash
./build/bin/erigon seg integrity --datadir=/data/erigon --failFast=false
```

## Important Notes

- **File Lock**: If Erigon is running, the command will fail due to file lock
- **Long running**: Integrity checks can take significant time on large datadirs, especially mainnet
- **Discover checks dynamically**: Always use `--help` to find the current list of available checks rather than assuming fixed names
