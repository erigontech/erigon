# Erigon TUI

Erigon TUI (`etui`) is a terminal interface for one concrete workflow:

1. Start with an empty data directory.
2. Run a short setup wizard.
3. Save the node configuration.
4. Start Erigon from the TUI.
5. Monitor the node and control it from the same screen.

This README describes only that flow.

## What This Version Does

`etui` is designed for a fresh local install where you want to bootstrap and monitor a single Erigon node.

Current first-run behavior:

- If the selected data directory does not contain `etui.toml`, `etui` opens the setup wizard.
- The wizard collects:
  - network
  - data directory
  - node type / pruning mode
- On the last step, `Save & Start` writes the config and starts Erigon.
- After startup, `etui` opens the monitoring dashboard directly.
- If the node is already running for that data directory, `etui` skips the wizard and opens the monitor.

## Build

Build the Erigon binary first:

```bash
make erigon
```

Run `etui` from the repository:

```bash
go run ./cmd/etui --datadir /path/to/fresh-datadir
```

Or build and run the TUI binary:

```bash
go build -o ./build/bin/etui ./cmd/etui
./build/bin/etui --datadir /path/to/fresh-datadir
```

For a clean first-run test, use a new empty directory:

```bash
mkdir -p /tmp/erigon-etui-fresh
go run ./cmd/etui --datadir /tmp/erigon-etui-fresh
```

## First-Run Setup

On an empty data directory, the wizard shows four steps:

1. Choose Network
2. Choose Data Directory
3. Choose Node Type
4. Review & Confirm

The final page displays:

- selected chain
- selected data directory
- pruning mode
- RPC setting
- private API address
- diagnostics URL
- destination config file

Press `Enter` on the last step to `Save & Start`.

The generated node config is stored in:

```text
<datadir>/etui.toml
```

The global last-used data directory is stored in the platform user config directory under:

```text
etui/etui.toml
```

## Monitoring UI

After the node starts, `etui` opens the monitoring dashboard.

Main areas:

- node status
- sync status
- downloader progress
- system health
- alerts
- log tail

Main controls:

- `◄` / `►`: cycle pages
- `L`: open logs
- `F4`: open validator page
- `R`: start or stop the node in standalone mode
- `C`: open the config dialog
- `q` or `Ctrl+C`: quit `etui`

Current page order for `◄` / `►`:

1. Node
2. Logs
3. Validator

## Node Startup Behavior

When `etui` starts Erigon, it launches the local `erigon` binary from `./build/bin/erigon` if present, or the sibling binary next to the running `etui` executable.

The TUI currently starts Erigon with:

- `--datadir <path>`
- `--chain <chain>` if configured
- `--pprof`

`--pprof` is required because the TUI reads downloader progress from the debug HTTP server.

## Validator Page

The validator page is optional.

It becomes useful only when:

- local validator keys are present in the data directory tree
- the Beacon API is available

Default Beacon API URL:

```text
http://localhost:5555
```

If no validator keys are configured, the page shows:

```text
No validator keys configured
```

The header role indicator shows:

- `Full+RPC` when validator data is not available
- `Validator` when validator data is detected

## Notes About Progress Endpoints

Downloader progress in `etui` depends on the Erigon debug HTTP endpoints exposed by the current build.

If downloader progress does not appear correctly:

1. rebuild Erigon:

```bash
make erigon
```

2. restart the node from `etui`

3. restart `etui`

`etui` can probe common local debug endpoints automatically, but it still depends on the running Erigon binary exposing the required downloader route.

## Windows

`etui` builds on Windows.

Current Windows support includes:

- node control
- logs
- downloader panel
- system health widget

Stage database polling is currently disabled in Windows builds, so the stage progress sections show placeholders instead of live MDBX stage data.

## Recommended Local Test Flow

```bash
make erigon
go run ./cmd/etui --datadir /tmp/erigon-etui-fresh
```

Then:

1. pick a network
2. keep or change the data directory
3. choose the node type
4. press `Enter` on `Save & Start`
5. wait for the monitor to switch to the node dashboard

## Scope

This README intentionally does not document every internal flag or every monitoring mode.

It documents the supported product flow for this version:

- fresh data directory
- wizard-based setup
- local Erigon startup
- TUI-based monitoring
