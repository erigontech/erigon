# node/components/downloader — Downloader Component

Manages BitTorrent-based snapshot distribution as an Erigon component.

**Status:** Design phase — Provider skeleton exists, backend.go integration pending.

## Component API

The primary interface. Replaces the gRPC-based `downloader.Client` with a direct Go API:

```go
// Provider is the component's public interface.
type Provider struct {
    Downloader *dl.Downloader  // direct access (nil if remote)
    Client     dl.Client       // Download/Seed/Delete operations
}
```

Consumers access the Provider via the component dependency graph — no gRPC needed for in-process usage.

## Events

Replace direct `OnFilesChange` callbacks with component events:

| Event | Published By | Consumed By | Replaces |
|-------|-------------|-------------|----------|
| `SnapshotFilesCreated{Paths []string}` | Storage/BlockReader | Downloader (seeds new files) | `chainDB.OnFilesChange` create callback |
| `SnapshotFilesDeleted{Paths []string}` | Storage/BlockReader | Downloader (removes torrents) | `chainDB.OnFilesChange` delete callback |
| `SnapshotDownloadComplete{}` | Downloader | Stages (unblocks OtterSync) | `afterSnapshotDownload` callback |
| `NewSnapshotAvailable{}` | Downloader | RPC/Notifications | `events.OnNewSnapshot()` |

Events flow through the `node/app/event` bus. The Downloader subscribes to file events and publishes download events.

## Configuration

Owned by the component, not spread across `ethconfig.Config`:

```go
type Config struct {
    // Mode
    Enabled      bool   // --snap.downloader (default: true)
    ExternalAddr string // --snap.downloader.addr (external gRPC, empty = in-process)

    // Torrent settings
    ChainName    string
    Dirs         datadir.Dirs
    TorrentPort  int
    Verbosity    int
    WebSeeds     []string

    // Limits
    DownloadRate *rate.Limit
    UploadRate   *rate.Limit
}
```

## gRPC Wrappers (Optional)

For running the downloader as a separate process:

- **Server**: wraps the Provider as a gRPC server (existing `NewGrpcServer`)
- **Client**: connects to remote downloader (existing `downloadergrpc.NewClient`)

When running in-process (default), gRPC is bypassed entirely — consumers call the Provider directly.

```
In-Process (default):
  Consumer → Provider.Client → Downloader (direct)

Remote (--snap.downloader.addr):
  Consumer → Provider.Client → gRPC Client → [network] → gRPC Server → Downloader
```

## Lifecycle

```
Configure: apply Config, validate settings
Initialize: create Downloader (or connect to remote), set up Client
Activate: start seeding, register for file events
Deactivate: stop seeding, close torrent client, cleanup
```

## Integration with backend.go

Current (monolithic):
```go
backend.setUpSnapDownloader(ctx, ...)  // 50 lines of inline wiring
backend.initDownloader(ctx, ...)       // 30 lines of conditional logic
backend.chainDB.OnFilesChange(...)     // callback registration
```

Target (component):
```go
dlProvider := downloadercomp.New(config)
dlProvider.Initialize(ctx)
domain.Register(dlProvider)
// Events handle the rest — no manual callback wiring
```

## Dependencies

- **Requires:** Storage component (for file events) — OR can accept events from any source
- **No dependency on:** P2P, Consensus, Sync
- **Depended on by:** plugins/snapshot-manager, plugins/ccip, Stages (snapshot download)

## TODO

- [x] Define event types in `node/components/downloader/events.go`
- [ ] Replace `downloader.Client` gRPC dependency with native Go request types
- [ ] Move `OnFilesChange` callback to event subscription
- [ ] Wire into `backend.go` using Provider
- [ ] Add gRPC server/client wrappers as optional mode
- [ ] Integration tests with real torrent client

## Event Bus TODOs (node/app/event)

These are framework-level improvements needed before event-driven components
can be debugged in production:

- [ ] **Event tracing**: Add structured trace logging for ALL event dispatches
  (not just async). Should log: event type, publisher, matched subscribers,
  delivery status. Current: only async handlers log at trace level.
- [ ] **Dead letter queue**: When `Publish()` returns 0 (no subscribers matched),
  log the undelivered event to a dead letter sink. Current: silently dropped.
  Options: log at warn level, write to a ring buffer queryable via debug API,
  or publish to a dedicated dead-letter topic.
- [ ] **Event flow visualization**: debug endpoint or log mode that shows the
  full event routing graph (which components subscribe to which event types).
