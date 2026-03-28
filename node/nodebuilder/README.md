# Node Component Builder

`nodebuilder` is the central inventory of extracted node components.

`backend.go` (`node/eth/`) is a ~1 300-line constructor that builds the entire Erigon
node inline. The componentization effort moves each subsystem into its own
`node/components/<name>/` package — a self-contained *Provider* with a stable
Configure → Initialize → Start lifecycle — and registers it here.

The registry has three jobs:

1. **One field** replaces N provider fields on the `Ethereum` struct.
2. **Unified Start / Close** replaces per-provider calls scattered through `Ethereum.Start()` and `Ethereum.Stop()`.
3. **Inventory** — the registry's field list is a live record of what has been
   extracted vs what is still inline in `backend.go`.

---

## Provider lifecycle

Every provider follows the same three-phase lifecycle:

```
Configure(static config, logger)
    ↓
Initialize(ctx, runtime deps)   ← deps from other already-constructed things
    ↓
Start(ctx, eg)                  ← optional; only if the provider owns goroutines
```

`Close()` is called by `Builder.Close()` during node shutdown.

Configure and Initialize are called inline in `backend.go` because each provider
needs different deps (there is no single dependency graph yet). The registry is
responsible for Start and Close only, since those have uniform signatures.

---

## How to extract a new component

### Step 1 — Create the provider package

```
node/components/<name>/
    provider.go          ← Provider struct + Configure + Initialize + Start/Close
    provider_test.go     ← unit tests for lifecycle and helper logic
```

**Provider struct layout:**

```go
// Package <name> provides the <Name>Provider …
package <name>

// Provider holds the runtime state for <subsystem>.
// Lifecycle: Configure → Initialize → Start (→ Close).
type Provider struct {
    // Public outputs — set by Initialize; read by callers via registry.
    Client SomeInterface // always set after Initialize (or disabled sentinel)

    // Private — set by Configure; used by Initialize.
    cfg    Config
    logger log.Logger
}

func (p *Provider) Configure(cfg Config, logger log.Logger) { … }

// Deps are the runtime dependencies that are only known after Configure.
type Deps struct {
    ChainDB kv.TemporalRoDB
    // …
}

func (p *Provider) Initialize(ctx context.Context, deps Deps) error { … }

// Start launches background goroutines. Omit if none are needed.
func (p *Provider) Start(ctx context.Context, eg ErrGroup) { … }

// Close releases resources. Omit if nothing to release.
func (p *Provider) Close() { … }

// IsEnabled returns false when the subsystem is disabled by config.
// Callers should check this before accessing Pool / Client / etc.
func (p *Provider) IsEnabled() bool { … }
```

Rules:
- Public outputs (the things consumers use) go at the top of the struct.
- Config and logger are private; deps are passed to Initialize, not Configure.
- If the subsystem can be disabled, return a no-op sentinel from Initialize so
  callers never need to nil-check (see `txpool.GrpcDisabled` as an example).
- Do not import `backend.go` or anything from `node/eth/`. The flow is one-way.

### Step 2 — Register in the registry

In [builder.go](builder.go):

```go
// 1. Import the new package.
import (
    <name>comp "github.com/erigontech/erigon/node/components/<name>"
    // existing imports …
)

// 2. Add a typed field to Builder.
type Builder struct {
    Downloader *downloadercomp.Provider
    TxPool     *txpoolcomp.Provider
    Shutter    *txpoolcomp.ShutterProvider
    <Name>     *<name>comp.Provider   // ← add here
}

// 3. Allocate in New().
func New() *Builder {
    return &Builder{
        Downloader: &downloadercomp.Provider{},
        TxPool:     &txpoolcomp.Provider{},
        Shutter:    &txpoolcomp.ShutterProvider{},
        <Name>:     &<name>comp.Provider{},   // ← add here
    }
}

// 4. If the provider has a Start method, add it to Builder.Start().
func (r *Builder) Start(ctx context.Context, eg ErrGroup) {
    r.TxPool.Start(ctx, eg)
    r.Shutter.Start(ctx, eg)
    r.<Name>.Start(ctx, eg)   // ← add here
}

// 5. If the provider has a Close method, add it to Builder.Close().
func (r *Builder) Close() {
    r.Downloader.Close()
    r.<Name>.Close()   // ← add here
}
```

Update the inventory comment at the top of `Builder` to reflect what has been
extracted and what remains.

### Step 3 — Wire in backend.go

Replace the inline construction in `node/eth/backend.go` with calls through the
registry. The pattern is always:

```go
// Before (inline, scattered):
backend.<name>Provider = &<name>comp.Provider{}
backend.<name>Provider.Configure(cfg.<Name>, logger)
if err = backend.<name>Provider.Initialize(ctx, <name>comp.Deps{…}); err != nil {
    return nil, err
}
backend.<name>OutputField = backend.<name>Provider.OutputField

// After (through registry, Configure/Initialize still inline):
backend.components.<Name>.Configure(cfg.<Name>, logger)
if err = backend.components.<Name>.Initialize(ctx, <name>comp.Deps{…}); err != nil {
    return nil, err
}
backend.<name>OutputField = backend.components.<Name>.OutputField
```

Remove the now-redundant provider field from the `Ethereum` struct.

**Alias fields** (`txPoolGrpcServer`, `downloaderClient`, …) can be kept during
the transition if they are referenced in many places — they are just shortcuts
set once from registry output. Remove them in a follow-up once the provider is
stable and callers are comfortable with `backend.components.<Name>.Output`.

### Step 4 — Verify

```bash
go build ./node/...          # must pass
go test ./node/nodebuilder/ # registry unit tests must pass
go test ./node/components/<name>/... # provider unit tests must pass
make lint                    # must pass
```

---

## Current inventory

| Component | Package | Status |
|-----------|---------|--------|
| Downloader | `node/components/downloader` | ✅ Extracted |
| TxPool | `node/components/txpool` | ✅ Extracted |
| Shutter (encrypted mempool) | `node/components/txpool` | ✅ Extracted |
| Sentry / P2P | — | ⬜ Inline in backend.go |
| ExecModule / StagedSync | — | ⬜ Inline in backend.go |
| RPC servers / clients | — | ⬜ Inline in backend.go |
| Mining | — | ⬜ Inline in backend.go |
| Caplin (embedded CL) | — | ⬜ Inline in backend.go (has own stage machine) |

---

## Alias fields (technical debt)

After extraction, some fields remain on the `Ethereum` struct as aliases set
once from registry output:

| Field | Set from | Referenced in |
|-------|----------|---------------|
| `txPoolGrpcServer` | `components.TxPool.GrpcServer` | 3 places in backend.go |
| `downloaderClient` | `components.Downloader.Client` | 5 places in backend.go + stage construction |

These exist to avoid a larger diff during the initial extraction PR. Remove them
when the component is mature and every reference is comfortable going through
`backend.components.<Name>.<Field>` directly.
