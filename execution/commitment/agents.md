# Commitment (Patricia Trie State Root)

This package computes Ethereum state root hashes using a hex Patricia trie over flat KV storage.

## Architecture

### Core Components

- `HexPatriciaHashed` — main trie processor. Receives sorted account/storage key updates, walks the trie grid, and produces the new state root hash along with branch node updates.
- `PatriciaContext` — interface for reading branch nodes, accounts, and storage from the underlying DB layer. Implementations live in `commitmentdb/`.
- `CachingPatriciaContext` — shared, concurrent read-through cache wrapping `PatriciaContext`. Uses three `maphash.Map` instances (branches, accounts, storage) for lock-free concurrent access. Created once per commitment cycle, shared between warmup workers and `Process()`.
- `TrieReader` — stateless, read-only trie navigator. Descends from root to leaf by hashed key without mutating the grid. Supports a `BranchVisitor` callback for prefetching during traversal.
- `Warmuper` — parallel pre-warming of MDBX page cache. Spawns N workers, each with a `TrieReader` backed by a `CachingPatriciaContext.Wrap(workerCtx)` view. Warmup results are cached and reused by the subsequent `Process()` call.
- `BranchEncoder` — serializes trie branch node updates into the compact `BranchData` format stored in the commitment domain.

### Cache Flow (Commitment Cycle)

```
ComputeCommitment():
  cache := NewCachingPatriciaContext()
  defer cache.Reset()

  // Warmup phase (parallel):
  //   N workers, each with TrieReader + cache.Wrap(workerCtx)
  //   LookupWithVisitor() traverses trie, visitor prefetches accounts/storage
  //   All reads go through cachedView -> populates shared cache

  // Process phase (sequential):
  //   hph.ctx = cache.Wrap(mainCtx)
  //   Process() reads hit warmed cache entries (cache hits)
  //   PutBranch() writes pass through to DB and invalidate cache
```

### Key Files

| File | Purpose |
|------|---------|
| `commitment.go` | `PatriciaContext` interface, `BranchData`, `Update`, `HashSort()`, `BranchEncoder` |
| `hex_patricia_hashed.go` | `HexPatriciaHashed.Process()` — trie grid walk, state root computation |
| `caching_patricia_context.go` | `CachingPatriciaContext`, `cachedView`, cache hit/miss counters |
| `trie_reader.go` | `TrieReader.Lookup()`, `LookupWithVisitor()`, `BranchVisitor` callback |
| `warmuper.go` | `Warmuper`, `WarmupConfig`, parallel TrieReader-based warmup |
| `recording_context.go` | `RecordingPatriciaContext` — wraps PatriciaContext for execution witness tracing |
| `trie_trace.go` | Trie operation tracing/debugging |
| `verify.go` | Commitment verification utilities |
| `metrics.go` | Prometheus metrics for trie processing |

### Important Interfaces

```go
// PatriciaContext provides read/write access to trie data
type PatriciaContext interface {
    Branch(prefix []byte) ([]byte, kv.Step, error)
    Account(plainKey []byte) (*Update, error)
    Storage(plainKey []byte) (*Update, error)
    PutBranch(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error
}

// BranchVisitor is called at each branch node during TrieReader traversal
type BranchVisitor func(depth int, cell *cell) error
```

### Testing

```bash
go test ./execution/commitment/...              # all tests
go test -run TestCachingPatriciaContext ./execution/commitment/...  # cache tests
go test -bench BenchmarkCaching ./execution/commitment/...         # cache benchmarks
go test -race ./execution/commitment/...         # race detector
```
