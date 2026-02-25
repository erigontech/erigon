# seboost: technical spec

See [objectives.md](objectives.md) for goals, phases, metrics, completion criteria, and artifacts.

## Design constraints

- **Simple**: generate once, minimal chance of bugs.
- **Small**: adding to snapshots or distributing should be cheap.
- **Universal format**: avoid coupling to erigon's internal file formats which evolve over time. No dependency on erigon supporting a deprecated format just because seboost data was written in it.
- **Optional per-block**: blocks with <3 txs are skipped. If a block is missing from seboost, the executor falls back to normal parallel execution (no hints).
- **Correctness is automatic**: stage_exec already does state root verification, so any bad seboost data will surface as a root mismatch.

## Code integration points

### Tx index mapping

Erigon internally uses `txIndex` starting at -1 (system tx), then 0, 1, 2, ... for user txs. VersionedIO stores data with a +1 offset: system tx at array index 0, user tx 0 at array index 1, etc. (`RecordReads/Writes` stores at `txVersion.TxIndex+1`).

`GetDep()` returns `map[int]map[int]bool` using these shifted array indices (loop starts at `i := 1`, deps `j` range from 0 upward).

**Seboost uses 0-based dep indices** where 0 = system tx (txIndex -1), 1 = user tx 0 (txIndex 0), etc. This matches `GetDep()`'s output directly — no translation needed during generation.

At consumption time, `Dependencies()` returns dep indices and the scheduler does `addDependency(depTxIndex+1, i)`. Since seboost stores the same indices as GetDep, and the scheduler already expects this +1 shift, the values flow through correctly. All txs including tx 0 are stored (tx 0 can depend on the system tx).

### Generation side

VersionedIO read/write sets are **already tracked** during production parallel execution — `blockIO.RecordReads()` and `blockIO.RecordWrites()` are called unconditionally for every transaction in `exec3_parallel.go:1317-1396`.

However, `GetDep()` — which computes the dependency map from that tracked data — is only called when the `profile` flag is `true`. In production, `profile` is hardcoded to `false` (`exec3.go:677`). Enabling `profile` also calls `BuildDAG()` which is unnecessary overhead for seboost.

**Generation hook**: when `SEBOOST_GENERATE=txdeps`, add a dedicated seboost gate at `exec3_parallel.go:1619` that calls only `state.GetDep(blockIO)` without `BuildDAG()`. The resulting `map[int]map[int]bool` is the dependency data to serialize. Store the **transitively reduced** set (which is what `GetDep()` already produces via `depsHelper`).

### Consumption side

**Parallel executor scheduling** (`exec3_parallel.go:785-811`):
```
case len(t.Dependencies()) > 0:       → use explicit dep list (seboost txdeps goes here)
case len(execRequest.accessList) != 0: → optimistic scheduling with access lists
default:                                → sender-based ordering fallback
```

**Loading approach**: pre-populate `TxTask.dependencies` during task creation in `exec3.go:641-661`.
- Add a `SetDependencies(deps []int)` method to `TxTask` (the field is private).
- Create a `SeboostReader` that loads seboost files and provides `GetDeps(blockNum uint64) [][]int`.
- In `exec3.go`, after creating each `TxTask`, if seboost data exists for that block, call `txTask.SetDependencies(blockDeps[depIndex])` where `depIndex = txIndex + 1` (mapping txIndex back to the 0-based dep index).
- `Dependencies()` returns pre-set deps unconditionally (the `return t.dependencies` is outside the `dbg.UseTxDependencies` gate). No env var needed for consumption — pre-set deps are always returned.
- If seboost data is missing for a block, `dependencies` stays `nil` → scheduler falls through to sender-based ordering.

**Why this approach**: single change location in `exec3.go`, no Engine interface changes, no wrapper types, graceful degradation. Same pattern BOR uses but with file-backed data instead of header-encoded data.

### Reference: existing dependency flow

From conversation with Mark Holt:
```
There are 3 options [in exec3_parallel.go]:
1. Use the passed in dependency list - where the executor uses this as its initial set of dependencies.
2. Do nothing, for BALs where we just run everything in parallel.
3. Use the sender heuristic from the original e2 parallel exec code.

The executor will optionally also build a set of deps (only done in tests currently).
This is based upon the DAG in versionedio.go which produces map[int]map[int]bool.

TxTask builds dependencies from the header, only implemented in BOR.
If we want customization we can plug into TxTask so it sends the correct set of
dependent indexes on a per TX basis and the parallel processor will schedule accordingly.
```

## Generation pipeline

Three separate passes per phase (to avoid generation overhead skewing benchmark numbers):

1. **Pass 1 — Generate**: run stage_exec with parallel execution + `SEBOOST_GENERATE=txdeps`. Extract deps via `GetDep()` after each block, serialize to seboost files. Write per-block size stats to CSV.
2. **Pass 2 — Baseline**: run stage_exec with parallel execution, no seboost generation or loading. This is the benchmark baseline.
3. **Pass 3 — Seboost**: run stage_exec with parallel execution + `SEBOOST_LOAD=txdeps`. Load deps from seboost files, feed into `TxTask.dependencies`.

- **Input**: synced Sepolia datadir with block/tx data present (no state files needed).
- **Block range**: 0–6M (Sepolia).

## Txdeps storage format

Sequential access only (stage_exec processes blocks in order, one seek per file at most). Two encoding strategies per block, picking whichever is smaller:

### Dep index convention

Deps use 0-based array indices matching `GetDep()` output:
- Index 0 = system tx (txIndex -1)
- Index 1 = user tx 0 (txIndex 0)
- Index k = user tx k-1 (txIndex k-1)

All txs are included in the encoding (tx 0 can depend on system tx).

### Encoding 0: Bitmap (dense graphs)

Lower-triangular bit matrix. For n entries (system tx + user txs), entry `i` can only depend on entry `j` where `j < i`, so the matrix is triangular.

- Size: `n*(n-1)/2` bits = `n*(n-1)/16` bytes.
- Layout: row-major, row `i` has `i` bits (for entries 0..i-1).
- Bit `(i,j)` = 1 means entry `i` depends on entry `j`.

Size examples:
| txs | bitmap bytes |
|-----|-------------|
| 10  | 6           |
| 50  | 154         |
| 100 | 619         |
| 200 | 2,488       |
| 300 | 5,607       |

### Encoding 1: Sparse (few dependencies per tx)

Per-entry dependency lists. For each entry `i` (in order), store:
- `depCount` (varint): number of dependencies
- `dep[0], dep[1], ...` (varints): entry indexes this entry depends on

Size: approximately `n * (1 + avgDeps)` bytes for n ≤ 128 (1-byte varints).

Size examples (n=200):
| avg deps | sparse bytes |
|----------|-------------|
| 2        | 600         |
| 5        | 1,200       |
| 10       | 2,200       |
| 13       | 2,800       |

### Per-block format selection

Crossover: sparse wins when deps are sparse, bitmap wins when deps are dense. For n=200 the crossover is around ~12 avg deps per tx.

**Per-block wire format**:
```
[formatBit: 1 byte (0=bitmap, 1=sparse)] [payload]
```

At generation time, compute both encodings, pick the smaller one, prefix with the format byte.

Note: 1 bit per dependency entry is sufficient — the relation is boolean (entry `i` either depends on entry `j` or it doesn't).

## File layout

- One file per 500k block range (final range may be shorter).
- Filename: `seboost-txdeps-{startBlock}-{endBlock}.bin`
- Location: `<datadir>/seboost_txdeps/`
- Blocks with <3 txs are omitted from the file.

**Per-file format**:
```
[file header]
  magic: 4 bytes ("SBTX")
  version: 1 byte
  startBlock: 8 bytes (uint64 LE)
  endBlock: 8 bytes (uint64 LE)
  blockCount: 4 bytes (uint32 LE)  — number of blocks actually stored (excluding skipped)

[block entries]  — concatenated, no index, sequential scan only
  per block:
    blockNum: varint
    txCount: varint  — total entries (system tx + user txs)
    formatBit: 1 byte
    payload: bitmap or sparse encoded deps
```

Runtime reader: open file, seek to first block entry (after header), scan forward. On file boundary (every 500k blocks), close current file, open next.

## Environment variables

- `SEBOOST_GENERATE=txdeps|bal|both` — enable seboost data generation during stage_exec.
- `SEBOOST_LOAD=txdeps|bal|none` — which seboost type to load at runtime.
- `SEBOOST_DIR=<path>` — override default `<datadir>/seboost_*/` location.

## Correctness

- stage_exec with seboost automatically verifies state roots — bad hints produce root mismatches.
- File integrity: validate magic/version on load, bounds-check all varint reads.

---

## Resolved decisions

- **Transitive reduction**: store the transitively reduced dependency set. `GetDep()` already produces this via `depsHelper`. Smaller files, scheduler infers transitive deps.
- **Block index**: skip it. Sequential access only, scan linearly. One seek per file max.
- **Varint vs fixed-width**: implementation detail, pick during coding. Varints are fine for now.
- **USE_TX_DEPENDENCIES flag**: not needed for seboost. Pre-set deps are returned unconditionally by `Dependencies()`.
- **BuildDAG overhead**: bypass `profile` flag entirely; add dedicated seboost gate that calls only `GetDep()`.
- **Index convention**: 0-based matching GetDep() output. System tx = 0, user tx 0 = 1, etc. All txs included.
- **Stats output**: CSV + plots in `~/seboost-stats/`.

## Open questions (phase 2 only)

### Does erigon do state prefetch/warmup today?

For the BAL phase: does erigon already prefetch state before block execution? If yes, BAL seboost plugs into that path. If not, a new prefetch mechanism is needed. Deferred until after phase 1 results.
