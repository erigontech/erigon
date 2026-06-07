# Erigon Snapshot Archive — How It Works

Companion to [`product-overview-snapshot-flow.md`](product-overview-snapshot-flow.md). The flow doc describes how the archive is *distributed*; this doc describes what the archive actually *is*.

---

## Executive brief

The Erigon archive is not a single file or a single database. It is a layered set of immutable files plus a small live database, organised so that:

- **The cost of holding "all of history" stays log-shaped, not linear.** Old data ends up in a small number of large, dense files; recent data lives in a thin live DB and a tail of small files. The cost of looking something up grows with the log of chain age, not its length.
- **Current state and historical state are separate artefacts.** A node that only needs the current value of a key never reads a history file. A node that needs the value at block N never has to scan forward.
- **The "what changed when" timeline is a first-class file.** History isn't reconstructed by replaying transactions — it's recorded inline as the chain advances, in a compact key→step index that supports point-in-time queries directly.
- **Files merge on a deterministic schedule.** New small files are produced as the chain advances; over time they are merged into fewer, larger files. The merge schedule is fixed and reproducible — two nodes following the same schedule will, in steady state, hold the same file set.

The model has two file families (block files + state files), one merge engine that runs over both, and a small fixed number of named *domains* that carve up state by category. The rest is naming conventions.

---

## 1. Two file families

### Block files (`.seg`)

A flat, append-only encoding of the chain's block data: headers, bodies, transactions, receipts. Named by block range:

```
v1.0-v1.0-000000-000500-headers.seg
v1.0-v1.0-000000-000500-bodies.seg
v1.0-v1.0-000000-000500-transactions.seg
```

These are the units the BitTorrent swarm distributes. They cover blocks 0–500,000 in 500k-block buckets at the smallest size, merging upward to larger ranges over time. A node that only wants to serve historical block queries can hold these and nothing else.

### State files (`.kv`, `.v`, `.ef`)

The state snapshots. Named by *step range* (see §3) and *domain* (see §4):

```
v1.0-accounts.0-256.kv     ← current account values, steps 0..256
v1.0-accounts.0-256.v      ← history of account changes within those steps
v1.0-accounts.0-256.ef     ← inverted index: which steps touched each key
```

Plus accessor files (`.bt`, `.kvei`, `.idx`) that let the node find a key inside a `.kv`/`.v` without scanning.

---

## 2. The boundary between archive and DB

At any moment, a running node holds:

- **Frozen files on disk**: everything up to the most recent merge horizon.
- **A live MDBX database**: state changes since that horizon, plus indexes that haven't yet been promoted to a file.

The boundary moves forward in chunks. When enough new state has accumulated in the DB to fill one *step* (§3), the aggregator flushes that step out to a new pair of small files (one for state, one for history+index) and prunes the corresponding range from the DB.

This is what lets the node survive a stop/start with no rebuild: the frozen files are immutable, the DB is small, and the boundary between them is a clean step boundary.

---

## 3. The step (why tx-numbers, not blocks)

A **step** is `390,625` transaction numbers (`DefaultStepSize`). It is *not* a block count — different blocks contain different tx counts, and the archive is partitioned by tx-numbers, not blocks.

- Each block extends the chain by `1 + len(txs)` tx-numbers (the +1 is the per-block system tx). A block with 200 user txs advances tx-numbers by 201.
- A step rolls over when cumulative tx-numbers cross the next 390,625 boundary, regardless of which block we are on.
- This gives the merge engine even-sized work units across blocks of any density.

Older datadirs were built with `LegacyStepSize = 1,562,500` (4× larger). Both schemas coexist on the network; consumers identify which they are reading from the file's name prefix and the chain manifest.

**Why tx-numbers and not blocks**: a write inside a tx is the atomic unit. Tx-numbering gives every domain a single monotonic clock and lets the inverted index (see §6) cite a precise tx-number for each change. Block-numbering would force the index to record "block-and-position-within-block," which loses determinism across re-orgs and forks.

---

## 4. Domains — current state, by category

The state is split into a fixed set of **domains**. Each domain is independent: it has its own files, its own DB tables, its own merge schedule, its own indexes.

| Domain | What it holds |
|---|---|
| `AccountsDomain` | Per-address account record (nonce, balance, incarnation, code hash) |
| `StorageDomain` | Per-(address, slot) storage value |
| `CodeDomain` | Per-code-hash contract bytecode |
| `CommitmentDomain` | The patricia trie state — branch nodes keyed by trie path |
| `RCacheDomain` | Receipt cache (block log indexing) |
| `ReceiptDomain` | Receipt-side state |

Some domains depend on others: the commitment trie is recomputed from changes in `AccountsDomain` and `StorageDomain`, so the aggregator records dependency edges (`AddDependencyBtwnDomains`) and processes commitment after its inputs settle.

A query like *"what is the balance of 0x1234… right now"* hits exactly one domain (Accounts). A query like *"what was the trie root at block 1,000,000"* hits two (commitment for the root, accounts/storage for replaying any changes since the nearest frozen anchor).

---

## 5. Two views of every domain — current state vs history

For each domain, the archive holds two distinct artefacts per step range:

- **`.kv` — the current-state view.** A sorted key→value map showing the *latest* value of each key as of the end of that step range. This is what a "what is X's balance" query reads.
- **`.v` — the history view.** The full *timeline* of value changes within that step range. Each entry is `(key, tx_number, value)`. This is what a "what was X's balance at tx N" query reads.

These are not redundant. The `.kv` is dense and small (one entry per key that *exists*); the `.v` is sparse and proportional to the number of *changes* in the range. A key that doesn't change in a range appears in `.kv` (carried forward from the previous range) but not in `.v`.

**Operator consequence**: a node that only needs the current state can prune the `.v` files and keep the `.kv` files — that is what `prune.mode=minimal` does. Archive mode keeps both. The format and the file boundaries are identical either way.

---

## 6. History indexing — the `.ef` file

A `.v` file is sorted by key, then by tx-number. Scanning it to find "all changes to key X" would be linear in the file size. The `.ef` file fixes this.

`.ef` (extension stands for **Elias–Fano**) is an inverted index over the history file:

```
key X → encoded set of tx-numbers at which X was changed
```

The set is stored in Elias–Fano encoding — a compact representation of an increasing sequence of integers that supports `predecessor(t)` in constant time. To find "what was the value of X at tx N":

1. Open `.ef` for the latest step range that contains N.
2. Look up X → get the set of tx-numbers where X changed.
3. `predecessor(N)` → tx-number `t* ≤ N` of the most recent change to X at or before N.
4. Open `.v` at offset for `(X, t*)` → read the value.

If `t*` doesn't exist in this range (X wasn't touched), recurse to the previous range; if it doesn't exist anywhere, read X from the latest `.kv`.

The `.ef` is what makes time-travel queries cheap. Without it, history would be effectively unsearchable.

---

## 7. Indexing inside a file — the accessors

Each `.kv` (and each `.v` and `.ef`) ships with one or more *accessor* sidecar files that let the node find a key in O(log) or O(1) without parsing the file body:

- **`.bt` — B-tree accessor.** A balanced search tree over the sorted keys of a `.kv`. Used for range scans and ordered iteration.
- **`.kvei` — existence filter.** A compact filter (Bloom-equivalent) that answers "does key K exist in this file?" without an actual lookup. Used to skip files that obviously don't contain a key, which dominates query cost on archives with many step ranges.
- **`.idx` — recsplit hash index.** A perfect hash function mapping a key to its byte offset in the file. Used where O(1) point lookup matters and existence is implied.

A `.kv` produced by the aggregator may carry any subset of these — the domain configuration declares which accessors it needs. The accessor files are regenerated locally from the `.kv` body if they go missing; only the `.kv` itself needs to be distributed.

---

## 8. The merge schedule

The archive grows by appending new small files at the leading edge. It does not grow by appending to existing files — those are immutable.

When the live DB has filled enough to span one full step, the aggregator emits new files for that step:

```
…
v1.0-accounts.121-122.kv   ← just written
v1.0-accounts.121-122.v
v1.0-accounts.121-122.ef
```

Over time, adjacent same-domain files merge into larger ranges. The schedule is fixed: a file of width `w` merges with its neighbours to produce a file of width `2w`, geometrically, up to a cap.

The cap is `DefaultStepsInFrozenFile = 256` steps. A file that covers 256 steps is *frozen* — it never merges further. Smaller files exist transiently between merges and either roll up into the next 256-step frozen file or sit at the leading edge until they do.

**At steady state on a typical chain**, a node holds:
- A handful of frozen 256-step files per domain (covering all history).
- Up to ~7 transient files per domain at progressively halving widths (256, 128, 64, 32, 16, 8, 4, …) covering the partially-merged tail.
- A live DB covering the most recent partial step.

The merge schedule is what bounds the *number* of files at log(chain age). Without it, an archive node would accumulate one new file per step indefinitely.

A merge is purely additive at the file-system level: the input files stay on disk until the output is fully written and its accessors built; only then are the inputs unlinked. A crash mid-merge leaves the inputs intact and the partial output discarded on next start.

---

## 9. Where commitment fits

The patricia trie root that the EVM cares about is recomputed from changes in `AccountsDomain` and `StorageDomain`. To make that affordable, the trie itself is materialised in its own domain — `CommitmentDomain` — with the same `.kv` / `.v` / `.ef` structure as the others.

- The `.kv` is the trie's branch-node map keyed by trie path.
- The `.v` records every branch-node mutation, tx-number-by-tx-number.
- The `.ef` lets a query like "what was the trie root at tx N" find the relevant branch nodes without replaying execution.

This is what makes admin-SetHead's mode-B unwind (see flow doc §8) tractable past the diffset horizon: the trie state at any past tx-number is reconstructible from `CommitmentDomain`'s files alone, without re-running history.

---

## 10. What an operator sees

| Concern | Mechanism |
|---|---|
| "How big is my datadir going to get?" | Block files grow linearly with chain length. State `.kv` files grow with the number of *distinct keys ever touched* (much slower than chain length). History `.v` + `.ef` files grow with the number of *changes* (proportional to total tx-numbers). Archive-mode keeps all three families; minimal-mode keeps blocks + `.kv` only. |
| "Can I prune history without losing current state?" | Yes — drop `.v` and `.ef` files. Current-state queries still work; time-travel queries return only what's in the live DB. |
| "Can I resume after a stop?" | Yes — the frozen files are immutable, the live DB is journaled. Recovery time is bounded by how much was in the live DB. |
| "Can I share files with another node?" | Yes — frozen files are bit-identical between nodes following the same step schedule. That is what the network features in the flow doc (§3 Quorum, §4 Adoption) build on. |
| "What does a 'merge' cost?" | Bounded I/O proportional to the sum of the input file sizes, and a brief atomic rename at completion. Block-readers serving the affected ranges quiesce for the cutover window only. |
| "What if a merge crashes mid-way?" | Inputs untouched, partial output discarded next start, retried automatically. |

---

## 11. The shape, in one paragraph

The archive is a set of immutable files cut on step boundaries (a step = 390,625 tx-numbers). Each domain (accounts, storage, code, commitment, receipts) carries three files per range: current-state (`.kv`), history (`.v`), and an inverted index (`.ef`) that maps key → set of tx-numbers where it changed. Block data lives in a parallel `.seg` family cut on block ranges. Small files merge into bigger files on a fixed geometric schedule capped at 256 steps per frozen file. Everything outside the most recent partial step lives on disk; the live DB holds only the tail. Two nodes that follow the same schedule converge on the same file set — which is what the [distribution layer](product-overview-snapshot-flow.md) takes advantage of to share those files cheaply across the network.

---

## Where to find this in the code

| Concept | Primary entry points |
|---|---|
| Domain definitions | `db/state/statecfg/state_schema.go`, `db/state/aggregator2.go` (registration) |
| Step size + frozen-file cap | `db/config3/config3.go` (`DefaultStepSize`, `DefaultStepsInFrozenFile`) |
| `.kv` / current state | `db/state/domain.go` |
| `.v` / history | `db/state/history.go` |
| `.ef` / inverted index | `db/state/inverted_index.go` |
| Accessor sidecars (`.bt`, `.kvei`, `.idx`) | `db/state/btree_index.go`, `db/state/existence_filter.go`, `db/recsplit/` |
| Merge schedule + execution | `db/state/merge.go`, `db/state/aggregator_files.go` |
| Block snapshots (`.seg`) | `db/snapshotsync/snaptype/`, `db/snapshotsync/snapshots.go` |
| File-name grammar | `db/snapshotsync/snaptype/` (`ParseFileName`) |
| Aggregator boundary management | `db/state/aggregator.go` |
