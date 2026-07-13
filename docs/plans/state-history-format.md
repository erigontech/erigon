# A Common File Format for Blockchain State History

Proposal: standardize a small set of **immutable, content-addressable files** carrying a blockchain's state history.
Clients distribute them p2p and derive every other data structure locally. The format exists in production form inside
Erigon; this doc describes it, its remaining problems, and one format change that removes them.

One sentence: **specify the update stream, let every client derive its own database** — what era files did for block
history, applied to state.

---

## 1. The data: everything is `(key, ts, value)`

Every kind of state is one thing — an append-only **stream of updates**:

```
(key, ts, value)
(0xAlice…, txNum=100, {nonce:1, balance:5})
(0xAlice…, txNum=250, {nonce:2, balance:3})
(0xBob…,   txNum=250, {nonce:1, balance:9})
```

| Entity         | key      | ts       | value                |
|----------------|----------|----------|----------------------|
| Accounts       | address  | txNum    | account record       |
| LogAddress     | address  | txNum    | (empty — index-only) |
| Headers/Bodies | blockNum | blockNum | header/body          |

- **Deletion** = update with empty value. No tombstone type.
- **Blocks are the degenerate case**: key == ts, so keys never rewrite (except tip reorgs); they need only the "latest"
  projection.
- The stream is the only source of truth. Everything else — current state, indexes, blooms, btrees — is a **derivable
  projection**: rebuildable, zero extra information.

## 2. The queries

Read shapes:

```
GetLatest(key)            -> value            # execution, eth_getBalance, ...
GetAsOf(key, ts)          -> value at ts      # historical RPC, debug/trace at block
Timestamps(key, from, to) -> {ts, ts, ...}    # "when did key change" — logs, traces
```

Operational shapes — a format is judged by these as much as by reads:

```
prune(old)      # forget history before X, keep serving latest + recent
unwind(recent)  # roll back bad/reorged recent data cheaply
sync(range)     # download a verifiable range from untrusted peers
verify(file)    # check a file against consensus without full re-execution
seed(file)      # serve files to the network at zero marginal cost
```

## 3. Current format (Erigon 3) and its problems

### 3.1 Layout: the stream stored as columns

```
logical stream:        (key, ts, value)
                          │    │    │
        ┌─────────────────┘    │    └────────────────────┐
        ▼                      ▼                         ▼
      .ef  = (key, {ts})     joined                    .v  = dead values
        keys stored once,    positionally:               value BEFORE each change,
        ts-lists Elias-Fano  i-th value in .v ↔          in .ef enumeration order
        compressed           i-th (key,ts) of .ef
                                                       .kv = live values
                                                         the CURRENT value of each key
                                                         (exists nowhere in .v!)
```

- `.ef + .v + .kv` = a **partition of the stream by liveness**. None is redundant.

Merge semantics differ:

- `.ef`/`.v`: ts-disjoint chunks; merge = concatenation, nothing dropped.
- `.kv`: an LSM — newer files shadow older per key; compaction drops superseded values (safe: supersession already
  recorded them into `.v`).
- Mutable tail (recent data) lives in an ordinary DB (MDBX); irrelevant to the format.

Concrete, steps 0–16:

```
ts →   0                  8                  16
       ├──────────────────┼──────────────────┤
history:  account.0-8.ef    account.8-16.ef       (key, {ts})
          account.0-8.v     account.8-16.v        dead values
latest:   ◄──────── account.0-16.kv ────────►     live values — one merged file

.kv merges keep widening it: 0-16 → 0-32 → … → single file for all history (→ P2, P3)
```

### 3.2 What this already gets right

- **Prune = `rm`.** `GetAsOf(k, ts)` reads the *first change after ts* (a pre-value) or falls through to live `.kv` — so
  `{live .kv} + {history after X}` is self-sufficient ("suffix-closure"). Old `.v + .ef` files just get deleted. A
  post-value log can't do this without extra checkpoint machinery.
- **Compact.** Retained data = current state + a window of changes, not the full log from genesis.
- **Seed-what-you-serve.** The mmap'd working files *are* the distributed files: seeding costs zero extra disk and I/O.
  Break this property and rational operators delete the network copy.

### 3.3 Problems

**P1 — Latest state is not derivable.** Pre-values say what a key *was*, never what it *became*: the current value
exists only in `.kv`. A buggy/corrupt `.kv` can't be rebuilt from history — only by re-executing blocks.

**P2 - no compaction**  `.kv` must be content-addressed - means canonical - means `LSM-like compaction` impossible. And
unbounded span is an attack/blast-radius problem: file span = minimum unit of repair and re-verification.

**P3 — Unbounded merge destroys repair points.** The only repair primitive is "delete files above X, re-execute" — it
works at file granularity. Merge `.kv` toward one 8-year file and there is nothing left to delete: a bug in recent data
means rebuilding everything. (Capped ladders give free save points: masking levels above X = exact state at X.)

**P4 — Latest-state format is politically unspecifiable.** The latest store is the most performance-sensitive,
engine-specific part of every client; no two teams will agree on its layout, so a spec containing it stalls. Old `.kv`
is also big, cold, laid out by update-time — clients legitimately want freedom to reshape it.

**P5 — Verification is bootstrap-by-trust.** File hashes come from a registry the client ships; no in-band check against
consensus.

## 4. Proposal: put latest values into .ef file

```
ts →   0                  8                  16
       ├──────────────────┼──────────────────┤
history:  account.0-8.ef    account.8-16.ef       (key, latestValue, {ts})
          account.0-8.v     account.8-16.v        dead values
                                                  latest lives INSIDE .ef:
                                                  account.8-16.ef carries state@16
                                                  for keys changed in [8,16)

Latest Values (for Blocks Execution on Chain Tip) is derived from `.ef` files - Clients can have their own format

large values: optional account.8-16.lv, same enumeration order as .ef — keeps .ef small
```

| Problem                  | After                                                                                                                                                                                                                            |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| P1: `.kv` not derivable  | `.kv` = pure cache. Repair = local re-fold (hours), never re-execution.                                                                                                                                                          |
| P2: no compaction        | Canonical spans are ts-disjoint, union-merge only. The serving LSM is local — **every client compacts whenever/however it wants**; content-addressing untouched.                                                                 |
| P3: no repair points     | Every retained span boundary is a save point, independent of local compaction.                                                                                                                                                   |
| P4: unspecifiable latest | Latest state **leaves the spec surface**. Spec = span package only; each client's latest store is its own business. Asking teams to read a file, not adopt a database.                                                           |
| P5: trust-bootstrap      | Free redundancy: span N's `latestValue(K)` == span N+1's first pre-value of K. Spans cross-check each other; records chain against per-block BAL commitments (EIP-7928). Verify against headers + predecessor span, no registry. |

### Cost

One extra value per (key, span-it-changed-in): noise for hot keys, ~2× for a key touched once ever. Mitigations:

- Not new duplication — exactly what un-compacted chunk `.kv` stores today; net-new bytes = values compaction would have
  dropped.
- Suffix-closure still applies: `.lv` prunes with its span. Only archives pay full boundary-value history.

### Refinements (open)

- **Shard `.kv`/`.lv` into ~64 MB pieces**, per-shard existence filters: bounds merge write-amplification, natural unit
  for distribution and piece-repair.
- **Span boundaries in gas, not blocks/txNums.** Bloatnet produced wildly unequal spans; gas is the unit of state-change
  work. (Possibly `FullImmutabilityThreshold` should be gas-denominated too.)
- **Lazy latest.** Once derivable, the serving structure can be built on-demand, driven by observed read patterns.

## 5. Context

- **Era files proved the shape**: one span of the block stream + beacon state at the boundary — self-sufficient,
  immutable, cross-client. This is era applied to execution state:
  `.ef+.v` play blocks, `.lv` plays the state snapshot.
- **Safe-deletion table** — the operational contract:

  | Operation | How |
    |---|---|
  | Forget old history | `rm` old spans (`.ef+.v+.lv`); suffix-closure keeps the rest self-sufficient |
  | Undo recent data (bug/deep reorg) | `rm` recent spans, re-fold latest, re-execute the tail |
  | Repair corrupted latest store | re-fold from spans — local, no re-execution |
  | Excise a range inside a file | never — file span = blast radius; cap it in the spec |

- **Determinism is load-bearing.** Content-addressing needs byte-identical producers: fixed span boundaries, fixed
  enumeration order (key-major, ts-ascending), fixed encodings, deterministic (or no) compression. Ship a conformance
  suite: input stream → expected hashes.
- **Everything else stays local**: btrees, recsplit/bloom accessors, salts, caches, serving LSM, mutable tail DB. The
  spec is three columns per span, nothing more.

## FAQ

**Why pre-values? A post-value log folds forward from genesis.**
Networks delete *old* data, not new. Post-value logs are prefix-closed: dropping the beginning needs a checkpoint that
must be re-materialized as the prune point moves. Pre-value + latest is suffix-closed: prune = `rm`. With `.lv`, each
span carries post-values at its boundary anyway — pre-values inside the span, post-values sampled at boundaries: the
good half of both.

**Isn't storing latest twice wasteful?**
It converts a global mandatory artifact (canonical `.kv` LSM, frozen compaction) into a per-span column plus a local
free-form cache. Bytes comparable to today's pre-compaction reality; the flexibility and repairability are not.

**Why would other clients adopt one client's files?**
They adopt three flat, spec'd, verifiable columns of `(key, ts, value)` — a file format, not an engine. What each client
builds from it is out of scope by design. That separation is the adoption strategy.
