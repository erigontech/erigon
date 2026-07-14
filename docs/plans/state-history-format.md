# A Common File Format for Blockchain State History

Proposal: standardize a small set of **immutable, content-addressable files** carrying a blockchain's state history.
Clients distribute them p2p and derive every other data structure locally.

---

## 1. The data: everything is `(key, ts, value)`

Everything is an append-only **stream of updates**:

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

- **Creation** = insert empty value
- **Deletion** = insert tombstone to LatestState and "last value" to History
- Stream is source of truth. Everything else — indexes, blooms, btrees — is a **derivable projection**

## 2. The queries

Read shapes:

```
GetLatest(key)            -> value            # execution, eth_getBalance, ...
GetAsOf(key, ts)          -> value at ts      # historical RPC, debug/trace at block
Timestamps(key, from, to) -> {ts, ts, ...}    # "when did key change" — eth_getLogs(filters)
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

- `.ef + .v + .kv` = together is "The History" of State. None is redundant.

Merge semantics differ:

- `.ef`/`.v`: ts-disjoint chunks; merge = concatenation, nothing dropped.
- `.kv`: an LSM — newer files shadow older per key
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

- **Prune = `rm old_chunks`.** `GetAsOf(k, ts)` reads the *first change after ts* (a pre-value) or fallback to Latest
  Value (`.kv`) — so `{Latest State .kv} + {history after X}` is self-sufficient ("suffix-closure")
- **It's small**: History doesn't store copy of Latest State. Block Execution on Chain Tip doesn't read History.
- **Seed-what-you-use**: Same files used by serving RPC requests and initial Syncing/Seeding. No re-packing, no
  double-space, no tar-balls. This is very important property! Because Users/Operators have reason to keep history -
  because they using it. Without this property - rational operators delete the network copy.
- **We made seeding cheap**: BitTorrent has nice feature "any HTTP Server can be a Peer". Means "S3/R2 bucket + CDN" can
  support Network. It's way cheaper than have server with running Erigon.

### 3.3 Problems

**P1 - we have merge, but no compaction** `.kv` must be content-addressed - means stay immutable for long time. Also
after compaction - impossible remove recent files (rest `.kv` files don't have enough keys after compaction).

**P2 — remove recent files works - but limited**: can drop recent (small) `.kv` files - execution will work. But last
`.kv` file is very big (5 years of Eth history) - if we delete it we will start exec from 0.

**P3 — Latest-state format is politically unspecifiable.** - it's the most performance-sensitive, engine-specific part
of every client; no two teams will agree on generic layout.

**P4 — How to verify files?** by content-only (not just checksum files)

**P5 — Unclear which hash store on chain** Adding disk-format to Spec: means freeze it forever. Even if we do - which
files to hash? Or maybe which data to hash?

## 4. Proposal: put latest values into .ef file and exclude .kv from Spec

```
ts →   0                  8                  16
       ├──────────────────┼──────────────────┤
history:  account.0-8.ef    account.8-16.ef       (key, latestValue, {ts})
          account.0-8.v     account.8-16.v        dead values
                                                  latest lives INSIDE .ef:
                                                  account.8-16.ef carries state@16
                                                  for keys changed in [8,16)

Clients: to derive their own format of Latest State from "The History" 
```

| Problem              | After                                                                                                                                                                                              |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| P1: no compaction    | Latest State doesn't need to be content-addressable anymore. All files will get "forever immutability" property (`.v` and `.ef` already have it)                                                   |
| P2: no repair points | Every shard boundary is a "save point" can start re-execution from there at any time                                                                                                               |
| P3: politics         | Spec = covers only history. Clients can derive any format of Latest State (or None - exec will work)                                                                                               |
| P4: verification     | Free redundancy: span N's `latestValue(K)` == span N+1's first pre-value of K                                                                                                                      |
| P5: hash what        | `(key, ts, value)` data-stream: is very fundamental thing. Means can `hash` it instead of files (maybe without MerkleTree). `Shard N+1`'s `hash` (of state history) can hook to `Shard N`'s `hash` |

### Limitations

Large values: can create

- Prune old files: old `LatestValues` can't be pruned - means can't store them in `.ef`. No problem: can create new file
  type `.lv` sharded as `.ef` - `account.8-16.lv`. But still "never-pruning and never-compaction" `.lv` files - is the
  Cost. But it's cold-storage (probably even RPC will not touch it). Does it means users will have incentive to delete
  it?
- Latest State derived format ideas: whole or partial (instead of storing last large file - can fallback to history),
  store it in db or files, read-amplification-driven or write-amp, etc...

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

## Erigon's far plans (not related to State Spec)

- **Shard `.kv` into ~1g**: to reduce Write-amplification (and amount of free space needed) of Merge
- **Shard history by Gas or GB's, not blocks/txNums** Bloatnet produced wildly unequal shards. Required manual Erigon
  configure. (Ethereum's `FullImmutabilityThreshold` should be gas-denominated too)


