# Per-domain history retention — design decisions

Bounding on-disk history retention per domain (#21306 "prune frozen files", #21198
commitment-history download bounding).

## Flag surface: `--history.<domain>=<retention>`

One flag per domain; the value carries both on/off and the window. In `config.toml`
each domain is a scalar leaf under one `[history]` table:

```toml
[history]
state         = '1y'        # keep ~1 year of state history
commitment    = 100_000     # keep the last 100k blocks
receipts      = false       # don't keep (re-exec on demand)
domainA       = true        # keep everything (archive)
domainB.from  = 10_000_000  # keep from absolute block 10M (inclusive)
```

Each is also a CLI flag of the same name, CLI overriding the file (`--history.state=1y`, …).

| value | meaning |
|---|---|
| `false` / `off` | not generated / not kept |
| `true` / `on` / `keep-all` | keep everything |
| `<blocks>` — `100000` | keep the last N blocks |
| `<duration>` — `10d`, `2w`, `6mo`, `1y` | keep ~that wall-clock (→ blocks) |

Duration units are `d`/`w`/`mo`/`y` only. `m` is rejected: `10m` is ambiguous
(minutes? million? months?) — months are `mo`. Blocks are plain integers, no
`k`/`M`/`B` (`_` separators allowed). Rule: pure digits → blocks; digits+unit →
duration; else error. Optional `--history.<domain>.from=<block>` keeps from an
absolute block (inclusive).

`--prune.mode=archive|full|minimal|blocks` stays the coarse preset, setting
`history.*` defaults that per-domain flags override.

## Why this shape

**`history.`, not `prune.`** — "what to keep" reads unambiguously (`history.state=1y`
= keep 1y; `prune.state=1y` = keep or remove?), matches Geth
(`--history.state`/`.transactions`/`.chain`), and drops a word
(`--prune.commitment-history.distance` → `--history.commitment`). `--prune.mode`
stays the preset — the same split as Geth (profiles + `--history.*`) and Reth
(`--full`/`--minimal` + per-segment).

**One polymorphic value, not `enable`+`distance`** — folding on/off into the value
(`=false` off, else on) drops the separate enable flag and `.distance` verb.
Decisive: a scalar leaf nests cleanly as `[history]` in `config.toml`, where no key
is both scalar and table (the conflict that sinks verb-first and `.enable`). It
extends existing code — `--prune.distance` is already a `StringFlag` parsed by
`prune.ParseHistoryDistance` (numbers + `keep-all`); we widen that parser.

**Bare number = last N; absolute via `.from`** — a scalar can't give a number two
meanings, so bare = distance (Geth/Polkadot) and the absolute anchor is a distinct
`.from` sub-key (like `--http`/`--http.api`). In `config.toml` a domain is *either*
scalar *or* table, so TOML rejects both at once — enforcing "one setting" for free
(the CLI must validate that XOR). `from` names what's kept (inclusive), fitting
`history.` better than Reth's delete-side `before`.

**No fork-named anchor sentinel** — Erigon runs many chains. `keep-post-merge` /
`post-prague` / any fork anchor is Ethereum-centric: elsewhere the fork is at a
different block, means something else, or doesn't exist. Use chain-agnostic forms
(`keep-all`, blocks, duration, `.from`); a fork alias, if ever offered, is per-chain
config resolving to a block number, never a universal sentinel.
(`ParseBlocksDistance`'s existing `keep-post-merge` is this eth-centric case — don't
extend it.)

**Blocks/durations, not steps** — operators think in blocks; steps and file counts
are implementation details. Block→step reuses `TxnumReader().Min(block)/stepSize`,
the same math as the retirement cutoff, so download-skip and on-disk retirement
agree file-for-file. A duration converts to blocks once at parse (nominal block
time): deterministic but approximate/chain-dependent (12s Ethereum, ~5s Gnosis) —
fine for a fuzzy window.

**Two persistence semantics in one value** — on/off is one-way and change-protected
(can't backfill ungenerated data → resync to flip); the window is freely shrinkable
(deleted history re-downloads). The parser yields `{enabled, kind, value}`;
persistence guards `enabled`, accepts window changes with a warning (today's
`isRetentionWindowChange`).

## Add a domain `X`

1. **Flag** — polymorphic `--history.X` (`StringFlag` + a `ParseRetention` mirroring
   `ParseHistoryDistance`); add `--history.X.from` only for an absolute anchor. Real
   primary flags, not urfave aliases (config-file aliases don't propagate to the
   primary).
2. **`prune.Mode`** — a `Domain` const + `extraDomainDBKey[X]`; the generic
   `Extra map[Domain]BlockAmount` persists via existing `EnsureNotChanged`.
3. **Wiring** — parse `--history.X` in `applyRemainingEthFlags` (+ cobra variant),
   set it, `prune.Validate`.
4. **Retirement** — a case in `RetireOldHistoryFiles` + extend
   `historyRetireCutoffStep` (inherits the general window unless bounded tighter).
5. **Download filter** — add X's step to `buildBlackListForPruning` (already per-domain).

## Migration

| current | new |
|---|---|
| `--prune.distance=N` | `--history.state=N` |
| `--prune.include-commitment-history` | `--history.commitment=keep-all` |
| `--prune.commitment-history.distance=N` | `--history.commitment=N` |
| `--persist.receipts` | `--history.receipts=keep-all` |

Legacy names stay as accepted spellings (separate flags read with precedence, not
urfave aliases); persisted DB keys unchanged.

## Other decisions

- **`prune.Mode` is the single retention model** — one `Extra` map + one
  `EnsureNotChanged` path, not per-domain `Config` fields.
- **A domain inherits the `state` window when unset; `commitment` must be `≤ state`**
  (a proof past retained state history can't be served). Minimal keeps all at
  `state`; archive can bound `commitment` alone (#21198).
- **Whitelist download filter (#15589) deferred** — orthogonal to the per-file skip.
- **RCache and standalone log/trace indices are out of scope** — they follow
  blocks/receipts, not `--history.state`.
