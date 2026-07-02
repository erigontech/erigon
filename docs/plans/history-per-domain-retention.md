# Per-domain history retention — design decisions

Context: bounding on-disk history retention per data domain (issue #21306 "prune
frozen files", #21198 commitment-history download bounding). This records the flag
surface, why it's shaped this way, how to add a new domain, and the other decisions.

## The flag surface: `--history.<domain>=<retention>`

One flag per domain, under a `history.` namespace, whose *value* carries both on/off
and the retention window. In `config.toml` every domain is a scalar leaf under one
`[history]` table:

```toml
[history]
state         = '1y'        # keep ~1 year of state history
commitment    = 100_000     # keep the last 100k blocks of commitment history
receipts      = false       # don't keep receipt history (re-exec on demand)
domainA       = true        # keep everything (archive)
domainB.after = 100_000     # keep history from absolute block 100000 onward
```

Every line is also a CLI flag with the identical name (CLI overrides the file):

```
--history.state=1y  --history.commitment=100000  --history.receipts=false
--history.domainA=true  --history.domainB.after=100000
```

Value grammar for the bare `--history.<domain>` flag:

| value | meaning |
|---|---|
| `false` / `off` | not generated / not kept |
| `true` / `keep-all` | keep everything (archive) |
| `<duration>` — `1y`, `90d` | keep ~that much wall-clock (converted to blocks) |
| `<blocks>` — `100000` | keep the **last N blocks** |

plus an optional sub-key for the absolute anchor:

| flag | meaning |
|---|---|
| `--history.<domain>.after=<block>` | keep history **from absolute block N** onward |

`--prune.mode=archive\|full\|minimal\|blocks` stays as the coarse preset; it just
sets `history.*` defaults that the per-domain flags override.

## Why this shape

**Why `history.`, not `prune.`** — Positive "what to keep" framing reads
unambiguously: `history.state = 1y` is plainly "keep one year of state history",
whereas `prune.state = 1y` forces the reader to guess whether `1y` is what's
*removed* or what's *retained*. It is also Geth-compatible — Geth ships
`--history.state=N` / `--history.transactions=N` / `--history.chain` — which eases
migration and reuses an established mental model, and it drops a redundant word
(`--prune.commitment-history.distance` becomes just `--history.commitment`).
`--prune.mode` remains the coarse policy preset; `--history.<domain>` are the fine
amounts — the same split Geth uses (profiles + `--history.*`) and Reth uses
(`--full`/`--minimal` + per-segment).

**Why one polymorphic value per domain, not `enable` + `distance` sub-keys** —
Folding on/off and the window into a single value (`=false` off, anything else on)
removes the need for a separate enable flag (`.enable`, a bare toggle, or
`--prune.include-*`) and a `.distance` verb. The decisive reason is `config.toml`:
Erigon's loader (`node/cli/config_file.go`) flattens nested tables into dotted keys
that must equal a flag name. A **scalar leaf per domain** nests perfectly as
`[history]` with scalar keys — no key is ever both a scalar and a table, which is
the exact conflict that sank every verb-first spelling (`prune.distance` scalar vs a
`[prune.distance]` table) and every `.enable`-under-a-bare-toggle spelling. It is
also an *extension, not new machinery*: `--prune.distance` is already a
`cli.StringFlag` parsed by `prune.ParseHistoryDistance`, which already accepts
several value shapes (base-0 integers, a `keep-all` sentinel) — we widen that
parser's vocabulary.

**Why bare number = "last N", and absolute via a `.after` sub-key** — A single
scalar can't give a bare number two meanings. Keeping bare = distance ("last N
blocks") matches Geth (`--history.state=N`) and Polkadot (`--state-pruning N`); the
rarer absolute anchor gets an explicit `--history.<domain>.after=<block>` sub-key —
a distinct flag — so there is no "last N vs from N" ambiguity (the same shape as
`--http` / `--http.api`). In `config.toml` this is safe because a domain is *either*
a scalar (`domainB = 100000`) *or* a table (`domainB.after = N`); TOML rejects both
in one file, which conveniently enforces "at most one retention setting" for free.
(On the CLI the two are independent flags, so that XOR must be validated in wiring.)
`after`/`from` names what's *kept*, which fits the `history.` framing better than
Reth's `before` (which names what's *deleted*) — document whether N is inclusive.

**Why no fork-named anchor sentinel (multi-chain)** — Erigon supports many chains
(Gnosis, Polygon/Bor, testnets), not just Ethereum mainnet. A sentinel like
`keep-post-merge` — or any fork-named anchor (`post-merge`, `post-prague`, …) — is
Ethereum-mainnet-centric: on other chains the merge or a given fork happened at a
different block, means something else, or does not exist, so the anchor is
undefined. Fork-named anchors therefore must **not** be part of the cross-chain
vocabulary. Prefer chain-agnostic forms: `keep-all`, a `<blocks>` distance, a
`<duration>`, or the absolute `.after=<block>`. If a fork alias is offered at all it
belongs behind per-chain config that *resolves to a block number* and is valid only
where that fork exists — never a universal sentinel. (The existing `keep-post-merge`
in `ParseBlocksDistance` is exactly this eth-centric case; don't extend the pattern
to the new surface.)

**Why blocks and durations, not steps** — Operators think in blocks (≈ time);
*steps*, *domains*, and how many files back a domain is are implementation details we
must stay free to change. Block→step conversion reuses `TxnumReader().Min(block) /
stepSize` — the same math the retirement cutoff uses, so download-skip and on-disk
retirement agree file-for-file. A duration is converted to blocks once, at parse
time, using the chain's nominal block time; that makes it a fixed, deterministic
block window, at the cost of being approximate and chain-dependent (12s Ethereum,
~5s Gnosis, pre/post-merge differ) — fine for a fuzzy retention window, documented
as such.

**Why the value carries two persistence semantics** — On/off (`false ↔ on`) is a
hard, one-way, change-protected execution-mode switch: you can't retroactively fill
data you never produced, so flipping it requires a resync. The window
(distance/duration) is a soft, freely shrinkable/wideable knob (deleted history is
re-downloadable). The parser yields `{enabled, kind, value}`; persistence
change-protects `enabled` and accepts window changes with a warning — the same
accept-with-warning behavior as today's `isRetentionWindowChange`.

## How to add a new domain `X`

1. **Flag** — a polymorphic `--history.X` (`cli.StringFlag` + a `ParseRetention`
   mirroring `ParseHistoryDistance`); add `--history.X.after` only if X needs an
   arbitrary absolute anchor. Both must be **real primary flags, not urfave aliases**
   — an alias set from the config file does not propagate to its primary (only CLI
   parsing's `normalizeFlags` does).
2. **`prune.Mode`** — add a `Domain` const + an `extraDomainDBKey[X]` entry; the
   generic `Extra map[Domain]BlockAmount` handles persistence via the existing
   `EnsureNotChanged`. No new `Config` field, no new persistence func.
3. **Wiring** — in `applyRemainingEthFlags` (and the cobra variant): parse
   `--history.X`, set the domain in the mode, then `prune.Validate(mode)`.
4. **Retirement** — add a case in `Aggregator.RetireOldHistoryFiles` mapping the
   domain to X's cutoff, and extend `historyRetireCutoffStep` to compute it
   (inheriting the general window unless X is bounded tighter).
5. **Download filter** — `buildBlackListForPruning` already takes a per-domain
   cutoff; add X's step to it.

## Migration from the current flags

| current | new |
|---|---|
| `--prune.distance=N` | `--history.state=N` |
| `--prune.include-commitment-history` | `--history.commitment=keep-all` |
| `+ --prune.commitment-history.distance=N` | `--history.commitment=N` |
| `--persist.receipts` | `--history.receipts=keep-all` |

Keep the legacy names as accepted spellings via the "separate flags read with
precedence" pattern (not urfave aliases, per the config-loader caveat above); their
persisted DB keys are unchanged.

## Other decisions

- **`prune.Mode` stays the single retention model.** Per-domain retention lives in a
  generic `Extra` map reusing one `EnsureNotChanged` persistence path — not
  per-domain `Config` fields / per-domain `Ensure*` functions (which scale tech-debt
  per domain).
- **A domain inherits the general (`state`) window when unset, and `commitment` must
  stay `≤ state`.** A commitment proof beyond the retained state history can't be
  served (it falls back to state history). A minimal node keeps everything at the
  `state` window; an archive node can bound `commitment` independently (the #21198
  case).
- **Whitelist download filter (#15589) deferred.** The per-file retention skip is
  orthogonal to per-type whitelisting and works the same under either outer filter.
- **RCache and standalone log/trace indices are out of scope** for state-history
  retention — they follow blocks/receipts retention.
