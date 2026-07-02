# Per-category prune retention — design decisions

Context: bounding on-disk history retention per data category (issue #21306 "prune
frozen files", #21198 commitment-history download bounding). This records the
flag-grammar choice, how to add a new category, and the other decisions made.

## Why the `--prune.<category>.<verb>` grammar (category-first)

The retention verbs are a mutually-exclusive set per category: `distance=N` (keep
last N blocks, relative to head), `before=<block>` (keep from an absolute block —
future), `full` (prune everything — future). We put the **category first** and the
verb last (`--prune.commitment-history.distance`, later `--prune.history.before`)
rather than verb-first (`--prune.distance.<category>`). Three reasons:

1. **It's the only ordering that nests cleanly in `config.toml`.** Erigon's config
   loader (`node/cli/config_file.go`) flattens nested TOML tables into dotted keys
   that must equal a CLI flag name. Category-first gives one table per category:

   ```toml
   [prune]
   mode = "archive"
   include-commitment-history = true
   [prune.commitment-history]
   distance = 100_000
   ```

   Verb-first would need `prune.distance` to be both a scalar (the legacy
   `--prune.distance` flag) *and* a table (`prune.distance.<category>`) in the same
   document — a hard TOML conflict.

2. **It makes the verb XOR structurally enforceable.** `distance`/`before`/`full`
   for one category are sibling keys under one `[prune.<category>]` table, so
   "at most one" is a local, obvious check. Verb-first scatters them across
   `[prune.distance]` / `[prune.before]` tables, hiding double-sets.

3. **It matches Reth** (`--prune.<segment>.distance|before|full`) and reuses
   Erigon's existing `distance` word, so the mental model transfers.

Reth/Geth/Polkadot/Cosmos survey: presets for humans (`--prune.mode`) that desugar
into per-category `distance`/`before` for automation is the convergent design;
`archive` is the clearest "keep everything" word (avoid Cosmos's `nothing`/
`everything` polarity footgun).

## How to add an "enable `<category>`" CLI flag

Some categories are opt-in: they are only *generated* when a boolean flag is set,
and retention only narrows what is generated. Commitment history is the model —
`--prune.include-commitment-history` (`KeepExecutionProofs`) is the **enable/
generate** toggle (persisted separately, with change-protection, because you can't
retroactively fill history you never produced), and `--prune.commitment-history.distance`
only *narrows* it.

| Flags | Meaning |
|---|---|
| *(neither)* | not generated / not kept |
| `--prune.include-commitment-history` | generate + keep all (archive) |
| `+ --prune.commitment-history.distance=N` | generate + keep last N blocks |

To add a new category `X` with an enable toggle:

1. **Enable flag** — a `cli.BoolFlag` (e.g. `--prune.include-X`) that drives
   *generation* and is persisted with change-protection (mirror
   `checkAndSetCommitmentHistoryFlag` / `rawdb.*CommitmentHistoryEnabled`). Keep it
   separate from retention: enabling is a hard, one-way execution-mode switch;
   retention is a soft, shrinkable window.
2. **Retention flag** — `--prune.X.distance` (`cli.Uint64Flag`, blocks; `0` =
   unbounded). Validate it *requires* the enable flag, and `<=` the general
   `--prune.distance`.
3. **`prune.Mode`** — add a `Category` const + an `extraCategoryDBKey[X]` entry;
   the generic `Extra map[Category]BlockAmount` handles persistence via the
   existing `EnsureNotChanged`. No new `Config` field, no new persistence func.
4. **Wiring** — in `applyRemainingEthFlags` (and the cobra variant): if the
   retention flag is set, require the enable flag, then `mode = mode.WithX(d)`;
   finally `prune.Validate(mode)`.
5. **Retirement** — add a case in `Aggregator.RetireOldHistoryFiles` mapping the
   domain to `X`'s cutoff, and extend `historyRetireCutoffStep` to compute it
   (inheriting the general window unless the category is bounded tighter).
6. **Download filter** — `buildBlackListForPruning` already takes a per-category
   cutoff; add the category's step to it.

## Other decisions

- **Blocks, not steps, at the CLI.** Operators think in blocks (≈ time). Steps,
  domains, and how many files back a domain is are implementation details. Block→
  step conversion reuses `TxnumReader().Min(block)/stepSize` — the *same* math the
  retirement cutoff uses, so download-skip and on-disk retirement agree file-for-file.
- **Commitment inherits the general history window when its own distance is unset.**
  A minimal node keeps commitment history at `--prune.distance` like everything
  else; an archive node can bound commitment independently (the #21198 case).
- **Accept-with-warning on widening** (both `Mode` fields and `Extra` categories),
  not expand-reject: deleted history is re-downloadable, so a wider window going
  forward is fine; matches the existing `isRetentionWindowChange` behavior.
- **`prune.Mode` stays the single retention model.** Per-category lives in a generic
  `Extra` map, reusing one `EnsureNotChanged` persistence path — not per-domain
  `Config` fields / per-domain `Ensure*` functions (that scales tech-debt per domain).
- **Separate flags, not urfave aliases**, for any category-first spelling of a
  legacy flag: `ctx.Set` from the config-file loader does not propagate an alias to
  its primary (only CLI parsing's `normalizeFlags` does).
- **Whitelist download filter (#15589) deferred.** The per-file retention skip is
  orthogonal to per-type whitelisting and works the same under either outer filter.
- **RCache and standalone log/trace indices are out of scope** for state-history
  retention — they follow blocks/receipts retention, not `--prune.distance`.
