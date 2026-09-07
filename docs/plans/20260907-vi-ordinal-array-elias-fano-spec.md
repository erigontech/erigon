# Replace the `.vi` perfect hash with two Elias-Fano sequences

## Problem

On a mainnet archive the accessor files total 83.5 GB, and `.vi` is 40.11 GB of
that — the largest single item. 95.4% of a `.vi` is the recsplit ordinal array:
one `bytesPerRec`-wide slot per key, holding an offset into the `.v` file.

`.vi` maps `txNum+key -> offset in .v`. It has 8.19 billion keys across 47
files, so the array alone is 38.25 GB.

## The offset is already known when `.vi` is consulted

`HistoryRoTx.historySeekInFiles` does two lookups for one read:

1. `iit.seekInFiles(key, txNum)` resolves the key in the inverted index. Inside,
   `IndexReader.TwoLayerLookupByHash` computes the key's **ordinal in `.ef`** and
   then discards it, returning only an offset. The key's txNum list is decoded
   and `seq.Seek(txNum)` returns the **rank of that txNum in the list** — dropped
   on the floor as `_` (`inverted_index.go`, `equalOrHigherTxNum, _, found`).
2. `reader.Lookup(historyKey)` then runs a second, full perfect-hash lookup over
   the 8.19 billion `.vi` keys, to obtain an offset.

But `History.buildVI` writes `.v` in exactly that order: it walks `.ef` in key
order, walks each key's txNums in order, and advances `valOffset` monotonically
with a single global counter. So

```
valueOrdinal = cumValues[keyOrdinal] + rank
offset       = pageOffsets[valueOrdinal / pageValuesCount]
```

Both inputs are in hand one line earlier. The 40 GB index recomputes from
scratch what the previous statement already knew.

`buildVI` also advances `valOffset` only every `CompressedPageValuesCount`
values, so the same offset is stored once per value on a page — 64 times over
on most files, 16 on the rest.

## Evidence

A checker walked `.ef` in key order, derived each value's ordinal, and compared
against what `.vi` returns for the corresponding `txNum+key`:

| file | `.vi` | values | distinct offsets | miss | non-monotone | values/offset |
|---|---|---|---|---|---|---|
| `accounts.0-256` | 1.2 GB | 290,996,426 | 4,546,820 | 0 | 0 | 64.0 |
| `accounts.768-1024` | 1014 MB | 251,368,293 | 3,927,630 | 0 | 0 | 64.0 |
| `accounts.1024-1152` | 507 MB | 125,572,467 | 1,962,070 | 0 | 0 | 64.0 |
| `accounts.1184-1186` | 8.35 MB | 1,974,926 | 30,859 | 0 | 0 | 64.0 |

670 million lookups, no misses, and `values` equals `.vi`'s own key count in
every case — the ordinal walk enumerates precisely the set the index holds.
Offsets are non-decreasing in ordinal order, so Elias-Fano applies.

## Sizing

Both replacement sequences were built for real over all 47 files, taking the
page count from each `.v` file rather than from config (40 files use 64, 7 use
16):

| domain | files | `.vi` now | replacement | |
|---|---|---|---|---|
| `commitment` | 7 | 26.84 GB | 283.9 MB | 95x |
| `accounts` | 8 | 5.11 GB | 95.6 MB | 53x |
| `storage` | 8 | 3.05 GB | 117.0 MB | 26x |
| `receipt` | 8 | 2.70 GB | 5.3 MB | 509x |
| `rcache` | 8 | 2.36 GB | 43.1 MB | 55x |
| `code` | 8 | 0.05 GB | 3.6 MB | 12x |
| **total** | 47 | **40.11 GB** | **549 MB** | **73x** |

The ratio improves with file size, so it holds up as files merge.

## Design

`.vi` v2 holds two Elias-Fano sequences and no perfect hash:

- `cumValues` — one entry per `.ef` key, the number of history values before
  that key. Monotone.
- `pageOffsets` — one entry per page of `CompressedPageValuesCount` values, the
  `.v` offset of that page. Monotone.

Lookup replaces `reader.Lookup(historyKey)` with one `Get` on each sequence.
`seg.GetFromPage` already selects the right value inside the page by key, so the
read after the offset is unchanged.

Build drops the recsplit pass entirely: `buildVI` already computes both
sequences as it walks, and no longer needs to hash 8.19 billion keys.

## Feasibility

- `.ef` and `.v` step ranges line up 1:1 for all 47 files and all six domains on
  a mainnet archive, so a `keyOrdinal` from the inverted index indexes into the
  matching history file.
- Five call sites read `.vi`: `historySeekInFiles`, `HistoryDump`, and three in
  `history_stream.go`. Each sits directly after an inverted-index lookup that
  has the key ordinal, and the two iterator paths walk `.ef` in order, so the
  ordinal simply increments.

## Work

- `IndexReader` returns the key ordinal alongside the offset; `seq.Seek` already
  returns the rank, stop discarding it.
- New `.vi` format plus a version bump, with the old format still readable.
- Rewrite `buildVI` to emit the two sequences.
- Convert the five read paths.
- Merge path: rebuild the sequences rather than the hash.
