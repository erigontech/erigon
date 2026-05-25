# `seg-bench` — codec benchmark for Erigon `.seg` files

A small Go binary that recompresses Erigon block snapshots under
candidate codecs (snappy, zstd at various levels), preserves the
per-record random-access property today's `seg/compress` provides,
and reports size + encode time + decode time + random-access decode
latency.

See the methodology / design spec at:
`erigon3/history-network/blocks/codec-benchmark-design.md` in
the [erigon-documents](https://github.com/erigontech/erigon-documents)
repo.

## Usage

```bash
# Build
make seg-bench    # or: go build -o build/bin/seg-bench ./cmd/seg-bench

# Step 1: fetch a representative mainnet sampling via R2 webseeds
./build/bin/seg-bench fetch --chain mainnet --out samples/

# Step 2: run the benchmark
./build/bin/seg-bench run --input samples/ --out report.md

# Optional flags for `run`:
#   --codecs snappy,zstd-3,zstd-9,zstd-19   (default; comma-separated)
#   --repeats 5                              (best-of-N timing)
#   --random-access-samples 2000             (random reads per codec)
#   --seed 1                                 (RNG seed, for reproducibility)
```

## What gets measured

For each input `.seg` file:

1. The file is decompressed through `db/seg` (Erigon's existing
   `seg/compress` reader) to recover the raw record stream.
2. For each comparison codec, the records are re-encoded with
   **per-record framing** — one codec frame per record, preserving
   the random-access property today's `.seg` files have. The
   measurement vehicle is a small purpose-built file format
   ([format.go](./format.go)) with a record-count + offset table
   header followed by concatenated codec frames.
3. Round-trip integrity is asserted: re-decoding the framed bytes
   must yield the original record stream byte-for-byte. Mismatch is
   a hard error — the size and timing numbers are only reported for
   verified-correct codecs.
4. Timings:
   - **Encode time** — best of N repeats; reported in ms/MB.
   - **Sequential decode time** — best of N repeats; ms/MB.
   - **Random-access decode latency** — K random record indices
     (default 2000), per-access seek + frame-read + codec decode;
     reported as median and p99 µs/record. This is the primary
     serving-path number.
5. Output: a single markdown report with one table per file plus a
   roll-up across all files.

Per-record framing for zstd means "one zstd frame per record."
Whole-stream framing would yield smaller numbers but isn't an
apples-to-apples replacement for `seg/compress` because the codec
must be independently decodable from a per-record offset for the
`.bt` / `.kvi` index to work.

## What is NOT measured (v1)

- **Trained zstd dictionary.** Requires a held-out training set
  distinct from the measurement set. Worth adding as v2.
- **Whole-stream framing.** As a separate measurement, useful for
  characterising the per-record-framing tax. Out of scope here.
- **Cold-cache random-access.** v1 measures hot-cache (the
  realistic warm-serving-node scenario). Cold-cache (page cache
  flushed between samples) is a v2 measurement for the disk-I/O
  floor.
- **State-domain `.kv` files.** Different methodology (the
  `seg/compress` 4× ratio on `.kv` files reflects file-level value
  sharing that naive recompression wouldn't reproduce). Block-stage
  only here.

## File layout

| File | Role |
|---|---|
| `main.go` | CLI dispatch (`fetch` / `run`) |
| `fetch.go` | Webseed download with the R2 Cloudflare header |
| `format.go` | Per-record framing format (offset table + concatenated frames) |
| `codecs.go` | Codec implementations (snappy, zstd at supported levels) |
| `bench.go` | Benchmark loop: decompress, re-encode, verify, time, measure |
| `report.go` | Markdown report rendering |

## Webseed access

`seg-bench fetch` uses Erigon's `db/snapcfg` package to fetch from
the R2 endpoint with the Cloudflare header
(see [`db/snapcfg/cdn.go`](../../db/snapcfg/cdn.go)). No torrent
client, no Erigon datadir bootstrap — just `curl`-equivalent
HTTP fetches with one custom header.

Each call:

```
GET https://erigon-snapshots.erigon.network/<branch>/<file>.seg
Header: lsjdjwcush6jbnjj3jnjscoscisoc5s: <token>
```
