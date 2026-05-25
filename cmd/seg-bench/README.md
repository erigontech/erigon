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
go build -o build/bin/seg-bench ./cmd/seg-bench

# Step 1: fetch a representative mainnet sampling via R2 webseeds
./build/bin/seg-bench fetch --chain mainnet --out samples/

# Step 2: run the benchmark
./build/bin/seg-bench run --input samples/ --out report.md

# Optional flags for `run`:
#   --codecs snappy,zstd-3,zstd-9,zstd-19   (default; comma-separated)
#   --framing per-record                     (also: whole-stream; or both: per-record,whole-stream)
#   --dict /path/to/trained-zstd.dict        (required by any zstd-*+dict codec)
#   --repeats 5                              (best-of-N timing)
#   --random-access-samples 2000             (random reads per codec)
#   --seed 1                                 (RNG seed, for reproducibility)

# Trained dictionary (v2): train out-of-band with the zstd CLI, then pass via --dict.
# Use a held-out training set distinct from the measurement set to avoid overfitting:
#
#   zstd --train -B<recordSize> -o trained.dict samples-train/*.seg-records-extracted
#   ./build/bin/seg-bench run --input samples-measure/ \
#                             --codecs snappy,zstd-3,zstd-3+dict,zstd-19,zstd-19+dict \
#                             --dict trained.dict --out report.md
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

## What IS measured (v2)

- **Both framing modes**: per-record (random-access shape today's
  `.seg` and era1 use) and whole-stream (bulk-sync optimum; one
  codec frame for the whole file). The size delta between the two
  on the same codec is the **per-record-framing tax**.
- **Trained zstd dictionary** as a codec variant (`zstd-3+dict`,
  `zstd-19+dict`). Requires the `--dict <path>` flag pointing at a
  pre-trained dictionary file. See [§ Trained dictionary](#trained-dictionary)
  below for the workflow.

## What is NOT measured (still v2.1+)

- **Streaming variant** for files too big to hold in memory (the
  mid-era transactions `.seg` is ~15 GB). Disk-backed per-record
  framing is the natural next addition.
- **Era1 input format**. The benchmark currently reads `.seg` files
  via `db/seg`; era1 input would use `db/era`'s reader (currently
  unmerged on the main branch — sits as untracked work). Required
  for the Q2 (interchange-codec) measurement to be honest; until
  then the snappy column on `.seg` input is a *proxy* for Q2,
  not a real era1 measurement.
- **Cold-cache random-access.** v2 measures hot-cache (warm
  serving-node scenario). Cold-cache (page cache flushed between
  samples) is a v2.1 measurement for the disk-I/O floor.
- **State-domain `.kv` files.** Different methodology (the
  `seg/compress` 4× ratio on `.kv` files reflects file-level value
  sharing that naive recompression wouldn't reproduce). Block-stage
  only here.

## Trained dictionary

To measure `zstd-3+dict` / `zstd-19+dict` honestly, train the
dictionary on a **held-out subset** distinct from the measurement
set — otherwise the dictionary is overfit to its own measurement
data and the size numbers are flattering.

Recommended workflow:

1. Split the sample files into `train/` and `measure/` (e.g. early-
   era files for training; mid-era + late-era for measurement).
2. Extract raw records from the train set into a directory (one
   file per record, or one big concatenated file with length
   prefixes — `zstd --train` is flexible).
3. Run `zstd --train -o trained.dict <train-records>`. Default
   dictionary size is 110 KB; tweak with `--maxdict=64K` or
   `--maxdict=256K` to characterise the size/ratio curve.
4. Pass `--dict trained.dict` to `seg-bench run` and add
   `zstd-3+dict`, `zstd-19+dict` to the codec list.

Training is one-off (out of the benchmark's hot path); the
dictionary file is small (KB–hundred-KB range) and ships alongside
the bench results for reproducibility.

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
