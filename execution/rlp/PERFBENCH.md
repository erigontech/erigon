# RLP layer performance harness

Independent, repeatable benchmark + escape-analysis harness for the RLP
layer and its top hot-path consumer (`Header` decode/encode/hash).

Designed to give a stable before/after measurement when iterating on
allocation reductions in the RLP layer.

## Background

Heap profiling of the sload-same-key bench
(`/home/erigon/perf_results/amsterdam-bench-pd3/step2c-aclshortcut-sload-v2/heap-post.pprof`)
flagged the RLP decode path as a top allocator family:

- By **alloc count**: `rlp.(*Stream).Decode` 29%, `(*Header).DecodeRLP`
  9%, `rlp.DecodeBytes(Partial)` 7% each.
- By **alloc bytes**: `freezeblocks.ForEachHeader.func1` is the single
  biggest byte allocator on the workload (~52% cumulative) — the
  `var header types.Header` declaration inside the loop escapes to the
  heap on every iteration.

The harness here lets us measure the impact of any RLP-layer changes
without re-running the full sload bench.

## Files

| File | Purpose |
|---|---|
| `streambench_test.go` | Synthetic benchmarks of `Stream.Bytes`, `Stream.ReadBytes`, `DecodeBytes` — no Header dependency |
| `../types/headerbench_test.go` | Real-header benchmarks using captured mainnet headers — measures the full Header decode/encode/hash path |
| `../types/testdata/headers-mainnet-00099.rlp` | 100 pre-London headers (mainnet epoch 99, ~Sep 2015) |
| `../types/testdata/headers-mainnet-01894.rlp` | 100 post-London headers (mainnet epoch 1894, ~Aug 2022, peak pre-merge) |
| `escape-report.sh` | Captures escape-analysis output for the RLP layer + Header decode site + ForEachHeader callsite |
| `baseline.txt` | Reference bench output captured when the harness was committed. See `git log execution/rlp/baseline.txt` for the commit it represents. |

The testdata files are length-prefixed RLP blobs:
`[u32 LE length][rlp bytes]` repeated. Total ~108 KB.

## Running

### Benchmarks

```
go test -run=^$ -bench='BenchmarkDecodeHeader|BenchmarkEncodeHeader|BenchmarkHeaderHash|BenchmarkStreamBytes|BenchmarkStreamReadBytes|BenchmarkDecodeBytes_Tiny' \
        -benchmem -count=5 -benchtime=1s \
        ./execution/rlp/ ./execution/types/ \
        | tee bench-new.txt
```

Then compare against the committed baseline:

```
benchstat execution/rlp/baseline.txt bench-new.txt
```

`benchstat` is available via `go install golang.org/x/perf/cmd/benchstat@latest`.

### Escape report

```
./execution/rlp/escape-report.sh
```

Produces three files in `execution/rlp/`:

- `escape-rlp.txt` — escapes inside the RLP package
- `escape-types-header.txt` — escapes inside `execution/types`
  involving Header / Hash / DecodeRLP / EncodeRLP
- `escape-foreachheader.txt` — escapes in the
  `db/snapshotsync/freezeblocks` package involving the Header walker

These are gitignored — they're generated artefacts, not source.

## Expected workflow for an optimisation PR

1. Branch from this commit
2. `cp execution/rlp/baseline.txt /tmp/before.txt`
3. Apply the optimisation
4. Run the bench again into `/tmp/after.txt`
5. `benchstat /tmp/before.txt /tmp/after.txt`
6. Include the benchstat diff in the PR description
7. Re-run the escape report to confirm escape removal (where claimed)

## Known allocation hot-spots (from the audit)

| Site | What | Fix path |
|---|---|---|
| `freezeblocks.ForEachHeader.func1` line 1053 — `var header types.Header` | Per-iteration heap alloc of full Header struct | Hoist out of loop + verify walker doesn't retain |
| `rlp.DecodeBytes(b, val any)` — `val any` boxing | Forces escape at the entry point regardless of caller | Add concrete-typed helpers (e.g. `types.DecodeHeader(b []byte, dst *Header)`) for hot types |
| `decodeDecoder` `val.Addr().Interface().(Decoder)` — interface boxing inside the reflective dispatch | Forces escape at the dispatcher | Bypass by calling the type's `DecodeRLP` directly via a concrete helper (same fix as above) |
| `Stream.Bytes()` line 705 — `make([]byte, size)` per call | Fresh slice per variable-length field decode (e.g. Header.Extra) | Add `Stream.BytesInto(scratch []byte) ([]byte, error)` for caller-owned scratch |
| `Header.Hash()` mutable path — RlpHash → `rlp.Encode(sha, x any)` | Header pointer escapes via `any` boxing | Memoise on mutable Header behind a dirty flag, OR provide concrete `EncodeHeader(w, h)` helper |

The first two (hoist + concrete `DecodeHeader`) capture the majority of
the byte-allocation cost. The Stream.BytesInto and Hash fixes are smaller
incremental wins.
