# Fuzzing

Erigon is fuzz-tested at the parsing and decoding boundaries that handle
untrusted input (peer messages, RLP/ABI payloads, on-disk codecs). This page
explains what is fuzzed, how to run the fuzzers locally, the nightly CI job, and
the [OSS-Fuzz](https://github.com/google/oss-fuzz) integration that runs them
continuously.

## Background

A fuzzer feeds randomly mutated inputs into a function looking for panics,
hangs, or assertion failures. Erigon uses Go's built-in
[native fuzzing](https://go.dev/doc/security/fuzz/) (`func FuzzXxx(f *testing.F)`
in `*_test.go` files) — no external tooling or build tags required.

Two things run these fuzzers:

- **The Go toolchain locally / in CI** (`go test -fuzz`) — see below.
- **OSS-Fuzz**, Google's continuous fuzzing service, which builds them into
  libFuzzer binaries and fuzzes them around the clock on dedicated
  infrastructure, reporting bugs to the Erigon maintainers.

## Fuzz targets

| Package | Fuzzers |
|---|---|
| `common/bitutil` | `FuzzEncoder`, `FuzzDecoder` |
| `db/datastruct/fusefilter` | `FuzzReaderOnBytes`, `FuzzReaderShardedOnBytes`, `FuzzWriterRoundTrip` |
| `db/recsplit/eliasfano16` | `FuzzSingleEliasFano`, `FuzzDoubleEliasFano` |
| `db/recsplit/eliasfano32` | `FuzzSingleEliasFano`, `FuzzDoubleEliasFano` |
| `db/recsplit` | `FuzzRecSplit` |
| `db/seg` | `FuzzCompress`, `FuzzDecompressMatch` |
| `db/seg/patricia` | `FuzzPatricia`, `FuzzLongestMatch` |
| `execution/abi` | `FuzzABI` |
| `execution/commitment/nibbles` | `FuzzHexCompactRoundtrip` |
| `execution/types` | `FuzzRLP` |
| `execution/vm` | `FuzzPrecompiledContracts` |
| `execution/vm/runtime` | `FuzzVmRuntime` |
| `txnprovider/txpool` | `FuzzParseTx`, `FuzzPooledTransactions66`, `FuzzGetPooledTransactions66`, `FuzzOnNewBlocks` |

Each target keeps a committed seed corpus under its package's
`testdata/fuzz/<FuzzName>/`. These seeds are replayed as ordinary unit tests by
`go test ./...` (no `-fuzz` flag), so the seed corpus is already a regression
guard in the normal test suite.

## Running locally

Run one target with active mutation, either via the Makefile helper:

```bash
make fuzz PKG=./db/seg FUZZ=FuzzCompress FUZZTIME=30s   # FUZZTIME defaults to 60s
```

or directly with `go test`:

```bash
go test ./db/seg/ -run '^$' -fuzz '^FuzzCompress$' -fuzztime 30s
```

- `-run '^$'` disables the normal tests so only the fuzzer runs.
- `-fuzz` takes a single regexp matching one target in that package.
- Newly discovered "interesting" inputs are cached under
  `$(go env GOCACHE)/fuzz`; a crashing input is written to the package's
  `testdata/fuzz/<FuzzName>/` so it can be committed and replayed.

Replay just the seed + discovered corpus (no mutation), e.g. to reproduce a
crash file someone committed:

```bash
go test ./db/seg/ -run '^FuzzCompress$'
```

## Nightly CI

[`.github/workflows/test-fuzz.yml`](../.github/workflows/test-fuzz.yml) runs
every target nightly (03:00 UTC) on a free GitHub-hosted runner, each for a
short `fuzztime`, with a per-target corpus cache so each run builds on the
previous night's findings. It can also be triggered manually
(`workflow_dispatch`) with a custom `fuzztime` for a deeper ad-hoc run.

This is intentionally **not** part of the PR/merge CI gate: seed-corpus
regression is already covered by the normal test suite, and putting mutation
fuzzing on the gate would make it flaky (a newly found crash would fail
unrelated PRs).

On a scheduled failure the workflow:

1. uploads each crash's minimized reproducer as a `fuzz-crash-<target>`
   artifact on the run;
2. opens or updates a tracking issue labelled `nightly-fuzz`;
3. posts a Discord alert if the `DISCORD_WEBHOOK` repository secret is set
   (it no-ops quietly otherwise).

## OSS-Fuzz integration

The continuous integration with OSS-Fuzz lives in the
[`google/oss-fuzz`](https://github.com/google/oss-fuzz) repository under
`projects/erigon/` (`project.yaml`, `Dockerfile`, `build.sh`) — **not** in this
repo, to avoid drift with upstream's tooling. The `build.sh` there wires the
same native fuzzers up via `compile_native_go_fuzzer`.

- Project: https://github.com/google/oss-fuzz/tree/master/projects/erigon
- Onboarding PR: https://github.com/google/oss-fuzz/pull/15642
- OSS-Fuzz Go guide: https://google.github.io/oss-fuzz/getting-started/new-project-guide/go-lang/

Bugs found by OSS-Fuzz are reported privately to the project contacts and made
public after the standard 90-day disclosure window.

> **Note:** the `txnprovider/txpool` targets link the MDBX C library. They run
> in local/nightly CI fine, but are deferred in the upstream `build.sh` until
> validated under the OSS-Fuzz sanitizer toolchain.

## Adding a new fuzzer

1. Write `func FuzzXxx(f *testing.F)` in a `*_test.go` file, seeding it with
   `f.Add(...)` calls.
2. Commit a small seed corpus under `testdata/fuzz/FuzzXxx/` if useful.
3. Add the target to the matrix in
   [`.github/workflows/test-fuzz.yml`](../.github/workflows/test-fuzz.yml).
4. Add a `compile_native_go_fuzzer` line to `projects/erigon/build.sh` in the
   OSS-Fuzz repo (separate PR) so it is fuzzed continuously.

New fuzzers are **not** picked up automatically by either CI or OSS-Fuzz — both
lists are explicit.

## When a fuzzer finds a crash

A fuzz crash is a real failure. Investigate and fix it — do not skip the test or
delete the crash corpus (see the test-skip policy in the root `CLAUDE.md`).
Commit the reproducing input under `testdata/fuzz/<FuzzName>/` so the fix is
verified and the case stays covered.
