// Command seg-bench measures the size, encode time, decode time, and
// random-access decode latency of recompressing Erigon .seg files
// under various codecs (snappy, zstd at multiple levels).
//
// The point of comparison is the current seg/compress baseline (file
// size on disk); the prototype does NOT re-encode under seg/compress
// itself — it uses the existing .seg file as the baseline and
// re-encodes the decompressed record stream with each comparison
// codec.
//
// See cmd/seg-bench/README.md for usage and
// erigon3/history-network/blocks/codec-benchmark-design.md (in the
// erigon-documents repo) for the methodology.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "fetch":
		runFetch(os.Args[2:])
	case "run":
		runBench(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `seg-bench — codec benchmark for Erigon .seg files

Usage:
  seg-bench fetch --chain mainnet [--branch main] [--out samples/]
      Fetch a representative sampling of mainnet .seg files via the
      R2 webseed endpoint, using the Cloudflare header from
      db/snapcfg/cdn.go. Skips files already present in --out.

  seg-bench run --input samples/ [--out report.md] [--codecs ...]
                [--repeats 5] [--random-access-samples 2000]
                [--seed 1]
      Run the benchmark over every .seg file in --input. For each
      file: decompress through db/seg, recompress under each codec
      using the per-record framing format defined in format.go,
      verify round-trip, measure size / encode / decode / random-
      access. Output is a markdown report.

  seg-bench help
      Print this message.

See cmd/seg-bench/README.md for the methodology and the design note
in erigon-documents (erigon3/history-network/blocks/codec-benchmark-design.md).`)
}
