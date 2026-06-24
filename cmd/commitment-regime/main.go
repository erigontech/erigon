// Command commitment-regime classifies a commitment .kv file as plain or referenced
// (shortened keys). Temporary diagnostic tooling as-is — remove once the regime work
// settles, or promote into cmd/integration.
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// scan reads every key/value pair greedily and stops at the first branch that
// carries a shortened key. referenced=true means at least one shortened key was
// found; firstAt is the 1-based pair index where it was found.
func scan(path string) (referenced bool, firstAt, pairs uint64, err error) {
	d, derr := seg.NewDecompressor(path)
	if derr != nil {
		return false, 0, 0, derr
	}
	defer d.Close()

	defer func() {
		if rec := recover(); rec != nil {
			referenced, err = true, fmt.Errorf("corrupt word stream: %v", rec)
		}
	}()

	r := seg.NewReader(d.MakeGetter(), seg.CompressKeys)
	var keyBuf, valBuf []byte
	for r.HasNext() {
		keyBuf, _ = r.Next(keyBuf[:0])
		if !r.HasNext() {
			break
		}
		valBuf, _ = r.Next(valBuf[:0])
		pairs++
		if bytes.Equal(keyBuf, commitmentdb.KeyCommitmentState) {
			continue
		}
		if commitment.BranchData(valBuf).HasShortenedKeys() {
			return true, pairs, pairs, nil
		}
	}
	return false, 0, pairs, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: commitment-regime <commitment.kv> [more.kv ...]")
		os.Exit(2)
	}
	for _, path := range os.Args[1:] {
		referenced, firstAt, pairs, err := scan(path)
		name := filepath.Base(path)
		switch {
		case err != nil && referenced:
			fmt.Printf("%-40s referenced (%v, scanned %d)\n", name, err, pairs)
		case err != nil:
			fmt.Printf("%-40s ERROR %v\n", name, err)
		case referenced:
			fmt.Printf("%-40s referenced (first shortened key at pair %d of %d)\n", name, firstAt, pairs)
		default:
			fmt.Printf("%-40s plain (scanned all %d pairs)\n", name, pairs)
		}
	}
}
