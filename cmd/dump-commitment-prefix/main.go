// Standalone diagnostic: dumps every dup row in kv.TblCommitmentVals for a
// supplied compact-hex prefix. Opens the chaindata read-only via Accede so it
// can run alongside a live erigon process.
//
// Each printed row shows the encoded step coordinate (from the DupSort value's
// 8-byte prefix), the decoded step, and the length of the trailing branch
// payload (or "<tombstone>" if the payload is empty).
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func main() {
	dir := flag.String("chaindata", "", "path to chaindata directory")
	prefixHex := flag.String("prefix", "", "compact-hex prefix (no 0x) to read")
	tableName := flag.String("table", kv.TblCommitmentVals, "DupSort values table to dump (e.g. StorageVals, AccountVals, CommitmentVals)")
	stepSize := flag.Uint64("step-size", 390625, "txns per step (default 390625 for mainnet/hoodi)")
	flag.Parse()
	if *dir == "" || *prefixHex == "" {
		fmt.Fprintln(os.Stderr, "usage: --chaindata=PATH --prefix=HEX [--step-size=N]")
		os.Exit(2)
	}
	prefix, err := hex.DecodeString(*prefixHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decode prefix: %v\n", err)
		os.Exit(2)
	}

	logger := log.New()
	ctx := context.Background()
	db, err := mdbx.New(dbcfg.ChainDB, logger).Path(*dir).Accede(true).Readonly(true).Open(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "begin ro: %v\n", err)
		os.Exit(1)
	}
	defer tx.Rollback()

	c, err := tx.CursorDupSort(*tableName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cursor: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	fmt.Printf("table=%s prefix=%x prefixLen=%d step_size=%d\n", *tableName, prefix, len(prefix), *stepSize)
	fmt.Printf("decoded step = ^binary.BigEndian.Uint64(value[:8])\n")
	fmt.Printf("higher step sorts first within a dup-key\n\n")

	v, err := c.SeekBothRange(prefix, []byte{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "SeekBothRange: %v\n", err)
		os.Exit(1)
	}
	k, _, _ := c.Current()
	if k == nil {
		fmt.Println("no row found for this prefix")
		return
	}
	if !bytes.Equal(k, prefix) {
		fmt.Printf("key drift: SeekBothRange landed on key=%x (different from prefix=%x); no exact-match row\n", k, prefix)
		return
	}

	type row struct {
		encodedStep uint64
		step        uint64
		payloadLen  int
		stepStartTx uint64
		stepEndTx   uint64
		tombstone   bool
	}
	var rows []row
	for v != nil {
		if err != nil {
			fmt.Fprintf(os.Stderr, "current: %v\n", err)
			os.Exit(1)
		}
		if len(v) < 8 {
			fmt.Printf("malformed dup: len(value)=%d (<8)\n", len(v))
			continue
		}
		encoded := binary.BigEndian.Uint64(v[:8])
		step := ^encoded
		payload := v[8:]
		rows = append(rows, row{
			encodedStep: encoded,
			step:        step,
			payloadLen:  len(payload),
			stepStartTx: step * *stepSize,
			stepEndTx:   (step + 1) * *stepSize,
			tombstone:   len(payload) == 0,
		})
		_, v, err = c.NextDup()
	}

	fmt.Printf("found %d dup row(s) for this prefix in writable shadow:\n", len(rows))
	fmt.Printf("%-6s  %-6s  %-12s  %-12s  %-6s  %s\n", "idx", "step", "stepStart", "stepEnd", "len", "kind")
	for i, r := range rows {
		kind := "branch"
		if r.tombstone {
			kind = "TOMBSTONE"
		}
		fmt.Printf("%-6d  %-6d  %-12d  %-12d  %-6d  %s\n", i, r.step, r.stepStartTx, r.stepEndTx, r.payloadLen, kind)
	}
}
