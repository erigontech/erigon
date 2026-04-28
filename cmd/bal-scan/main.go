// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// One-shot dev tool: scan the kv.BlockAccessList table and print one line per
// entry whose block number falls in [low, high]. Used to debug what BAL bytes
// are actually persisted (e.g. after the BAL downloader fetches from peers,
// to confirm the writes survived prune/compaction):
//
//	bal-scan <chaindata-dir> [low] [high]
//
// low/high default to 0 and ^uint64(0). Requires the MDBX to be unlocked
// (erigon stopped) for read access.

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bal-scan <chaindata-dir> [low] [high]")
		os.Exit(2)
	}
	path := os.Args[1]
	var low uint64 = 0
	var high uint64 = ^uint64(0)
	if len(os.Args) > 2 {
		low, _ = strconv.ParseUint(os.Args[2], 10, 64)
	}
	if len(os.Args) > 3 {
		high, _ = strconv.ParseUint(os.Args[3], 10, 64)
	}

	logger := log.New()
	db, err := mdbx.New(kv.Label(dbcfg.ChainDB), logger).Path(path).Readonly(true).Accede(true).Open(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	count := 0
	matched := 0
	err = db.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.BlockAccessList)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) < 8 {
				continue
			}
			n := binary.BigEndian.Uint64(k[:8])
			if n >= low && n <= high {
				fmt.Printf("block=%d hash=%x bal_len=%d\n", n, k[8:], len(v))
				matched++
			}
			count++
		}
		return nil
	})
	fmt.Fprintf(os.Stderr, "total %d entries; matched %d in [%d,%d]; err=%v\n", count, matched, low, high, err)
}
