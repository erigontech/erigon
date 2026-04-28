// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// One-shot dev tool: dump / delete / compare BlockAccessList rawdb entries
// for a list of block numbers. Used to drive the eth/71 BAL downloader test:
//
//	bal-test dump   --datadir=<dir> --blocks=N1,N2,...   > truth.json
//	bal-test delete --datadir=<dir> --blocks=N1,N2,...
//	# (restart erigon, wait for BALDownloader to refetch)
//	bal-test compare --datadir=<dir> --blocks=N1,N2,...  --truth=truth.json
//
// Requires the chaindata MDBX to be unlocked (erigon stopped) for delete.

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
)

type entry struct {
	Number uint64 `json:"number"`
	Hash   string `json:"hash"`
	BAL    string `json:"bal"` // hex-encoded
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bal-test <dump|delete|compare> [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	datadir := fs.String("datadir", "", "erigon datadir (must be unlocked for delete)")
	blocksStr := fs.String("blocks", "", "comma-separated block numbers (e.g. 100,101,102)")
	truth := fs.String("truth", "", "(compare only) path to truth.json from dump")
	_ = fs.Parse(os.Args[2:])

	if *datadir == "" || *blocksStr == "" {
		fmt.Fprintln(os.Stderr, "--datadir and --blocks are required")
		os.Exit(2)
	}
	blocks, err := parseBlocks(*blocksStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad --blocks:", err)
		os.Exit(2)
	}

	logger := log.New()
	chainDB := *datadir + "/chaindata"

	switch cmd {
	case "dump":
		readOnly := true
		entries, err := readEntries(chainDB, blocks, readOnly, logger)
		if err != nil {
			fmt.Fprintln(os.Stderr, "dump failed:", err)
			os.Exit(1)
		}
		_ = json.NewEncoder(os.Stdout).Encode(entries)
	case "delete":
		if err := deleteEntries(chainDB, blocks, logger); err != nil {
			fmt.Fprintln(os.Stderr, "delete failed:", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "deleted %d block(s)\n", len(blocks))
	case "compare":
		if *truth == "" {
			fmt.Fprintln(os.Stderr, "--truth required")
			os.Exit(2)
		}
		if err := compare(chainDB, blocks, *truth, logger); err != nil {
			fmt.Fprintln(os.Stderr, "compare failed:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", cmd)
		os.Exit(2)
	}
}

func parseBlocks(s string) ([]uint64, error) {
	out := []uint64{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", p, err)
		}
		out = append(out, n)
	}
	return out, nil
}

func openDB(path string, readOnly bool, logger log.Logger) (kv.RwDB, error) {
	b := mdbx.New(kv.Label(dbcfg.ChainDB), logger).Path(path)
	if readOnly {
		b = b.Readonly(true).Accede(true)
	} else {
		b = b.Accede(true)
	}
	return b.Open(context.Background())
}

func readEntries(path string, blocks []uint64, readOnly bool, logger log.Logger) ([]entry, error) {
	db, err := openDB(path, readOnly, logger)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	out := make([]entry, 0, len(blocks))
	err = db.View(context.Background(), func(tx kv.Tx) error {
		for _, n := range blocks {
			hash, err := canonicalHash(tx, n)
			if err != nil {
				return fmt.Errorf("block %d: %w", n, err)
			}
			if hash == (common.Hash{}) {
				out = append(out, entry{Number: n})
				continue
			}
			bal, err := rawdb.ReadBlockAccessListBytes(tx, hash, n)
			if err != nil {
				return fmt.Errorf("read BAL block %d: %w", n, err)
			}
			out = append(out, entry{
				Number: n,
				Hash:   "0x" + hex.EncodeToString(hash[:]),
				BAL:    "0x" + hex.EncodeToString(bal),
			})
		}
		return nil
	})
	return out, err
}

func deleteEntries(path string, blocks []uint64, logger log.Logger) error {
	db, err := openDB(path, false, logger)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	return db.Update(context.Background(), func(tx kv.RwTx) error {
		for _, n := range blocks {
			hash, err := canonicalHash(tx, n)
			if err != nil {
				return fmt.Errorf("block %d: %w", n, err)
			}
			if hash == (common.Hash{}) {
				fmt.Fprintf(os.Stderr, "block %d: no canonical hash, skip\n", n)
				continue
			}
			key := dbutils.BlockBodyKey(n, hash)
			if err := tx.Delete(kv.BlockAccessList, key); err != nil {
				return fmt.Errorf("delete block %d: %w", n, err)
			}
			fmt.Fprintf(os.Stderr, "deleted BAL for block %d hash %x\n", n, hash[:8])
		}
		return nil
	})
}

func compare(path string, blocks []uint64, truthPath string, logger log.Logger) error {
	tdata, err := os.ReadFile(truthPath)
	if err != nil {
		return fmt.Errorf("read truth: %w", err)
	}
	var truthEntries []entry
	if err := json.Unmarshal(tdata, &truthEntries); err != nil {
		return fmt.Errorf("parse truth: %w", err)
	}
	truthBy := map[uint64]entry{}
	for _, e := range truthEntries {
		truthBy[e.Number] = e
	}

	current, err := readEntries(path, blocks, true, logger)
	if err != nil {
		return err
	}

	matched := 0
	missing := 0
	mismatch := 0
	for _, c := range current {
		t, ok := truthBy[c.Number]
		if !ok {
			fmt.Printf("block %d: NOT IN TRUTH\n", c.Number)
			continue
		}
		if c.BAL == "0x" {
			fmt.Printf("block %d: still missing in current rawdb\n", c.Number)
			missing++
			continue
		}
		if c.BAL == t.BAL && c.Hash == t.Hash {
			fmt.Printf("block %d: MATCH (%d bytes)\n", c.Number, (len(c.BAL)-2)/2)
			matched++
			continue
		}
		fmt.Printf("block %d: MISMATCH\n  truth   %d bytes (sha %s...)\n  current %d bytes (sha %s...)\n",
			c.Number,
			(len(t.BAL)-2)/2, shortSha(t.BAL),
			(len(c.BAL)-2)/2, shortSha(c.BAL))
		mismatch++
	}
	fmt.Printf("\nresult: %d matched, %d still missing, %d mismatch\n", matched, missing, mismatch)
	if mismatch > 0 {
		return fmt.Errorf("byte mismatch on %d block(s)", mismatch)
	}
	return nil
}

func shortSha(hexStr string) string {
	if len(hexStr) < 18 {
		return hexStr
	}
	return hexStr[:18]
}

func canonicalHash(tx kv.Tx, number uint64) (common.Hash, error) {
	v, err := tx.GetOne(kv.HeaderCanonical, encodeBE(number))
	if err != nil {
		return common.Hash{}, err
	}
	if len(v) != 32 {
		return common.Hash{}, nil
	}
	var h common.Hash
	copy(h[:], v)
	return h, nil
}

func encodeBE(n uint64) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(n)
		n >>= 8
	}
	return b
}
