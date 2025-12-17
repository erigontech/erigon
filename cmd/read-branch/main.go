// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

var (
	flagDatadir = flag.String("datadir", "", "path to the datadir")
	flagPrefix  = flag.String("prefix", "", `hex prefix to read (default: "")`)
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `read-branch - Read branch data from the commitment domain

Usage:
  read-branch --datadir <path> [--prefix <hex>]

Description:
  Opens the commitment domain from a given datadir and reads the branch data
  for the specified prefix. The prefix should be provided as hex nibbles.

Examples:
  read-branch --datadir ~/data/eth-mainnet --prefix aa
  read-branch --datadir /path/to/datadir --prefix 0a1b

Flags:
`)
		flag.PrintDefaults()
	}

	flag.Parse()

	if *flagDatadir == "" {
		fmt.Println("Error: --datadir is required")
		flag.Usage()
		os.Exit(1)
	}

	prefix, err := commitment.PrefixStringToNibbles(*flagPrefix)
	if err != nil {
		fmt.Printf("Failed to read prefix %v", err)
		os.Exit(1)
	}

	ctx := context.Background()
	temporalDB, err := createTemporalDB(ctx, *flagDatadir)
	if err != nil {
		fmt.Printf("Error creating commitment reader %v", err)
		os.Exit(1)
	}
	defer temporalDB.Close()

	// Start a read-only temporal transaction
	tx, err := temporalDB.BeginTemporalRo(ctx)
	if err != nil {
		fmt.Printf("failed to begin temporal tx: %v", err)
		os.Exit(1)
	}
	defer tx.Rollback()

	// Use LatestStateReader to read from the commitment domain.
	// This is the same approach used by commitmentdb.TrieContext.Branch internally:
	// TrieContext.Branch -> TrieContext.readDomain -> StateReader.Read
	commitmentReader := commitmentdb.NewLatestStateReader(tx)

	if err := readBranch(commitmentReader, prefix); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func createTemporalDB(ctx context.Context, datadirPath string) (*temporal.DB, error) {
	dirs := datadir.New(datadirPath)
	logger := log.New()
	// Open  raw mdbx database
	rawDB, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(dirs.Chaindata).
		Accede(true). // don't create, just open existing
		Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open chaindata: %w", err)
	}

	agg, err := state.New(dirs).Logger(logger).Open(ctx, rawDB)
	if err != nil {
		return nil, fmt.Errorf("failed to open aggregator: %w", err)
	}

	if err := agg.OpenFolder(); err != nil {
		return nil, fmt.Errorf("failed to open aggregator folder: %w", err)
	}

	// Create temporal database wrapper
	tdb, err := temporal.New(rawDB, agg)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporal db: %w", err)
	}
	return tdb, nil
}

func readBranch(stateReader *commitmentdb.LatestStateReader, prefix []byte) error {
	compactKey := commitment.HexNibblesToCompactBytes(prefix)
	val, step, err := stateReader.Read(kv.CommitmentDomain, compactKey, config3.DefaultStepSize)
	if err != nil {
		return fmt.Errorf("failed to get branch for prefix %x: %w", prefix, err)
	}

	fmt.Printf("Prefix: 0x%s\n", hex.EncodeToString(prefix))
	fmt.Printf("Step: %d\n", step)

	if len(val) == 0 {
		fmt.Println("Branch data: <empty>")
		return nil
	}

	fmt.Printf("Branch data (hex): %s\n", hex.EncodeToString(val))
	fmt.Printf("Branch data length: %d bytes\n", len(val))

	// Parse and display the branch data in human-readable format
	branchData := commitment.BranchData(val)
	fmt.Printf("\nParsed branch data:\n%s\n", branchData.String())

	return nil
}
