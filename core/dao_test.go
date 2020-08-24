// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"context"
	"math/big"
	"runtime"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// Tests that DAO-fork enabled clients can properly filter out fork-commencing
// blocks based on their extradata fields.
func TestDAOForkRangeExtradata(t *testing.T) {
	forkBlock := big.NewInt(32)

	// Generate a common prefix for both pro-forkers and non-forkers
	db := ethdb.NewMemDatabase()
	defer db.Close()
	gspec := &Genesis{Config: params.TestChainConfig}
	genesis := gspec.MustCommit(db)

	proDb := ethdb.NewMemDatabase()
	defer proDb.Close()
	gspec.MustCommit(proDb)

	// Create the concurrent, conflicting two nodes
	proConf := *params.TestChainConfig
	proConf.DAOForkBlock = forkBlock
	proConf.DAOForkSupport = true
	txCacher := NewTxSenderCacher(runtime.NumCPU())
	proBc, _ := NewBlockChain(proDb, nil, &proConf, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer proBc.Stop()

	prefix, _, err := GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db, int(forkBlock.Int64()-1), func(i int, gen *BlockGen) {}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate prefix chain: %v", err)
	}

	conDb := ethdb.NewMemDatabase()
	defer conDb.Close()
	gspec.MustCommit(conDb)

	conConf := *params.TestChainConfig
	conConf.DAOForkBlock = forkBlock
	conConf.DAOForkSupport = false

	txCacherConBc := NewTxSenderCacher(runtime.NumCPU())
	conBc, _ := NewBlockChain(conDb, nil, &conConf, ethash.NewFaker(), vm.Config{}, nil, txCacherConBc)
	defer conBc.Stop()

	if _, err = proBc.InsertChain(context.Background(), prefix); err != nil {
		t.Fatalf("pro-fork: failed to import chain prefix: %v", err)
	}
	if _, err = conBc.InsertChain(context.Background(), prefix); err != nil {
		t.Fatalf("con-fork: failed to import chain prefix: %v", err)
	}
	// Try to expand both pro-fork and non-fork chains iteratively with other camp's blocks
	for i := int64(0); i < params.DAOForkExtraRange.Int64(); i++ {
		t.Run(strconv.Itoa(int(i)), func(t *testing.T) {
			// Create a pro-fork block, and try to feed into the no-fork chain
			db = ethdb.NewMemDatabase()
			defer db.Close()
			gspec.MustCommit(db)
			txCacherSubTest := NewTxSenderCacher(runtime.NumCPU())
			bc, _ := NewBlockChain(db, nil, &conConf, ethash.NewFaker(), vm.Config{}, nil, txCacherSubTest)
			defer bc.Stop()

			blocks := conBc.GetBlocksFromHash(conBc.CurrentBlock().Hash(), int(conBc.CurrentBlock().NumberU64()))
			for j := 0; j < len(blocks)/2; j++ {
				blocks[j], blocks[len(blocks)-1-j] = blocks[len(blocks)-1-j], blocks[j]
			}
			if _, err = bc.InsertChain(context.Background(), blocks); err != nil {
				t.Fatalf("failed to import contra-fork chain for expansion: %v", err)
			}
			blocks, _, err = GenerateChain(&proConf, conBc.CurrentBlock(), ethash.NewFaker(), conDb, 1, func(i int, gen *BlockGen) {}, false /* intermediateHashes */)
			if err != nil {
				t.Fatalf("generate blocks: %v", err)
			}
			if _, err = conBc.InsertChain(context.Background(), blocks); err == nil {
				t.Fatalf("contra-fork chain accepted pro-fork block: %v", blocks[0])
			}
			// Create a proper no-fork block for the contra-forker
			blocks, _, err = GenerateChain(&conConf, conBc.CurrentBlock(), ethash.NewFaker(), conDb, 1, func(i int, gen *BlockGen) {}, false /* intermediateHashes */)
			if err != nil {
				t.Fatalf("generate blocks: %v", err)
			}
			if _, err = conBc.InsertChain(context.Background(), blocks); err != nil {
				t.Fatalf("contra-fork chain didn't accepted no-fork block: %v", err)
			}
			db.Close()
			// Create a no-fork block, and try to feed into the pro-fork chain
			db = ethdb.NewMemDatabase()
			defer db.Close()

			gspec.MustCommit(db)
			txCacherSubTest = NewTxSenderCacher(runtime.NumCPU())
			bc, _ = NewBlockChain(db, nil, &proConf, ethash.NewFaker(), vm.Config{}, nil, txCacherSubTest)
			defer bc.Stop()

			blocks = proBc.GetBlocksFromHash(proBc.CurrentBlock().Hash(), int(proBc.CurrentBlock().NumberU64()))
			for j := 0; j < len(blocks)/2; j++ {
				blocks[j], blocks[len(blocks)-1-j] = blocks[len(blocks)-1-j], blocks[j]
			}
			if _, err = bc.InsertChain(context.Background(), blocks); err != nil {
				t.Fatalf("failed to import pro-fork chain for expansion: %v", err)
			}
			blocks, _, err = GenerateChain(&conConf, proBc.CurrentBlock(), ethash.NewFaker(), proDb, 1, func(i int, gen *BlockGen) {}, false /* intermediateHashes */)
			if err != nil {
				t.Fatalf("generate blocks: %v", err)
			}
			if _, err = proBc.InsertChain(context.Background(), blocks); err == nil {
				t.Fatalf("pro-fork chain accepted contra-fork block: %v", blocks[0])
			}
			// Create a proper pro-fork block for the pro-forker
			blocks, _, err = GenerateChain(&proConf, proBc.CurrentBlock(), ethash.NewFaker(), proDb, 1, func(i int, gen *BlockGen) {}, false /* intermediateHashes */)
			if err != nil {
				t.Fatalf("generate blocks: %v", err)
			}
			if _, err = proBc.InsertChain(context.Background(), blocks); err != nil {
				t.Fatalf("pro-fork chain didn't accepted pro-fork block: %v", err)
			}
		})
	}
	// Verify that contra-forkers accept pro-fork extra-datas after forking finishes
	db = ethdb.NewMemDatabase()
	defer db.Close()
	gspec.MustCommit(db)
	txCacher1 := NewTxSenderCacher(runtime.NumCPU())
	bc, _ := NewBlockChain(db, nil, &conConf, ethash.NewFaker(), vm.Config{}, nil, txCacher1)
	defer bc.Stop()

	blocks := conBc.GetBlocksFromHash(conBc.CurrentBlock().Hash(), int(conBc.CurrentBlock().NumberU64()))
	for j := 0; j < len(blocks)/2; j++ {
		blocks[j], blocks[len(blocks)-1-j] = blocks[len(blocks)-1-j], blocks[j]
	}
	if _, err = bc.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("failed to import contra-fork chain for expansion: %v", err)
	}
	blocks, _, err = GenerateChain(&proConf, conBc.CurrentBlock(), ethash.NewFaker(), conDb, 1, func(i int, gen *BlockGen) {}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	if _, err = conBc.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("contra-fork chain didn't accept pro-fork block post-fork: %v", err)
	}
	// Verify that pro-forkers accept contra-fork extra-datas after forking finishes
	db = ethdb.NewMemDatabase()
	defer db.Close()
	gspec.MustCommit(db)
	txCacher = NewTxSenderCacher(runtime.NumCPU())
	bc, _ = NewBlockChain(db, nil, &proConf, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer bc.Stop()

	blocks = proBc.GetBlocksFromHash(proBc.CurrentBlock().Hash(), int(proBc.CurrentBlock().NumberU64()))
	for j := 0; j < len(blocks)/2; j++ {
		blocks[j], blocks[len(blocks)-1-j] = blocks[len(blocks)-1-j], blocks[j]
	}
	if _, err = bc.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("failed to import pro-fork chain for expansion: %v", err)
	}
	blocks, _, err = GenerateChain(&conConf, proBc.CurrentBlock(), ethash.NewFaker(), proDb, 1, func(i int, gen *BlockGen) {}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	if _, err = proBc.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("pro-fork chain didn't accept contra-fork block post-fork: %v", err)
	}
}
