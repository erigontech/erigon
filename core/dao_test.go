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

package core_test

import (
	"context"
	"math/big"
	"runtime"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/stretchr/testify/require"
)

// Tests that DAO-fork enabled clients can properly filter out fork-commencing
// blocks based on their extradata fields.
func TestDAOForkRangeExtradata(t *testing.T) {
	t.Skip("remove blockchain or remove test")
	forkBlock := big.NewInt(32)

	// Generate a common prefix for both pro-forkers and non-forkers
	db := ethdb.NewMemDatabase()
	defer db.Close()
	gspec := &core.Genesis{Config: params.TestChainConfig}
	genesis := gspec.MustCommit(db)

	proDb := ethdb.NewMemDatabase()
	defer proDb.Close()
	gspec.MustCommit(proDb)

	// Create the concurrent, conflicting two nodes
	proConf := *params.TestChainConfig
	proConf.DAOForkBlock = forkBlock
	proConf.DAOForkSupport = true
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	proBc, _ := core.NewBlockChain(proDb, nil, &proConf, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer proBc.Stop()

	prefix, _, err := core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db, int(forkBlock.Int64()-1), func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
	require.NoError(t, err)

	conDb := ethdb.NewMemDatabase()
	defer conDb.Close()
	gspec.MustCommit(conDb)

	conConf := *params.TestChainConfig
	conConf.DAOForkBlock = forkBlock
	conConf.DAOForkSupport = false

	txCacherConBc := core.NewTxSenderCacher(runtime.NumCPU())
	conBc, _ := core.NewBlockChain(conDb, nil, &conConf, ethash.NewFaker(), vm.Config{}, nil, txCacherConBc)
	defer conBc.Stop()

	_, err = proBc.InsertChain(context.Background(), prefix)
	require.NoError(t, err)
	_, err = conBc.InsertChain(context.Background(), prefix)
	require.NoError(t, err)

	// Try to expand both pro-fork and non-fork chains iteratively with other camp's blocks
	for i := int64(0); i < params.DAOForkExtraRange.Int64(); i++ {
		t.Run(strconv.Itoa(int(i)), func(t *testing.T) {
			// Create a pro-fork block, and try to feed into the no-fork chain
			db = ethdb.NewMemDatabase()
			defer db.Close()
			gspec.MustCommit(db)
			blocks, err := rawdb.ReadBlocksByHash(conDb, rawdb.ReadCurrentHeader(conDb).Hash(), int(rawdb.ReadCurrentHeader(conDb).Number.Uint64()))
			require.NoError(t, err)
			for j := 0; j < len(blocks)/2; j++ {
				blocks[j], blocks[len(blocks)-1-j] = blocks[len(blocks)-1-j], blocks[j]
			}
			_, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, &conConf, &vm.Config{}, ethash.NewFaker(), blocks, true /* checkRoot */)
			require.NoError(t, err)
			blocks, _, err = core.GenerateChain(&proConf, rawdb.ReadCurrentBlock(conDb), ethash.NewFaker(), conDb, 1, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
			require.NoError(t, err)
			if _, err = conBc.InsertChain(context.Background(), blocks); err == nil {
				t.Fatalf("contra-fork chain accepted pro-fork block: %v", blocks[0])
			}
			// Create a proper no-fork block for the contra-forker
			blocks, _, err = core.GenerateChain(&conConf, rawdb.ReadCurrentBlock(conDb), ethash.NewFaker(), conDb, 1, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
			require.NoError(t, err)
			if _, err = conBc.InsertChain(context.Background(), blocks); err != nil {
				t.Fatalf("contra-fork chain didn't accepted no-fork block: %v", err)
			}
			db.Close()
			// Create a no-fork block, and try to feed into the pro-fork chain
			db = ethdb.NewMemDatabase()
			defer db.Close()

			gspec.MustCommit(db)

			blocks, err = rawdb.ReadBlocksByHash(proDb, rawdb.ReadCurrentHeader(proDb).Hash(), int(rawdb.ReadCurrentHeader(proDb).Number.Uint64()))
			require.NoError(t, err)
			for j := 0; j < len(blocks)/2; j++ {
				blocks[j], blocks[len(blocks)-1-j] = blocks[len(blocks)-1-j], blocks[j]
			}
			_, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, &proConf, &vm.Config{}, ethash.NewFaker(), blocks, true /* checkRoot */)
			require.NoError(t, err)
			blocks, _, err = core.GenerateChain(&conConf, rawdb.ReadCurrentBlock(proDb), ethash.NewFaker(), proDb, 1, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
			require.NoError(t, err)
			if _, err = proBc.InsertChain(context.Background(), blocks); err == nil {
				t.Fatalf("pro-fork chain accepted contra-fork block: %v", blocks[0])
			}
			// Create a proper pro-fork block for the pro-forker
			blocks, _, err = core.GenerateChain(&proConf, rawdb.ReadCurrentBlock(proDb), ethash.NewFaker(), proDb, 1, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
			require.NoError(t, err)
			if _, err = proBc.InsertChain(context.Background(), blocks); err != nil {
				t.Fatalf("pro-fork chain didn't accepted pro-fork block: %v", err)
			}
		})
	}
	// Verify that contra-forkers accept pro-fork extra-datas after forking finishes
	db = ethdb.NewMemDatabase()
	defer db.Close()
	gspec.MustCommit(db)

	blocks, err := rawdb.ReadBlocksByHash(conDb, rawdb.ReadCurrentHeader(conDb).Hash(), int(rawdb.ReadCurrentHeader(conDb).Number.Uint64()))
	require.NoError(t, err)
	for j := 0; j < len(blocks)/2; j++ {
		blocks[j], blocks[len(blocks)-1-j] = blocks[len(blocks)-1-j], blocks[j]
	}
	_, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, &conConf, &vm.Config{}, ethash.NewFaker(), blocks, true /* checkRoot */)
	require.NoError(t, err)
	blocks, _, err = core.GenerateChain(&proConf, conBc.CurrentBlock(), ethash.NewFaker(), conDb, 1, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
	require.NoError(t, err)
	if _, err = conBc.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("contra-fork chain didn't accept pro-fork block post-fork: %v", err)
	}
	// Verify that pro-forkers accept contra-fork extra-datas after forking finishes
	db = ethdb.NewMemDatabase()
	defer db.Close()
	gspec.MustCommit(db)

	blocks, err = rawdb.ReadBlocksByHash(proDb, rawdb.ReadCurrentHeader(proDb).Hash(), int(rawdb.ReadCurrentHeader(proDb).Number.Uint64()))
	require.NoError(t, err)
	for j := 0; j < len(blocks)/2; j++ {
		blocks[j], blocks[len(blocks)-1-j] = blocks[len(blocks)-1-j], blocks[j]
	}
	_, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, &proConf, &vm.Config{}, ethash.NewFaker(), blocks, true /* checkRoot */)
	require.NoError(t, err)
	blocks, _, err = core.GenerateChain(&conConf, rawdb.ReadCurrentBlock(proDb), ethash.NewFaker(), proDb, 1, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
	require.NoError(t, err)
	if _, err = proBc.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("pro-fork chain didn't accept contra-fork block post-fork: %v", err)
	}
}
