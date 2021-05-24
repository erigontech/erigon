// Copyright 2015 The go-ethereum Authors
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

package eth

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/mock"
)

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)
)

// testHandler is a live implementation of the Ethereum protocol handler, just
// preinitialized with some sane testing defaults and the transaction pool mocked
// out.
type testHandler struct {
	db          ethdb.Database
	ChainConfig *params.ChainConfig
	vmConfig    *vm.Config
	genesis     *types.Block
	engine      consensus.Engine
	txpool      *mock.TestTxPool
	handler     *handler
	headBlock   *types.Block
}

// newTestHandler creates a new handler for testing purposes with no blocks.
func newTestHandler(t *testing.T) *testHandler {
	return newTestHandlerWithBlocks(t, 0)
}

// newTestHandlerWithBlocks creates a new handler for testing purposes, with a
// given number of initial blocks.
func newTestHandlerWithBlocks(t *testing.T, blocks int) *testHandler {
	// Create a database pre-initialize with a genesis block
	db := ethdb.NewTestDB(t)
	genesis := (&core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{testAddr: {Balance: big.NewInt(1000000)}},
	}).MustCommit(db)

	headBlock := genesis
	if blocks > 0 {
		chain, _ := core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db.RwKV(), blocks, nil, false)
		if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true /* checkRoot */); err != nil {
			panic(err)
		}
		headBlock = chain.TopBlock
	}
	txpool := mock.NewTestTxPool()

	handler, _ := newHandler(&handlerConfig{
		Database:    db,
		ChainConfig: params.TestChainConfig,
		genesis:     genesis,
		vmConfig:    &vm.Config{},
		engine:      ethash.NewFaker(),
		TxPool:      txpool,
		Network:     1,
		BloomCache:  1,
	})
	handler.Start(1000)

	b := &testHandler{
		db:          db,
		ChainConfig: params.TestChainConfig,
		genesis:     genesis,
		vmConfig:    &vm.Config{},
		engine:      ethash.NewFaker(),
		txpool:      txpool,
		handler:     handler,
		headBlock:   headBlock,
	}
	t.Cleanup(func() {
		b.handler.Stop()
	})
	return b
}
