// Copyright 2020 The go-ethereum Authors
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

// Package miner implements Ethereum block creation and mining.
package miner

import (
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/params"
)

type mockBackend struct {
	bc     *core.BlockChain
	txPool *core.TxPool
}

func NewMockBackend(bc *core.BlockChain, txPool *core.TxPool) *mockBackend {
	return &mockBackend{
		bc:     bc,
		txPool: txPool,
	}
}

func (m *mockBackend) BlockChain() *core.BlockChain {
	return m.bc
}

func (m *mockBackend) TxPool() *core.TxPool {
	return m.txPool
}

type testBlockChain struct {
	gasLimit      uint64
	chainHeadFeed *event.Feed
}

func (bc *testBlockChain) CurrentBlock() *types.Block {
	return types.NewBlock(&types.Header{
		GasLimit: bc.gasLimit,
	}, nil, nil, nil)
}

func (bc *testBlockChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	return bc.CurrentBlock()
}

func (bc *testBlockChain) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return bc.chainHeadFeed.Subscribe(ch)
}

func TestMiner(t *testing.T) {
	t.Skip("skipped for turbo-geth")
	miner, mux := createMiner(t)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Start the downloader
	mux.Post(downloader.StartEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, false)
	// Stop the downloader and wait for the update loop to run
	mux.Post(downloader.DoneEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, true)
	// Start the downloader and wait for the update loop to run
	mux.Post(downloader.StartEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, false)
	// Stop the downloader and wait for the update loop to run
	mux.Post(downloader.FailedEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, true)
}

func TestStartWhileDownload(t *testing.T) {
	t.Skip("skipped for turbo-geth")
	miner, mux := createMiner(t)
	waitForMiningState(t, miner, false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Stop the downloader and wait for the update loop to run
	mux.Post(downloader.StartEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, false)
	// Starting the miner after the downloader should not work
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, false)
}

func TestStartStopMiner(t *testing.T) {
	t.Skip("skipped for turbo-geth")
	miner, _ := createMiner(t)
	waitForMiningState(t, miner, false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	miner.Stop()
	waitForMiningState(t, miner, false)
}

func TestCloseMiner(t *testing.T) {
	t.Skip("skipped for turbo-geth")
	miner, _ := createMiner(t)
	waitForMiningState(t, miner, false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Terminate the miner and wait for the update loop to run
	miner.Close()
	waitForMiningState(t, miner, false)
}

// waitForMiningState waits until either
// * the desired mining state was reached
// * a timeout was reached which fails the test
func waitForMiningState(t *testing.T, m *Miner, mining bool) {
	t.Helper()

	var state bool
	for i := 0; i < 100; i++ {
		if state = m.Mining(); state == mining {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Mining() == %t, want %t", state, mining)
}

func createMiner(t *testing.T) (*Miner, *event.TypeMux) {
	// Create Ethash config
	config := Config{
		Etherbase: common.HexToAddress("123456789"),
	}
	// Create chainConfig
	chainDB := ethdb.NewMemDatabase()
	genesis := core.DeveloperGenesisBlock(15, common.HexToAddress("12345"))
	chainConfig, _, _, err := core.SetupGenesisBlock(chainDB, genesis, false, false)
	if err != nil {
		t.Fatalf("can't create new chain config: %v", err)
	}
	// Create event Mux
	mux := new(event.TypeMux)
	// Create consensus engine
	engine := ethash.New(ethash.Config{}, []string{}, false)
	engine.SetThreads(-1)
	// Create isLocalBlock
	isLocalBlock := func(block *types.Block) bool {
		return true
	}
	// Create Ethereum backend
	bc, err := core.NewBlockChain(chainDB, new(core.CacheConfig), chainConfig, engine, vm.Config{}, isLocalBlock, nil)
	if err != nil {
		t.Fatalf("can't create new chain %v", err)
	}
	pool := core.NewTxPool(testTxPoolConfig, params.TestChainConfig, ethdb.NewMemDatabase(), nil)
	backend := NewMockBackend(bc, pool)
	// Create Miner
	return New(backend, &config, chainConfig, mux, engine, isLocalBlock), mux
}
