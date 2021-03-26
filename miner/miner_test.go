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
	"github.com/ledgerwatch/turbo-geth/consensus/clique"
	"github.com/ledgerwatch/turbo-geth/core"
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

// TestMinerDownloaderFirstFails tests that mining is only
// permitted to run indefinitely once the downloader sees a DoneEvent (success).
// An initial FailedEvent should allow mining to stop on a subsequent
// downloader StartEvent.
func TestMinerDownloaderFirstFails(t *testing.T) {
	miner, mux := createMiner(t)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Start the downloader
	mux.Post(downloader.StartEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, false)

	// Stop the downloader and wait for the update loop to run
	mux.Post(downloader.FailedEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, true)

	// Since the downloader hasn't yet emitted a successful DoneEvent,
	// we expect the miner to stop on next StartEvent.
	mux.Post(downloader.StartEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, false)

	// Downloader finally succeeds.
	mux.Post(downloader.DoneEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, true)

	// Downloader starts again.
	// Since it has achieved a DoneEvent once, we expect miner
	// state to be unchanged.
	mux.Post(downloader.StartEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, true)

	mux.Post(downloader.FailedEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, true)
}

func TestMinerStartStopAfterDownloaderEvents(t *testing.T) {
	miner, mux := createMiner(t)

	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Start the downloader
	mux.Post(downloader.StartEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, false)

	// Downloader finally succeeds.
	mux.Post(downloader.DoneEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, true)

	miner.Stop()
	waitForMiningState(t, miner, false)

	miner.Start(common.HexToAddress("0x678910"))
	waitForMiningState(t, miner, true)

	miner.Stop()
	waitForMiningState(t, miner, false)
}

// TestMinerSetEtherbase checks that etherbase becomes set even if mining isn't
// possible at the moment
func TestMinerSetEtherbase(t *testing.T) {
	miner, mux := createMiner(t)
	// Start with a 'bad' mining address
	miner.Start(common.HexToAddress("0xdead"))
	waitForMiningState(t, miner, true)
	// Start the downloader
	mux.Post(downloader.StartEvent{}) //nolint:errcheck
	waitForMiningState(t, miner, false)
	// Now user tries to configure proper mining address
	miner.Start(common.HexToAddress("0x1337"))
	// Stop the downloader and wait for the update loop to run
	mux.Post(downloader.DoneEvent{}) //nolint:errcheck

	waitForMiningState(t, miner, true)
	// The miner should now be using the good address
	if got, exp := miner.coinbase, common.HexToAddress("0x1337"); got != exp {
		t.Fatalf("Wrong coinbase, got %x expected %x", got, exp)
	}
}

// waitForMiningState waits until either
// * the desired mining state was reached
// * a timeout was reached which fails the test
func waitForMiningState(t *testing.T, m *Miner, mining bool) {
	t.Helper()

	var state bool
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		if state = m.Mining(); state == mining {
			return
		}
	}
	t.Fatalf("Mining() == %t, want %t", state, mining)
}

func createMiner(t *testing.T) (*Miner, *event.TypeMux) {
	// Create Ethash config
	config := params.MiningConfig{
		Etherbase: common.HexToAddress("123456789"),
	}
	// Create chainConfig
	chainDB := ethdb.NewMemDatabase()
	genesis := core.DeveloperGenesisBlock(15, common.HexToAddress("12345"))
	chainConfig, _, err := core.SetupGenesisBlock(chainDB, genesis, false, false)
	if err != nil {
		t.Fatalf("can't create new chain config: %v", err)
	}
	// Create consensus engine
	engine := clique.New(chainConfig.Clique, chainDB)
	// Create Ethereum backend
	bc, err := core.NewBlockChain(chainDB, new(core.CacheConfig), chainConfig, engine, vm.Config{}, nil /*isLocalBlock*/, nil)
	if err != nil {
		t.Fatalf("can't create new chain %v", err)
	}
	cfg := core.DefaultTxPoolConfig
	cfg.Journal = ""
	pool := core.NewTxPool(cfg, params.TestChainConfig, ethdb.NewMemDatabase(), nil)
	backend := NewMockBackend(bc, pool)
	// Create event Mux
	mux := new(event.TypeMux)
	// Create Miner
	return New(backend, &config, chainConfig, mux, engine, nil), mux
}
