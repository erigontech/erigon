// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package ethash implements the ethash proof-of-work rules engine.
package ethash

import (
	"math/big"
	"math/rand"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash/ethashcfg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
)

var (
	// two256 is a big integer representing 2^256
	two256 = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), big.NewInt(0))

	// sharedEthash is a full instance that can be shared between multiple users.
	sharedEthashOnce sync.Once
	sharedEthash     *Ethash
)

// GetSharedEthash returns a process-wide shared Ethash instance.
func GetSharedEthash() *Ethash {
	sharedEthashOnce.Do(func() {
		sharedEthash = New(ethashcfg.Config{PowMode: ethashcfg.ModeNormal}, false)
	})
	return sharedEthash
}

// Ethash is a rules engine based on proof-of-work implementing the ethash algorithm.
type Ethash struct {
	config ethashcfg.Config

	// Mining related fields
	rand     *rand.Rand     // Properly seeded random source for nonces
	hashrate *hashRateMeter // Meter tracking the average hashrate
	remote   *remoteSealer

	// The fields below are hooks for testing
	shared *Ethash // Shared PoW verifier to avoid cache regeneration

	lock      sync.Mutex // Ensures thread safety for the mining fields
	closeOnce sync.Once  // Ensures exit channel will not be closed twice.
}

// New creates a full sized ethash PoW scheme and starts a background thread for
// remote mining, also optionally notifying a batch of remote services of new work
// packages.
func New(config ethashcfg.Config, noverify bool) *Ethash {
	if config.Log == nil {
		config.Log = log.Root()
	}
	ethash := &Ethash{
		config:   config,
		hashrate: newHashRateMeter(),
	}
	if config.PowMode == ethashcfg.ModeShared {
		ethash.shared = GetSharedEthash()
	}
	ethash.remote = startRemoteSealer(ethash, noverify)
	return ethash
}

// NewTester creates a small sized ethash PoW scheme useful only for testing
// purposes.
func NewTester(noverify bool) *Ethash {
	return New(ethashcfg.Config{PowMode: ethashcfg.ModeTest}, noverify)
}

// NewShared creates a full sized ethash PoW shared between all requesters running
// in the same process.
func NewShared() *Ethash {
	return &Ethash{shared: GetSharedEthash()}
}

// Close closes the exit channel to notify all backend threads exiting.
func (ethash *Ethash) Close() error {
	ethash.closeOnce.Do(func() {
		// Short circuit if the exit channel is not allocated.
		if ethash.remote == nil {
			return
		}
		close(ethash.remote.requestExit)
		<-ethash.remote.exitCh
	})
	return nil
}

// Hashrate implements PoW, returning the measured rate of the search invocations
// per second over the last minute.
// Note the returned hashrate includes local hashrate, but also includes the total
// hashrate of all remote miner.
func (ethash *Ethash) Hashrate() float64 {
	// Short circuit if we are run the ethash in normal/test mode.
	if (ethash.config.PowMode != ethashcfg.ModeNormal && ethash.config.PowMode != ethashcfg.ModeTest) || ethash.remote == nil {
		return ethash.hashrate.Rate()
	}
	var res = make(chan uint64, 1)

	select {
	case ethash.remote.fetchRateCh <- res:
	case <-ethash.remote.exitCh:
		// Return local hashrate only if ethash is stopped.
		return ethash.hashrate.Rate()
	}

	// Gather total submitted hash rate of remote sealers.
	return ethash.hashrate.Rate() + float64(<-res)
}

// APIs implements rules.Engine, returning the user facing RPC APIs.
func (ethash *Ethash) APIs(chain rules.ChainHeaderReader) []rpc.API {
	// In order to ensure backward compatibility, we exposes ethash RPC APIs
	// to both eth and ethash namespaces.
	return []rpc.API{
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   &API{ethash},
			Public:    true,
		},
		{
			Namespace: "ethash",
			Version:   "1.0",
			Service:   &API{ethash},
			Public:    true,
		},
	}
}

// SeedHash is the seed to use for generating a verification cache and the mining
// dataset.
func SeedHash(block uint64) []byte {
	return seedHash(block)
}

func (ethash *Ethash) GetTransferFunc() evmtypes.TransferFunc {
	return misc.Transfer
}

func (ethash *Ethash) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

func (c *Ethash) TxDependencies(h *types.Header) [][]int {
	return nil
}
