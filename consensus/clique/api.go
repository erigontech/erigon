// Copyright 2017 The go-ethereum Authors
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

package clique

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

// API is a user facing RPC API to allow controlling the signer and voting
// mechanisms of the proof-of-authority scheme.
type API struct {
	// chain  consensus.ChainHeaderReader
	db          kv.RoDB
	clique      *Clique
	logger      log.Logger
	blockReader services.FullBlockReader
}

// GetSnapshot retrieves the state snapshot at a given block.
func (api *API) GetSnapshot(ctx context.Context, number *rpc.BlockNumber) (*Snapshot, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chain := consensus.ChainReaderImpl{Cfg: *api.clique.ChainConfig, Db: tx, BlockReader: api.blockReader}

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = chain.CurrentHeader()
	} else {
		header = chain.GetHeaderByNumber(uint64(number.Int64()))
	}
	// Ensure we have an actually valid block and return its snapshot
	if header == nil {
		return nil, errUnknownBlock
	}

	snap, err := api.clique.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)
	if err != nil {
		return nil, err
	}

	return snap, err
}

// GetSnapshotAtHash retrieves the state snapshot at a given block.
func (api *API) GetSnapshotAtHash(ctx context.Context, hash libcommon.Hash) (*Snapshot, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chain := consensus.ChainReaderImpl{Cfg: *api.clique.ChainConfig, Db: tx, BlockReader: api.blockReader}

	header := chain.GetHeaderByHash(hash)
	if header == nil {
		return nil, errUnknownBlock
	}

	snap, err := api.clique.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)
	if err != nil {
		return nil, err
	}

	return snap, err
}

// GetSigners retrieves the list of authorized signers at the specified block.
func (api *API) GetSigners(ctx context.Context, number *rpc.BlockNumber) ([]libcommon.Address, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chain := consensus.ChainReaderImpl{Cfg: *api.clique.ChainConfig, Db: tx, BlockReader: api.blockReader}

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = chain.CurrentHeader()
	} else {
		header = chain.GetHeaderByNumber(uint64(number.Int64()))
	}
	// Ensure we have an actually valid block and return the signers from its snapshot
	if header == nil {
		return nil, errUnknownBlock
	}

	snap, err := api.clique.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)
	if err != nil {
		return nil, err
	}
	return snap.GetSigners(), nil
}

// GetSignersAtHash retrieves the list of authorized signers at the specified block.
func (api *API) GetSignersAtHash(ctx context.Context, hash libcommon.Hash) ([]libcommon.Address, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chain := consensus.ChainReaderImpl{Cfg: *api.clique.ChainConfig, Db: tx, BlockReader: api.blockReader}

	header := chain.GetHeaderByHash(hash)
	if header == nil {
		return nil, errUnknownBlock
	}
	snap, err := api.clique.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)
	if err != nil {
		return nil, err
	}
	return snap.GetSigners(), nil
}

// Proposals returns the current proposals the node tries to uphold and vote on.
func (api *API) Proposals() map[libcommon.Address]bool {
	api.clique.lock.RLock()
	defer api.clique.lock.RUnlock()

	proposals := make(map[libcommon.Address]bool)
	for address, auth := range api.clique.proposals {
		proposals[address] = auth
	}
	return proposals
}

// Propose injects a new authorization proposal that the signer will attempt to
// push through.
func (api *API) Propose(address libcommon.Address, auth bool) {
	api.clique.lock.Lock()
	defer api.clique.lock.Unlock()

	api.clique.proposals[address] = auth
}

// Discard drops a currently running proposal, stopping the signer from casting
// further votes (either for or against).
func (api *API) Discard(address libcommon.Address) {
	api.clique.lock.Lock()
	defer api.clique.lock.Unlock()

	delete(api.clique.proposals, address)
}

type status struct {
	InturnPercent float64                   `json:"inturnPercent"`
	SigningStatus map[libcommon.Address]int `json:"sealerActivity"`
	NumBlocks     uint64                    `json:"numBlocks"`
}

// Status returns the status of the last N blocks,
// - the number of active signers,
// - the number of signers,
// - the percentage of in-turn blocks
func (api *API) Status(ctx context.Context) (*status, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chain := consensus.ChainReaderImpl{Cfg: *api.clique.ChainConfig, Db: tx, BlockReader: api.blockReader}

	var (
		numBlocks = uint64(64)
		header    = chain.CurrentHeader()
		diff      = uint64(0)
		optimals  = 0
	)
	snap, err := api.clique.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)
	if err != nil {
		return nil, err
	}
	var (
		signers = snap.GetSigners()
		end     = header.Number.Uint64()
		start   = end - numBlocks
	)
	if numBlocks > end {
		start = 1
		numBlocks = end - start
	}
	signStatus := make(map[libcommon.Address]int)
	for _, s := range signers {
		signStatus[s] = 0
	}
	for n := start; n < end; n++ {
		h := chain.GetHeaderByNumber(n)
		if h == nil {
			return nil, fmt.Errorf("missing block %d", n)
		}
		if h.Difficulty.Cmp(DiffInTurn) == 0 {
			optimals++
		}
		diff += h.Difficulty.Uint64()
		sealer, err := api.clique.Author(h)
		if err != nil {
			return nil, err
		}
		signStatus[sealer]++
	}
	return &status{
		InturnPercent: float64(100*optimals) / float64(numBlocks),
		SigningStatus: signStatus,
		NumBlocks:     numBlocks,
	}, nil
}
