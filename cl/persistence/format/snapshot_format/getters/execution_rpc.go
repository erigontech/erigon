// Copyright 2026 The Erigon Authors
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

package getters

import (
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// ExecutionEngineBodyFetcher fetches raw block bodies. Matches a subset of
// execution_client.ExecutionEngine to avoid an import cycle.
type ExecutionEngineBodyFetcher interface {
	GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error)
}

// cachedBody stores a recently produced block body for immediate retrieval.
type cachedBody struct {
	transactions [][]byte
	withdrawals  []*types.Withdrawal
}

// ExecutionEngineReader implements ExecutionBlockReaderByNumber by fetching
// block bodies from the execution engine (via Engine API or direct access).
// It includes a small in-memory cache for recently produced blocks to handle
// the race between beacon block writes and EL block commits.
type ExecutionEngineReader struct {
	ctx       context.Context
	engine    ExecutionEngineBodyFetcher
	beaconCfg *clparams.BeaconChainConfig

	mu         sync.RWMutex
	cache      map[common.Hash]*cachedBody
	cacheOrder []common.Hash
}

func NewExecutionEngineReader(ctx context.Context, engine ExecutionEngineBodyFetcher) *ExecutionEngineReader {
	return &ExecutionEngineReader{
		ctx:    ctx,
		engine: engine,
		cache:  make(map[common.Hash]*cachedBody),
	}
}

func (r *ExecutionEngineReader) SetBeaconChainConfig(beaconCfg *clparams.BeaconChainConfig) {
	r.beaconCfg = beaconCfg
}

// CacheBody stores a block body for immediate retrieval. Call this when a
// block is produced, before the EL has committed the data to its DB.
func (r *ExecutionEngineReader) CacheBody(number uint64, hash common.Hash, transactions [][]byte, withdrawals []*types.Withdrawal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.cache[hash]; !ok {
		r.cacheOrder = append(r.cacheOrder, hash)
	}
	r.cache[hash] = &cachedBody{transactions: transactions, withdrawals: withdrawals}
	if len(r.cacheOrder) > 64 {
		delete(r.cache, r.cacheOrder[0])
		copy(r.cacheOrder, r.cacheOrder[1:])
		r.cacheOrder = r.cacheOrder[:64]
	}
}

func (r *ExecutionEngineReader) getCached(hash common.Hash) *cachedBody {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cache[hash]
}

func (r *ExecutionEngineReader) body(number uint64, hash common.Hash) (*cachedBody, error) {
	if cached := r.getCached(hash); cached != nil {
		return cached, nil
	}
	if r.engine == nil {
		return nil, fmt.Errorf("%w for block %d (%x)", snapshot_format.ErrExecutionBodyNotFound, number, hash)
	}
	bodies, err := r.engine.GetBodiesByHashes(r.ctx, []common.Hash{hash})
	if err != nil {
		return nil, fmt.Errorf("GetBodiesByHashes(%x): %w", hash, err)
	}
	if len(bodies) == 0 || bodies[0] == nil {
		return nil, fmt.Errorf("%w for block %d (%x)", snapshot_format.ErrExecutionBodyNotFound, number, hash)
	}
	return &cachedBody{transactions: bodies[0].Transactions, withdrawals: bodies[0].Withdrawals}, nil
}

func (r *ExecutionEngineReader) Transactions(number uint64, hash common.Hash) (*solid.TransactionsSSZ, error) {
	body, err := r.body(number, hash)
	if err != nil {
		return nil, err
	}
	return solid.NewTransactionsSSZFromTransactions(body.transactions), nil
}

func (r *ExecutionEngineReader) Withdrawals(number uint64, hash common.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error) {
	maxWithdrawals := 16
	if r.beaconCfg != nil {
		maxWithdrawals = int(r.beaconCfg.MaxWithdrawalsPerPayload)
	}
	body, err := r.body(number, hash)
	if err != nil {
		return nil, err
	}
	ret := solid.NewStaticListSSZ[*cltypes.Withdrawal](maxWithdrawals, 44)
	for _, w := range body.withdrawals {
		if w == nil {
			continue
		}
		ret.Append(&cltypes.Withdrawal{
			Index:     uint64(w.Index),
			Validator: uint64(w.Validator),
			Address:   w.Address,
			Amount:    uint64(w.Amount),
		})
	}
	return ret, nil
}
