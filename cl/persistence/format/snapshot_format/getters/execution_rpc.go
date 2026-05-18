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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// ExecutionEngineBodyFetcher fetches raw block bodies. Matches a subset of
// execution_client.ExecutionEngine to avoid an import cycle.
type ExecutionEngineBodyFetcher interface {
	GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error)
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

	mu            sync.RWMutex
	cache         map[uint64]*cachedBody // block number -> body
	highestCached uint64
}

func NewExecutionEngineReader(ctx context.Context, engine ExecutionEngineBodyFetcher) *ExecutionEngineReader {
	return &ExecutionEngineReader{
		ctx:    ctx,
		engine: engine,
		cache:  make(map[uint64]*cachedBody),
	}
}

func (r *ExecutionEngineReader) SetBeaconChainConfig(beaconCfg *clparams.BeaconChainConfig) {
	r.beaconCfg = beaconCfg
}

// CacheBody stores a block body for immediate retrieval. Call this when a
// block is produced, before the EL has committed the data to its DB.
func (r *ExecutionEngineReader) CacheBody(number uint64, transactions [][]byte, withdrawals []*types.Withdrawal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[number] = &cachedBody{transactions: transactions, withdrawals: withdrawals}
	if number > r.highestCached {
		r.highestCached = number
	}
	// Evict old entries (keep last 64 blocks)
	if len(r.cache) > 64 {
		threshold := r.highestCached - 64
		for k := range r.cache {
			if k < threshold {
				delete(r.cache, k)
			}
		}
	}
}

func (r *ExecutionEngineReader) getCached(number uint64) *cachedBody {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cache[number]
}

func (r *ExecutionEngineReader) Transactions(number uint64, hash common.Hash) (*solid.TransactionsSSZ, error) {
	// Check cache first (handles race between beacon write and EL commit).
	if cached := r.getCached(number); cached != nil {
		return solid.NewTransactionsSSZFromTransactions(cached.transactions), nil
	}
	if r.engine == nil {
		return solid.NewTransactionsSSZFromTransactions(nil), nil
	}
	bodies, err := r.engine.GetBodiesByRange(r.ctx, number, 1)
	if err != nil {
		return nil, fmt.Errorf("GetBodiesByRange(%d): %w", number, err)
	}
	if len(bodies) == 0 || bodies[0] == nil {
		return solid.NewTransactionsSSZFromTransactions(nil), nil
	}
	return solid.NewTransactionsSSZFromTransactions(bodies[0].Transactions), nil
}

func (r *ExecutionEngineReader) Withdrawals(number uint64, hash common.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error) {
	maxWithdrawals := 16
	if r.beaconCfg != nil {
		maxWithdrawals = int(r.beaconCfg.MaxWithdrawalsPerPayload)
	}
	// Check cache first (handles race between beacon write and EL commit).
	if cached := r.getCached(number); cached != nil {
		ret := solid.NewStaticListSSZ[*cltypes.Withdrawal](maxWithdrawals, 44)
		for _, w := range cached.withdrawals {
			if w == nil {
				continue
			}
			ret.Append(&cltypes.Withdrawal{
				Index:     w.Index,
				Validator: w.Validator,
				Address:   w.Address,
				Amount:    w.Amount,
			})
		}
		return ret, nil
	}
	if r.engine == nil {
		return solid.NewStaticListSSZ[*cltypes.Withdrawal](maxWithdrawals, 44), nil
	}
	bodies, err := r.engine.GetBodiesByRange(r.ctx, number, 1)
	if err != nil {
		return nil, fmt.Errorf("GetBodiesByRange(%d): %w", number, err)
	}
	ret := solid.NewStaticListSSZ[*cltypes.Withdrawal](maxWithdrawals, 44)
	if len(bodies) == 0 || bodies[0] == nil {
		return ret, nil
	}
	for _, w := range bodies[0].Withdrawals {
		if w == nil {
			continue
		}
		ret.Append(&cltypes.Withdrawal{
			Index:     w.Index,
			Validator: w.Validator,
			Address:   w.Address,
			Amount:    w.Amount,
		})
	}
	return ret, nil
}
