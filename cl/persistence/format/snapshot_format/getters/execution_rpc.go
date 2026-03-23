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

package getters

import (
	"context"
	"fmt"

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

// ExecutionEngineReader implements ExecutionBlockReaderByNumber by fetching
// block bodies from the execution engine (via Engine API or direct access).
// Used in standalone caplin mode where local EL snapshot access is unavailable.
type ExecutionEngineReader struct {
	ctx       context.Context
	engine    ExecutionEngineBodyFetcher
	beaconCfg *clparams.BeaconChainConfig
}

func NewExecutionEngineReader(ctx context.Context, engine ExecutionEngineBodyFetcher) *ExecutionEngineReader {
	return &ExecutionEngineReader{ctx: ctx, engine: engine}
}

func (r *ExecutionEngineReader) SetBeaconChainConfig(beaconCfg *clparams.BeaconChainConfig) {
	r.beaconCfg = beaconCfg
}

func (r *ExecutionEngineReader) Transactions(number uint64, hash common.Hash) (*solid.TransactionsSSZ, error) {
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
	bodies, err := r.engine.GetBodiesByRange(r.ctx, number, 1)
	if err != nil {
		return nil, fmt.Errorf("GetBodiesByRange(%d): %w", number, err)
	}
	maxWithdrawals := 16
	if r.beaconCfg != nil {
		maxWithdrawals = int(r.beaconCfg.MaxWithdrawalsPerPayload)
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
