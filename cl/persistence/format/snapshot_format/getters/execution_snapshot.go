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
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
)

type ExecutionSnapshotReader struct {
	ctx context.Context

	blockReader dbservices.FullBlockReader
	beaconCfg   *clparams.BeaconChainConfig

	db kv.RoDB
}

func NewExecutionSnapshotReader(ctx context.Context, blockReader dbservices.FullBlockReader, db kv.RoDB) *ExecutionSnapshotReader {
	return &ExecutionSnapshotReader{ctx: ctx, blockReader: blockReader, db: db}
}

func (r *ExecutionSnapshotReader) SetBeaconChainConfig(beaconCfg *clparams.BeaconChainConfig) {
	r.beaconCfg = beaconCfg
}

func (r *ExecutionSnapshotReader) Transactions(number uint64, hash common.Hash) (*solid.TransactionsSSZ, error) {
	tx, err := r.db.BeginRo(r.ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Get the body and fill both caches
	body, err := r.blockReader.BodyWithTransactions(r.ctx, tx, hash, number)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("transactions not found for block %d", number)
	}
	// compute txs flats
	txs, err := types.MarshalTransactionsBinary(body.Transactions)
	if err != nil {
		return nil, err
	}

	return solid.NewTransactionsSSZFromTransactions(txs), nil
}

func (r *ExecutionSnapshotReader) Withdrawals(number uint64, hash common.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error) {
	tx, err := r.db.BeginRo(r.ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Get the body and fill both caches
	body, _, err := r.blockReader.Body(r.ctx, tx, hash, number)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("transactions not found for block %d", number)
	}
	ret := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(r.beaconCfg.MaxWithdrawalsPerPayload), 44)
	for _, w := range body.Withdrawals {
		ret.Append(&cltypes.Withdrawal{
			Index:     w.Index,
			Validator: w.Validator,
			Address:   w.Address,
			Amount:    w.Amount,
		})
	}
	return ret, nil
}

func (r *ExecutionSnapshotReader) CacheBody(blockNumber uint64, transactions [][]byte, withdrawals []*types.Withdrawal) {
	// No-op: local snapshot reader doesn't need caching — EL data is always available.
}
