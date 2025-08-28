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
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
)

type ExecutionSnapshotReader struct {
	ctx context.Context

	blockReader services.FullBlockReader
	beaconCfg   *clparams.BeaconChainConfig

	db kv.RoDB
}

func NewExecutionSnapshotReader(ctx context.Context, blockReader services.FullBlockReader, db kv.RoDB) *ExecutionSnapshotReader {
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

func convertTxsToBytesSSZ(txs [][]byte) []byte {
	sumLenTxs := 0
	for _, txn := range txs {
		sumLenTxs += len(txn)
	}
	flat := make([]byte, 0, 4*len(txs)+sumLenTxs)
	offset := len(txs) * 4
	for _, txn := range txs {
		flat = append(flat, ssz.OffsetSSZ(uint32(offset))...)
		offset += len(txn)
	}
	for _, txn := range txs {
		flat = append(flat, txn...)
	}
	return flat
}

func convertWithdrawalsToBytesSSZ(ws []*types.Withdrawal) []byte {
	ret := make([]byte, 44*len(ws))
	for i, w := range ws {
		currentPos := i * 44
		binary.LittleEndian.PutUint64(ret[currentPos:currentPos+8], w.Index)
		binary.LittleEndian.PutUint64(ret[currentPos+8:currentPos+16], w.Validator)
		copy(ret[currentPos+16:currentPos+36], w.Address[:])
		binary.LittleEndian.PutUint64(ret[currentPos+36:currentPos+44], w.Amount)
	}
	return ret
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
