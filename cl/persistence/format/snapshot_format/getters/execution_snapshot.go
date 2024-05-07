package getters

import (
	"context"
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type ExecutionSnapshotReader struct {
	ctx context.Context

	blockReader services.FullBlockReader
	beaconCfg   *clparams.BeaconChainConfig

	db kv.RoDB
}

func NewExecutionSnapshotReader(ctx context.Context, beaconCfg *clparams.BeaconChainConfig, blockReader services.FullBlockReader, db kv.RoDB) *ExecutionSnapshotReader {
	return &ExecutionSnapshotReader{ctx: ctx, beaconCfg: beaconCfg, blockReader: blockReader, db: db}
}

func (r *ExecutionSnapshotReader) Transactions(number uint64, hash libcommon.Hash) (*solid.TransactionsSSZ, error) {
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
	for _, tx := range txs {
		sumLenTxs += len(tx)
	}
	flat := make([]byte, 0, 4*len(txs)+sumLenTxs)
	offset := len(txs) * 4
	for _, tx := range txs {
		flat = append(flat, ssz.OffsetSSZ(uint32(offset))...)
		offset += len(tx)
	}
	for _, tx := range txs {
		flat = append(flat, tx...)
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

func (r *ExecutionSnapshotReader) Withdrawals(number uint64, hash libcommon.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error) {
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
