package jsonrpc

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

// Defines the `internal_` JSON-RPC namespace.
//
// The methods defined here are for exposing internal Erigon info and meant to serve as development support if you are
// working on Erigon code. They can be added/changed/removed without further notice.
type InternalAPI interface {
	GetTxNumInfo(ctx context.Context, txNum uint64) (*TxNumInfo, error)
}

type TxNumInfo struct {
	BlockNum uint64 `json:"blockNum"`
	Idx      uint64 `json:"idx"`
}

type InternalAPIImpl struct {
	*BaseAPI
	db kv.TemporalRoDB
}

func NewInternalAPI(base *BaseAPI, db kv.TemporalRoDB) *InternalAPIImpl {
	return &InternalAPIImpl{
		BaseAPI: base,
		db:      db,
	}
}

func (api *InternalAPIImpl) GetTxNumInfo(ctx context.Context, txNum uint64) (*TxNumInfo, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))

	ok, bn, err := txNumsReader.FindBlockNum(tx, txNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("block not found by txnID=%d", txNum)
	}
	minTxNum, err := txNumsReader.Min(tx, bn)
	if err != nil {
		return nil, err
	}
	txIndex := int(txNum) - int(minTxNum) - 1 /* system-tx */
	if txIndex == -1 {
		return nil, nil
	}

	return &TxNumInfo{
		BlockNum: bn,
		Idx:      uint64(txIndex),
	}, nil
}
