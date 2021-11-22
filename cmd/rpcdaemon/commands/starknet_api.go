package commands

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/common/hexutil"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
)

type StarknetAPI interface {
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
}

type StarknetImpl struct {
	*BaseAPI
	txPool txpool.TxpoolClient
	db     kv.RoDB
}

func NewStarknetAPI(base *BaseAPI, db kv.RoDB) *StarknetImpl {
	return &StarknetImpl{
		BaseAPI: base,
		db:      db,
	}
}
