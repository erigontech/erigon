package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
)

type StarknetAPI interface {
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
	GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)
	Call(ctx context.Context, request StarknetCallRequest, blockNrOrHash rpc.BlockNumberOrHash) ([]string, error)
}

type StarknetImpl struct {
	*BaseAPI
	db     kv.RoDB
	client starknet.CAIROVMClient
	txPool txpool.TxpoolClient
}

func NewStarknetAPI(base *BaseAPI, db kv.RoDB, client starknet.CAIROVMClient, txPool txpool.TxpoolClient) *StarknetImpl {
	return &StarknetImpl{
		BaseAPI: base,
		db:      db,
		client:  client,
		txPool:  txPool,
	}
}
