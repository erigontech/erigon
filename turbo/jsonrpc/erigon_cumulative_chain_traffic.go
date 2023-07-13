package jsonrpc

import (
	"context"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/rpc"
)

// CumulativeGasIndex implements erigon_cumulativeChainTraffic. Returns how much traffic there has been at the specified block number.
// Aka. amount of gas used so far + total transactions issued to the network
func (api *ErigonImpl) CumulativeChainTraffic(ctx context.Context, blockNr rpc.BlockNumber) (ChainTraffic, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return ChainTraffic{}, err
	}
	defer tx.Rollback()

	blockNumber := uint64(blockNr)
	cumulativeGasUsed, err := rawdb.ReadCumulativeGasUsed(tx, blockNumber)
	if err != nil {
		return ChainTraffic{}, err
	}

	_, baseTxId, txCount, err := rawdb.ReadBodyByNumber(tx, blockNumber)
	if err != nil {
		return ChainTraffic{}, err
	}

	cumulativeTransactionCount := baseTxId + uint64(txCount)
	return ChainTraffic{
		CumulativeGasUsed:           (*hexutil.Big)(cumulativeGasUsed),
		CumulativeTransactionsCount: (*hexutil.Uint64)(&cumulativeTransactionCount),
	}, nil
}

type ChainTraffic struct {
	CumulativeGasUsed           *hexutil.Big    `json:"cumulativeGasUsed"`
	CumulativeTransactionsCount *hexutil.Uint64 `json:"cumulativeTransactionsCount"`
}
