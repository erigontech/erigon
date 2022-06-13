package commands

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

func getBlockNumber(number rpc.BlockNumber, tx kv.Tx, filters *rpchelper.Filters) (uint64, error) {
	var blockNum uint64
	latest, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return 0, err
	}

	switch number {
	case rpc.LatestBlockNumber:
		return latest, nil

	case rpc.PendingBlockNumber:
		pendingBlock := filters.LastPendingBlock()
		if pendingBlock == nil {
			return latest, nil
		}
		return pendingBlock.NumberU64(), nil

	case rpc.EarliestBlockNumber:
		blockNum = 0

	case rpc.FinalizeBlockNumber:
		return rpchelper.GetFinalizedBlockNumber(tx)

	case rpc.SafeBlockNumber:
		return rpchelper.GetSafeBlockNumber(tx)

	default:
		blockNum = uint64(number.Int64())
	}

	return blockNum, nil
}
