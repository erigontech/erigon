package commands

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

func getBlockNumber(number rpc.BlockNumber, tx kv.Tx) (uint64, error) {
	var blockNum uint64
	switch number {

	case rpc.LatestBlockNumber:
		return rpchelper.GetLatestBlockNumber(tx)

	case rpc.PendingBlockNumber:
		return rpchelper.GetLatestBlockNumber(tx)

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
