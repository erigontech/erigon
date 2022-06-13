package commands

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

func getBlockNumber(number rpc.BlockNumber, tx kv.Tx) (uint64, error) {
	var blockNum uint64
	var err error
	switch number {

	case rpc.LatestBlockNumber:
		blockNum, err = rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return 0, err
		}
	case rpc.PendingBlockNumber:
		blockNum, err = rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return 0, err
		}

	case rpc.EarliestBlockNumber:
		blockNum = 0

	case rpc.FinalizeBlockNumber:
		blockNum, err = rpchelper.GetFinalizedBlockNumber(tx)
		if err != nil {
			return 0, err
		}

	case rpc.SafeBlockNumber:
		blockNum, err = rpchelper.GetSafeBlockNumber(tx)
		if err != nil {
			return 0, err
		}

	default:
		blockNum = uint64(number.Int64())
	}

	return blockNum, nil
}
