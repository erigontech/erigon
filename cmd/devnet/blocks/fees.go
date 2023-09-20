package blocks

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/rpc"
)

func BaseFeeFromBlock(ctx context.Context) (uint64, error) {
	res, err := devnet.SelectNode(ctx).GetBlockByNumber(rpc.LatestBlockNumber, false)

	if err != nil {
		return 0, fmt.Errorf("failed to get base fee from block: %v\n", err)
	}

	return res.BaseFee.Uint64(), err
}
