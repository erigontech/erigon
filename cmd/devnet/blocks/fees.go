package blocks

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
)

func BaseFeeFromBlock(ctx context.Context) (uint64, error) {
	var val uint64
	res, err := devnet.SelectNode(ctx).GetBlockDetailsByNumber("latest", false)
	if err != nil {
		return 0, fmt.Errorf("failed to get base fee from block: %v\n", err)
	}

	if v, ok := res["baseFeePerGas"]; !ok {
		return val, fmt.Errorf("baseFeePerGas field missing from response")
	} else {
		val = devnetutils.HexToInt(v.(string))
	}

	return val, err
}
