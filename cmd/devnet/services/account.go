package services

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

func GetNonce(node devnet.Node, address libcommon.Address) (uint64, error) {
	res, err := node.GetTransactionCount(address, requests.BlockNumbers.Latest)
	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count for address 0x%x: %v", address, err)
	}

	return uint64(res.Result), nil
}
