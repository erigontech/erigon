package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/common"
)

func callSubscribeToNewHeads(hash common.Hash) {
	_, err := services.SearchBlockForTransactionHash(hash)
	if err != nil {
		fmt.Printf("FAILURE => %v\n", err)
		return
	}
}
