package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
)

func callTxPoolContent() {
	if err := requests.TxpoolContent(models.ReqId); err != nil {
		fmt.Printf("error getting txpool content: %v\n", err)
	}
}
