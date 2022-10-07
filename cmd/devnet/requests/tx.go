package requests

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
)

func TxpoolContent(reqId int) error {
	reqGen := initialiseRequestGenerator(reqId)
	var b rpctest.EthTxPool

	if res := reqGen.Erigon("txpool_content", reqGen.txpoolContent(), &b); res.Err != nil {
		return fmt.Errorf("failed to fetch txpool content: %v", res.Err)
	}

	s, err := devnetutils.ParseResponse(b)
	if err != nil {
		return fmt.Errorf("error parsing resonse: %v", err)
	}

	fmt.Printf("Txpool content: %v\n", s)
	return nil
}
