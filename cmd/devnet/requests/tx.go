package requests

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
)

func TxpoolContent(reqId int) (int, int, error) {
	var (
		b       rpctest.EthTxPool
		pending map[string]interface{}
		queued  map[string]interface{}
	)

	reqGen := initialiseRequestGenerator(reqId)

	if res := reqGen.Erigon("txpool_content", reqGen.TxpoolContent(), &b); res.Err != nil {
		return len(pending), len(queued), fmt.Errorf("failed to fetch txpool content: %v", res.Err)
	}

	resp := b.Result.(map[string]interface{})

	s, err := devnetutils.ParseResponse(b)
	if err != nil {
		return len(pending), len(queued), fmt.Errorf("error parsing resonse: %v", err)
	}

	if resp["pending"] != nil {
		pending = resp["pending"].(map[string]interface{})
	}
	if resp["queue"] != nil {
		queued = resp["queue"].(map[string]interface{})
	}

	fmt.Printf("Txpool content: %v\n", s)
	return len(pending), len(queued), nil
}
