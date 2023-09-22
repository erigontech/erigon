package health

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/rpc"
)

func checkBlockNumber(blockNumber rpc.BlockNumber, api EthAPI) error {
	if api == nil {
		return fmt.Errorf("no connection to the Erigon server or `eth` namespace isn't enabled")
	}
	data, err := api.GetBlockByNumber(context.TODO(), blockNumber, false)
	if err != nil {
		return err
	}
	if len(data) == 0 { // block not found
		return fmt.Errorf("no known block with number %v (%x hex)", blockNumber.Uint64(), blockNumber.Uint64())
	}

	return nil
}
