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
	_, err := api.GetBlockByNumber(context.TODO(), blockNumber, false)
	// returning error if the block wasn't found
	return err
}
