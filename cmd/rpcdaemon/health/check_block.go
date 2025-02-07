package health

import (
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"time"

	"github.com/ledgerwatch/erigon/rpc"
)

func checkBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber, api EthAPI) error {
	if api == nil {
		return fmt.Errorf("no connection to the Erigon server or `eth` namespace isn't enabled")
	}
	fullTx := false
	data, err := api.GetBlockByNumber(ctx, blockNumber, &fullTx)
	if err != nil {
		return err
	}
	if len(data) == 0 { // block not found
		return fmt.Errorf("no known block with number %v (%x hex)", blockNumber.Uint64(), blockNumber.Uint64())
	}

	return nil
}

func checkBlockTime(ctx context.Context, api EthAPI) (*uint, error) {
	if api == nil {
		return nil, fmt.Errorf("no connection to the Erigon server or `eth` namespace isn't enabled")
	}
	fullTx := false
	data, err := api.GetBlockByNumber(ctx, -1, &fullTx)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("last block time not found")
	}

	timestamp, ok := data["timestamp"].(hexutil.Uint64)
	if !ok {
		return nil, fmt.Errorf("unexpected response type for block: %T", data["transactions"])
	}

	blockTime := time.Unix(int64(timestamp), 0).UTC()
	now := time.Now().UTC()
	if blockTime.After(now) {
		return nil, fmt.Errorf("last block time is in the future: %v", lastBlockTime)
	}
	diff := now.Sub(blockTime)
	seconds := uint(diff.Seconds())

	return &seconds, nil
}

func blockTimeStringOrError(err error, blockTime *uint) string {
	if err != nil {
		if errors.Is(err, errCheckDisabled) {
			return "DISABLED"
		}
		return fmt.Sprintf("ERROR: %v", err)
	}

	if blockTime == nil {
		return "block time is nil"
	}

	return fmt.Sprintf("last block was mined more than %v seconds ago", *blockTime)
}
