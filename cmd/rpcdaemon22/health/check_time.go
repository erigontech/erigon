package health

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/rpc"
)

var (
	errTimestampTooOld = errors.New("timestamp too old")
)

func checkTime(
	r *http.Request,
	seconds int,
	ethAPI EthAPI,
) error {
	i, err := ethAPI.GetBlockByNumber(r.Context(), rpc.LatestBlockNumber, false)
	if err != nil {
		return err
	}
	timestamp := 0
	if ts, ok := i["timestamp"]; ok {
		if cs, ok := ts.(uint64); ok {
			timestamp = int(cs)
		}
	}
	if timestamp > seconds {
		return fmt.Errorf("%w: got ts: %d, need: %d", errTimestampTooOld, timestamp, seconds)
	}

	return nil
}
