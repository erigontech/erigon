package health

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon/rpc"
)

var errTimestampTooOld = errors.New("timestamp too old")

func checkTime(
	r *http.Request,
	seconds int,
	ethAPI EthAPI,
) error {
	fullTx := false
	i, err := ethAPI.GetBlockByNumber(r.Context(), rpc.LatestBlockNumber, &fullTx)
	if err != nil {
		return err
	}
	timestamp := uint64(0)
	if ts, ok := i["timestamp"]; ok {
		if cs, ok := ts.(hexutil.Uint64); ok {
			timestamp = cs.Uint64()
		}
	}
	if timestamp < uint64(seconds) {
		return fmt.Errorf("%w: got ts: %d, need: %d", errTimestampTooOld, timestamp, seconds)
	}

	return nil
}
