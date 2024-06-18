package da

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
)

const maxAttempts = 10
const retryDelay = 500 * time.Millisecond

func GetOffChainData(ctx context.Context, url string, hash common.Hash) ([]byte, error) {
	attemp := 0

	for attemp < maxAttempts {
		response, err := client.JSONRPCCall(url, "sync_getOffChainData", hash)

		if httpErr, ok := err.(*client.HTTPError); ok && httpErr.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay)
			attemp += 1
			continue
		}

		if err != nil {
			return nil, err
		}

		if response.Error != nil {
			return nil, fmt.Errorf("%v %v", response.Error.Code, response.Error.Message)
		}

		return hexutil.Decode(strings.Trim(string(response.Result), "\""))
	}

	return nil, fmt.Errorf("max attempts of data fetching reached, attempts: %v, DA url: %s", maxAttempts, url)
}
