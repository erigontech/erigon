package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

// SendRawTransaction send a raw transaction
func (api *APIImpl) SendRawTransaction(_ context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	if api.ethBackend == nil {
		// We're running in --chaindata mode or otherwise cannot get the backend
		return common.Hash{}, fmt.Errorf("eth_sendRawTransaction function is not available")
	}
	res, err := api.ethBackend.AddLocal(encodedTx)
	return common.BytesToHash(res), err
}
