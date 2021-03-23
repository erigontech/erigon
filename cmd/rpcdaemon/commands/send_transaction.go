package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

// SendRawTransaction implements eth_sendRawTransaction. Creates new message call transaction or a contract creation for previously-signed transactions.
func (api *APIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	if api.ethBackend == nil {
		// We're running in --chaindata mode or otherwise cannot get the backend
		return common.Hash{}, fmt.Errorf(NotAvailableChainData, "eth_sendRawTransaction")
	}
	res, err := api.ethBackend.AddLocal(ctx, encodedTx)
	return common.BytesToHash(res), err
}

// SendTransaction implements eth_sendTransaction. Creates new message call transaction or a contract creation if the data field contains code.
func (api *APIImpl) SendTransaction(_ context.Context, txObject interface{}) (common.Hash, error) {
	return common.Hash{0}, fmt.Errorf(NotImplemented, "eth_sendTransaction")
}
