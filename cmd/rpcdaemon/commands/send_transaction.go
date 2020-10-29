package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

// SendRawTransaction implements eth_sendRawTransaction. Creates new message call transaction or a contract creation for previously-signed transactions.
func (api *APIImpl) SendRawTransaction(_ context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	if api.ethBackend == nil {
		// We're running in --chaindata mode or otherwise cannot get the backend
		return common.Hash{}, fmt.Errorf(NotAvailableChainData, "eth_sendRawTransaction")
	}
	res, err := api.ethBackend.AddLocal(encodedTx)
	return common.BytesToHash(res), err
}

// SendTransaction implements eth_sendTransaction. Creates new message call transaction or a contract creation if the data field contains code.
// Note: Use eth_getTransactionReceipt to get the contract address, after the transaction was mined, when you created a contract
// Parameters:
//   Object - The transaction object
//   from: DATA, 20 Bytes - The address the transaction is send from
//   to: DATA, 20 Bytes - (optional when creating new contract) The address the transaction is directed to
//   gas: QUANTITY - (optional, default 90000) Integer of the gas provided for the transaction execution. It will return unused gas
//   gasPrice: QUANTITY - (optional, default To-Be-Determined) Integer of the gasPrice used for each paid gas
//   value: QUANTITY - (optional) Integer of the value sent with this transaction
//   data: DATA - The compiled code of a contract OR the hash of the invoked method signature and encoded parameters. For details see Ethereum Contract ABI
//   nonce: QUANTITY - (optional) Integer of a nonce. This allows to overwrite your own pending transactions that use the same nonce
// Returns:
//   DATA, 32 Bytes - The transaction hash, or the zero hash if the transaction is not yet available
