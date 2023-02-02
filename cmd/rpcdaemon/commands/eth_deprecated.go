package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
)

// Accounts implements eth_accounts. Returns a list of addresses owned by the client.
// Deprecated: This function will be removed in the future.
func (api *APIImpl) Accounts(ctx context.Context) ([]common.Address, error) {
	return []common.Address{}, fmt.Errorf(NotAvailableDeprecated, "eth_accounts")
}

// Sign implements eth_sign. Calculates an Ethereum specific signature with: sign(keccak256('\\x19Ethereum Signed Message:\\n' + len(message) + message))).
// Deprecated: This function will be removed in the future.
func (api *APIImpl) Sign(ctx context.Context, _ common.Address, _ hexutil.Bytes) (hexutil.Bytes, error) {
	return hexutil.Bytes(""), fmt.Errorf(NotAvailableDeprecated, "eth_sign")
}

// SignTransaction deprecated
func (api *APIImpl) SignTransaction(_ context.Context, txObject interface{}) (common.Hash, error) {
	return common.Hash{0}, fmt.Errorf(NotAvailableDeprecated, "eth_signTransaction")
}
