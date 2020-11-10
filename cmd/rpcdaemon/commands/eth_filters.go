package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

// NewPendingTransactionFilter new transaction filter
func (api *APIImpl) NewPendingTransactionFilter(_ context.Context) (hexutil.Uint64, error) {
	return 0, fmt.Errorf(NotImplemented, "eth_newPendingTransactionFilter")
}

// NewBlockFilter new transaction filter
func (api *APIImpl) NewBlockFilter(_ context.Context) (hexutil.Uint64, error) {
	return 0, fmt.Errorf(NotImplemented, "eth_newBlockFilter")
}

// NewFilter implements eth_newFilter. Creates an arbitrary filter object, based on filter options, to notify when the state changes (logs).
func (api *APIImpl) NewFilter(_ context.Context, filter interface{}) (hexutil.Uint64, error) {
	return 0, fmt.Errorf(NotImplemented, "eth_newFilter")
}

// UninstallFilter new transaction filter
func (api *APIImpl) UninstallFilter(_ context.Context, index hexutil.Uint64) (bool, error) {
	return false, fmt.Errorf(NotImplemented, "eth_uninstallFilter")
}

// GetFilterChanges implements eth_getFilterChanges. Polling method for a previously-created filter, which returns an array of logs which occurred since last poll.
func (api *APIImpl) GetFilterChanges(_ context.Context, index hexutil.Uint64) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "eth_getFilterChanges")
}
