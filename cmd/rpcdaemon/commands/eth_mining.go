package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
)

// Propose changing this file name to eth_mining.go and adding missing mining related commands

// Coinbase is the address that mining rewards will be sent to
func (api *APIImpl) Coinbase(_ context.Context) (common.Address, error) {
	if api.ethBackend == nil {
		// We're running in --chaindata mode or otherwise cannot get the backend
		return common.Address{}, fmt.Errorf(NotAvailableChainData, "eth_coinbase")
	}
	return api.ethBackend.Etherbase()
}

// Missing routines (incorrect interfaces)

// HashRate(ctx context.Context) (string, error) {}
// Mining(ctx context.Context) (string, error) {}
// GetWork(ctx context.Context) (string, error) {}
// SubmitWork(ctx context.Context) (string, error) {}
// SubmitHashrate(ctx context.Context) (string, error) {}
