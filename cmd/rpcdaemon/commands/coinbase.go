package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
)

// Etherbase is the address that mining rewards will be send to
func (api *APIImpl) Coinbase(_ context.Context) (common.Address, error) {
	return api.ethBackend.Etherbase()
}
