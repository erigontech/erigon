package borfinality

import (
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
)

// ValidateReorg calls the chain validator service to check if the reorg is valid or not
func ValidateReorg(current *types.Header, chain []*types.Header, config *chain.Config) (bool, error) {
	// Call the bor chain validator service
	s := whitelist.Service{}
	if config.Bor != nil {
		return s.IsValidChain(current, chain)
	}

	return true, nil
}
