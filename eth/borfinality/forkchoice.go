package borfinality

import (
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
)

// ValidateReorg calls the chain validator service to check if the reorg is valid or not
// This function is specific to Bor chain
func ValidateReorg(current *types.Header) (bool, error) {
	// Call the bor chain validator service
	s := whitelist.GetWhitelistingService()
	if s != nil {
		return s.IsValidChain(current, nil)
	}

	return true, nil
}
