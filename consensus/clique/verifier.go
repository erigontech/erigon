package clique

import (
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

func (c *Clique) Verify(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header, uncle bool, seal bool) error {
	return c.VerifyHeader(chain, header, seal)
}

// Fixme: TBD
func (c *Clique) NeededForVerification(header *types.Header) int {
	return 0
}