package clique

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

func (c *Clique) Verify(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header, uncle bool, seal bool) error {
	panic("not implemented")
}

func (c *Clique) NeededForVerification(header *types.Header) []common.Hash {
	panic("not implemented")
}

func (c *Clique) IsFake() bool {
	panic("not implemented")
}
