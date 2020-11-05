package ethash

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

func (ethash *Ethash) Verify(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header, uncle bool, seal bool) error {
	if len(parents) == 0 {
		return errors.New("need a parent to verify the header")
	}
	return ethash.verifyHeader(chain, header, parents[len(parents)-1], uncle, seal)
}

func (ethash *Ethash) NeededForVerification(h *types.Header) int {
	const prev = 10
	if h.Number.Uint64() < prev {
		return int(h.Number.Uint64())
	}
	return prev
}
