package sync

import "github.com/ledgerwatch/erigon/core/types"

type BlocksVerifier func(blocks []*types.Block) error

func VerifyBlocks(_ []*types.Block) error {
	//
	// TODO (subsequent PRs)
	//
	return nil
}
