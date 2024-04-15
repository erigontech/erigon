package sync

import "github.com/ledgerwatch/erigon/core/types"

type BlocksVerifier func(blocks []*types.Block) error

func VerifyBlocks(blocks []*types.Block) error {
	for _, block := range blocks {
		if err := block.SanityCheck(); err != nil {
			return err
		}

		if err := block.HashCheck(); err != nil {
			return err
		}
	}

	return nil
}
