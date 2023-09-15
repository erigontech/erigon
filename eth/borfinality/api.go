package borfinality

import (
	"context"
	"errors"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
	"github.com/ledgerwatch/erigon/turbo/services"
)

func GetFinalizedBlockNumber(tx kv.Tx, blockReader services.FullBlockReader) (uint64, error) {
	currentBlockNum := rawdb.ReadCurrentHeader(tx)

	service := whitelist.GetWhitelistingService()

	doExist, number, hash := service.GetWhitelistedMilestone()
	if doExist && number <= currentBlockNum.Number.Uint64() {
		block, err := blockReader.BlockByNumber(context.Background(), tx, number)

		if err != nil {
			return 0, err
		}

		if block.Hash() == hash {
			return number, nil
		}
	}

	doExist, number, hash = service.GetWhitelistedCheckpoint()
	if doExist && number <= currentBlockNum.Number.Uint64() {
		block, err := blockReader.BlockByNumber(context.Background(), tx, number)

		if err != nil {
			return 0, err
		}

		if block.Hash() == hash {
			return number, nil
		}
	}

	return 0, errors.New("no finalized block")
}

// CurrentFinalizedBlock retrieves the current finalized block of the canonical
// chain. The block is retrieved from the blockchain's internal cache.
func CurrentFinalizedBlock(tx kv.Tx, number uint64) *types.Block {
	hash, err := rawdb.ReadCanonicalHash(tx, number)
	if err != nil || hash == (common.Hash{}) {
		return nil
	}

	return rawdb.ReadBlock(tx, hash, number)
}
