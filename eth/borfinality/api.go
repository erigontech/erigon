package borfinality

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
)

func GetFinalizedBlockNumber(tx kv.Tx) (uint64, error) {
	currentBlockNum := rawdb.ReadCurrentHeader(tx)
	s := whitelist.Service{}

	doExist, number, hash := s.GetWhitelistedMilestone()
	if doExist && number <= currentBlockNum.Number.Uint64() {
		block, err := requests.GetBlockByNumber(models.ReqId, number, false)

		if err != nil {
			return 0, err
		}

		if block.Result.Hash == hash {
			return number, nil
		}
	}

	return 0, fmt.Errorf("No finalized block")
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
