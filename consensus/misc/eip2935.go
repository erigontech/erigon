package misc

import (
	"math/big"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func StoreBlockHashesEip2935(header *types.Header, state *state.IntraBlockState, config *chain.Config, headerReader consensus.ChainHeaderReader) {
	parent := headerReader.GetHeaderByHash(header.ParentHash)
	if parent == nil && header.Number.Cmp(big.NewInt(0)) == 0 { // Only Genesis block shouldn't have a parent
		return
	}
	_setStorage(parent.Number, parent.Hash(), state)
	// If this is the fork block, add the parent's direct `HISTORY_SERVE_WINDOW - 1` ancestors as well
	if parent.Time < config.OsakaTime.Uint64() {
		for i := params.HISTORY_SERVE_WINDOW - 1; i > 0; i-- {
			parent = headerReader.GetHeaderByHash(parent.ParentHash)
			_setStorage(parent.Number, parent.Hash(), state)
			if parent.Number.Cmp(big.NewInt(0)) == 0 { // Genesis
				break
			}
		}
	}
}

func _setStorage(num *big.Int, hash libcommon.Hash, state *state.IntraBlockState) {
	storageSlot := libcommon.BigToHash(big.NewInt(0).Mod(num, big.NewInt(8192)))
	parentHashInt := uint256.MustFromHex(hash.String())
	state.SetState(params.HistoryStorageAddress, &storageSlot, *parentHashInt)
}
