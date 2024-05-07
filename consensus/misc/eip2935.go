package misc

import (
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func StoreBlockHashesEip2935(header *types.Header, state *state.IntraBlockState, config *chain.Config, headerReader consensus.ChainHeaderReader) {
	headerNum := header.Number.Uint64()
	if headerNum == 0 { // Activation of fork at Genesis
		return
	}
	storeHash(headerNum-1, header.ParentHash, state)
	// If this is the fork block, add the parent's direct `HISTORY_SERVE_WINDOW - 1` ancestors as well
	parent := headerReader.GetHeader(header.ParentHash, headerNum-1)
	if parent.Time < config.PragueTime.Uint64() {
		p := headerNum - 1
		window := params.BlockHashHistoryServeWindow - 1
		if p < window {
			window = p
		}
		for i := window; i > 0; i-- {
			p = p - 1
			storeHash(p, parent.ParentHash, state)
			parent = headerReader.GetHeader(parent.ParentHash, p)
		}
	}
}

func storeHash(num uint64, hash libcommon.Hash, state *state.IntraBlockState) {
	slotNum := num % params.BlockHashHistoryServeWindow
	storageSlot := libcommon.BytesToHash(uint256.NewInt(slotNum).Bytes())
	parentHashInt := uint256.NewInt(0).SetBytes32(hash.Bytes())
	state.SetState(params.HistoryStorageAddress, &storageSlot, *parentHashInt)
}
