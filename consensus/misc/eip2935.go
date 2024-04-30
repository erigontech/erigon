package misc

import (
	// "fmt"
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
	if header.Number.Cmp(big.NewInt(0)) == 0 { // Activation of fork at Genesis
		return
	}
	parent := headerReader.GetHeaderByHash(header.ParentHash)
	_storeHash(parent.Number, header.ParentHash, state)
	// If this is the fork block, add the parent's direct `HISTORY_SERVE_WINDOW - 1` ancestors as well
	if parent.Time < config.PragueTime.Uint64() {
		p := parent.Number.Uint64()
		window := params.BlockHashHistoryServeWindow - 1
		if p < window {
			window = p
		}
		for i := window; i > 0; i-- {
			_storeHash(big.NewInt(0).Sub(parent.Number, big.NewInt(1)), parent.ParentHash, state)
			parent = headerReader.GetHeaderByHash(parent.ParentHash)
			// fmt.Println("Storing parent %x, for i=%d", parent.ParentHash, i)
		}
	}
}

func _storeHash(num *big.Int, hash libcommon.Hash, state *state.IntraBlockState) {
	storageSlot := libcommon.BigToHash(big.NewInt(0).Mod(num, big.NewInt(int64(params.BlockHashHistoryServeWindow))))
	hh := hash.Big()
	parentHashInt := uint256.MustFromBig(hh)
	state.SetState(params.HistoryStorageAddress, &storageSlot, *parentHashInt)
}
