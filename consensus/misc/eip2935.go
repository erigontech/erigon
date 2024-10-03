package misc

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func StoreBlockHashesEip2935(header *types.Header, state *state.IntraBlockState, config *chain.Config, headerReader consensus.ChainHeaderReader) {
	if state.GetCodeSize(params.HistoryStorageAddress) == 0 {
		log.Debug("[EIP-2935] No code deployed to HistoryStorageAddress before call to store EIP-2935 history")
		return
	}
	headerNum := header.Number.Uint64()
	if headerNum == 0 { // Activation of fork at Genesis
		return
	}
	storeHash(headerNum-1, header.ParentHash, state)
}

func storeHash(num uint64, hash libcommon.Hash, state *state.IntraBlockState) {
	slotNum := num % params.BlockHashHistoryServeWindow
	storageSlot := libcommon.BytesToHash(uint256.NewInt(slotNum).Bytes())
	parentHashInt := uint256.NewInt(0).SetBytes32(hash.Bytes())
	state.SetState(params.HistoryStorageAddress, &storageSlot, *parentHashInt)
}
