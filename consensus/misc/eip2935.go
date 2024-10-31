package misc

import (
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
)

func StoreBlockHashesEip2935(header *types.Header, state *state.IntraBlockState, config *chain.Config, headerReader consensus.ChainHeaderReader) {
	headerNum := header.Number.Uint64()
	if headerNum == 0 { // Activation of fork at Genesis
		return
	}
	storeHash(headerNum-1, header.ParentHash, state)
}

func storeHash(num uint64, hash libcommon.Hash, state *state.IntraBlockState) {
	// state.Witness().TouchFullAccount(params.HistoryStorageAddress[:], true)
	slotNum := num % params.BlockHashHistoryServeWindow
	storageSlot := libcommon.BytesToHash(uint256.NewInt(slotNum).Bytes())
	parentHashInt := uint256.NewInt(0).SetBytes32(hash.Bytes())
	state.SetState(params.HistoryStorageAddress, &storageSlot, *parentHashInt)
	state.Witness().TouchSlotAndChargeGas(params.HistoryStorageAddress[:], storageSlot, true)
}

func GetBlockHashFromContractEip2935(number uint64, state evmtypes.IntraBlockState, value *uint256.Int, witness *state.AccessWitness) uint64 {
	slotNum := number % params.BlockHashHistoryServeWindow
	storageSlot := libcommon.BytesToHash(uint256.NewInt(slotNum).Bytes())
	statelessGas := witness.TouchSlotAndChargeGas(params.HistoryStorageAddress[:], storageSlot, false)
	state.GetState(params.HistoryStorageAddress, &storageSlot, value)
	return statelessGas
}
