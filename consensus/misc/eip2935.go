package misc

import (
	"math/big"

	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func StoreBlockHash2935(header *types.Header, state *state.IntraBlockState ) {
		// EIP-2935
		// TODO @somnathb1 Hash the slot?
		storageSlot := libcommon.BigToHash(big.NewInt(0).Sub(header.Number, big.NewInt(1)))
		parentHashInt, err := uint256.FromHex(header.ParentHash.String())
		if err !=nil {
			panic(err)	// should not happen
		}
		state.SetState(params.HistoryStorageAddress, &storageSlot, *parentHashInt)
}
