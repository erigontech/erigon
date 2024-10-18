package polygoncommon

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rlp"
)

func NewBorMessages(stateSyncEvents []rlp.RawValue, stateReceiverContract *libcommon.Address, gasLimit uint64) []*types.Message {
	msgs := make([]*types.Message, len(stateSyncEvents))
	for i, event := range stateSyncEvents {
		msg := types.NewMessage(
			state.SystemAddress, // from
			stateReceiverContract,
			0,         // nonce
			u256.Num0, // amount
			gasLimit,
			u256.Num0, // gasPrice
			nil,       // feeCap
			nil,       // tip
			event,
			nil,   // accessList
			false, // checkNonce
			true,  // isFree
			nil,   // maxFeePerBlobGas
		)

		msgs[i] = &msg
	}

	return msgs
}
