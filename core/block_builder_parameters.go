package core

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
)

// Parameters for PoS block building
// See also https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#payloadattributesv3
type BlockBuilderParameters struct {
	PayloadId             uint64
	ParentHash            libcommon.Hash
	Timestamp             uint64
	PrevRandao            libcommon.Hash
	SuggestedFeeRecipient libcommon.Address
	Withdrawals           []*types.Withdrawal // added in Shapella (EIP-4895)
	ParentBeaconBlockRoot *libcommon.Hash     // added in Dencun (EIP-4788)
}
