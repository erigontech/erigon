package core

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
)

// Parameters for PoS block building
// See also https://github.com/ethereum/execution-apis/blob/main/src/engine/specification.md#payloadattributesv2
type BlockBuilderParameters struct {
	ParentHash            libcommon.Hash
	Timestamp             uint64
	PrevRandao            libcommon.Hash
	SuggestedFeeRecipient libcommon.Address
	Withdrawals           []*types.Withdrawal
	PayloadId             uint64
}
