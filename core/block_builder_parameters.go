package core

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

// Parameters for PoS block building
// See also https://github.com/ethereum/execution-apis/blob/main/src/engine/specification.md#payloadattributesv2
type BlockBuilderParameters struct {
	ParentHash            common.Hash
	Timestamp             uint64
	PrevRandao            common.Hash
	SuggestedFeeRecipient common.Address
	Withdrawals           []*types.Withdrawal
	PayloadId             uint64
}
