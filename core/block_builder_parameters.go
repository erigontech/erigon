package core

import "github.com/ledgerwatch/erigon/common"

// Parameters for PoS block building
// See also https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.9/src/engine/specification.md#payloadattributesv1
type BlockBuilderParameters struct {
	ParentHash            common.Hash
	Timestamp             uint64
	PrevRandao            common.Hash
	SuggestedFeeRecipient common.Address
	PayloadId             uint64
}
