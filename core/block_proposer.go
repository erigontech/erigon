package core

import "github.com/ledgerwatch/erigon/common"

// See https://github.com/ethereum/execution-apis/blob/main/src/engine/specification.md#payloadattributesv1
type BlockProposerParametersPOS struct {
	ParentHash            common.Hash
	Timestamp             uint64
	PrevRandao            common.Hash
	SuggestedFeeRecipient common.Address
}
