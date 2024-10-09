package types

import (
	"reflect"

	"github.com/ledgerwatch/erigon-lib/common"
)

// Sequence represents an operation sent to the PoE smart contract to be
// processed.
type Sequence struct {
	GlobalExitRoot, StateRoot, LocalExitRoot common.Hash //
	AccInputHash                             common.Hash // 1024
	Timestamp                                int64       //64
	BatchL2Data                              []byte
	IsSequenceTooBig                         bool   // 8
	BatchNumber                              uint64 // 64
	ForcedBatchTimestamp                     int64  // 64
}

// IsEmpty checks is sequence struct is empty
func (s Sequence) IsEmpty() bool {
	return reflect.DeepEqual(s, Sequence{})
}
