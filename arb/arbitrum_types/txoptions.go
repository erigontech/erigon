package arbitrum_types

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/rpc"
	"github.com/pkg/errors"
)

type rejectedError struct {
	msg string
}

func NewRejectedError(msg string) *rejectedError {
	return &rejectedError{msg: msg}
}
func (e rejectedError) Error() string { return e.msg }
func (rejectedError) ErrorCode() int  { return -32003 }

type limitExceededError struct {
	msg string
}

func NewLimitExceededError(msg string) *limitExceededError {
	return &limitExceededError{msg: msg}
}
func (e limitExceededError) Error() string { return e.msg }
func (limitExceededError) ErrorCode() int  { return -32005 }

func WrapOptionsCheckError(err error, msg string) error {
	wrappedMsg := func(e rpc.Error, msg string) string {
		return strings.Join([]string{msg, e.Error()}, ":")
	}
	switch e := err.(type) {
	case *rejectedError:
		return NewRejectedError(wrappedMsg(e, msg))
	case *limitExceededError:
		return NewLimitExceededError(wrappedMsg(e, msg))
	default:
		return errors.Wrap(err, msg)
	}
}

type RootHashOrSlots struct {
	RootHash  *common.Hash
	SlotValue map[common.Hash]common.Hash
}

func (r *RootHashOrSlots) UnmarshalJSON(data []byte) error {
	var hash common.Hash
	var err error
	if err = json.Unmarshal(data, &hash); err == nil {
		r.RootHash = &hash
		return nil
	}
	return json.Unmarshal(data, &r.SlotValue)
}

func (r RootHashOrSlots) MarshalJSON() ([]byte, error) {
	if r.RootHash != nil {
		return json.Marshal(*r.RootHash)
	}
	return json.Marshal(r.SlotValue)
}

type ConditionalOptions struct {
	KnownAccounts  map[common.Address]RootHashOrSlots `json:"knownAccounts"`
	BlockNumberMin *math.HexOrDecimal64               `json:"blockNumberMin,omitempty"`
	BlockNumberMax *math.HexOrDecimal64               `json:"blockNumberMax,omitempty"`
	TimestampMin   *math.HexOrDecimal64               `json:"timestampMin,omitempty"`
	TimestampMax   *math.HexOrDecimal64               `json:"timestampMax,omitempty"`
}

func (o *ConditionalOptions) Check(l1BlockNumber uint64, l2Timestamp uint64, statedb *state.StateDB) error {
	if o.BlockNumberMin != nil && l1BlockNumber < uint64(*o.BlockNumberMin) {
		return NewRejectedError("BlockNumberMin condition not met")
	}
	if o.BlockNumberMax != nil && l1BlockNumber > uint64(*o.BlockNumberMax) {
		return NewRejectedError("BlockNumberMax condition not met")
	}
	if o.TimestampMin != nil && l2Timestamp < uint64(*o.TimestampMin) {
		return NewRejectedError("TimestampMin condition not met")
	}
	if o.TimestampMax != nil && l2Timestamp > uint64(*o.TimestampMax) {
		return NewRejectedError("TimestampMax condition not met")
	}
	for address, rootHashOrSlots := range o.KnownAccounts {
		if rootHashOrSlots.RootHash != nil {
			storageRoot := statedb.GetStorageRoot(address)
			if storageRoot != *rootHashOrSlots.RootHash {
				return NewRejectedError("Storage root hash condition not met")
			}
		} else if len(rootHashOrSlots.SlotValue) > 0 {
			for slot, value := range rootHashOrSlots.SlotValue {
				stored := statedb.GetState(address, slot)
				if !bytes.Equal(stored.Bytes(), value.Bytes()) {
					return NewRejectedError("Storage slot value condition not met")
				}
			}
		} // else rootHashOrSlots.SlotValue is empty - ignore it and check the rest of conditions
	}
	return nil
}
