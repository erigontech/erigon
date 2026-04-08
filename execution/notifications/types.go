// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package notifications

import (
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// Direction of a state change (forward execution or unwind/reorg).
type Direction int

const (
	DirectionForward Direction = iota
	DirectionUnwind
)

// Action describes what happened to an account in a state change.
type Action int

const (
	ActionStorage    Action = iota // storage-only change
	ActionUpsert                   // account data changed
	ActionUpsertCode               // account data + code changed
	ActionCode                     // code-only change
	ActionRemove                   // account deleted
)

// StateChange holds per-block state changes in native Go types.
// No protobuf dependencies — conversion to protobuf happens at the gRPC boundary.
type StateChange struct {
	BlockHeight uint64
	BlockHash   common.Hash
	BlockTime   uint64
	Direction   Direction
	Txs         [][]byte
	Changes     []AccountChange
}

// AccountChange holds a single account's state change.
type AccountChange struct {
	Address        common.Address
	Incarnation    uint64
	Action         Action
	Data           []byte
	Code           []byte
	StorageChanges []StorageChange
}

// StorageChange holds a single storage slot change.
type StorageChange struct {
	Location common.Hash
	Data     []byte
}

// BlockBatchNotification carries state changes for a batch of blocks.
// This is the primary notification type for local (in-process) consumers.
type BlockBatchNotification struct {
	StateVersionID      uint64
	Changes             []StateChange
	PendingBlockBaseFee uint64
	PendingBlobFee      uint64
	BlockGasLimit       uint64
	FinalizedBlock      uint64
}

// LogNotification carries a log event in native types.
type LogNotification struct {
	*types.Log
	Removed bool // true on unwind
}

// ReceiptNotification carries a receipt with its transaction and header context.
type ReceiptNotification struct {
	Receipt *types.Receipt
	Tx      types.Transaction
	Header  *types.Header
	Removed bool
}

// Proto conversion methods — used at the gRPC boundary only.

func directionToProto(d Direction) remoteproto.Direction {
	if d == DirectionUnwind {
		return remoteproto.Direction_UNWIND
	}
	return remoteproto.Direction_FORWARD
}

func actionToProto(a Action) remoteproto.Action {
	switch a {
	case ActionStorage:
		return remoteproto.Action_STORAGE
	case ActionUpsert:
		return remoteproto.Action_UPSERT
	case ActionUpsertCode:
		return remoteproto.Action_UPSERT_CODE
	case ActionCode:
		return remoteproto.Action_CODE
	case ActionRemove:
		return remoteproto.Action_REMOVE
	default:
		panic(fmt.Sprintf("unknown action: %d", a))
	}
}

// ToProto converts a StateChange to its protobuf representation.
func (sc *StateChange) ToProto() *remoteproto.StateChange {
	proto := &remoteproto.StateChange{
		BlockHeight: sc.BlockHeight,
		BlockHash:   gointerfaces.ConvertHashToH256(sc.BlockHash),
		BlockTime:   sc.BlockTime,
		Direction:   directionToProto(sc.Direction),
	}
	if sc.Txs != nil {
		proto.Txs = make([][]byte, len(sc.Txs))
		copy(proto.Txs, sc.Txs)
	}
	for i := range sc.Changes {
		proto.Changes = append(proto.Changes, sc.Changes[i].ToProto())
	}
	return proto
}

// ToProto converts an AccountChange to its protobuf representation.
func (ac *AccountChange) ToProto() *remoteproto.AccountChange {
	proto := &remoteproto.AccountChange{
		Address:     gointerfaces.ConvertAddressToH160(ac.Address),
		Incarnation: ac.Incarnation,
		Action:      actionToProto(ac.Action),
		Data:        ac.Data,
		Code:        ac.Code,
	}
	for i := range ac.StorageChanges {
		proto.StorageChanges = append(proto.StorageChanges, ac.StorageChanges[i].ToProto())
	}
	return proto
}

// ToProto converts a StorageChange to its protobuf representation.
func (sc *StorageChange) ToProto() *remoteproto.StorageChange {
	return &remoteproto.StorageChange{
		Location: gointerfaces.ConvertHashToH256(sc.Location),
		Data:     sc.Data,
	}
}

// ToProtoBatch converts a BlockBatchNotification to a protobuf StateChangeBatch.
func (b *BlockBatchNotification) ToProtoBatch() *remoteproto.StateChangeBatch {
	batch := &remoteproto.StateChangeBatch{
		StateVersionId:       b.StateVersionID,
		PendingBlockBaseFee:  b.PendingBlockBaseFee,
		PendingBlobFeePerGas: b.PendingBlobFee,
		BlockGasLimit:        b.BlockGasLimit,
		FinalizedBlock:       b.FinalizedBlock,
	}
	for i := range b.Changes {
		batch.ChangeBatch = append(batch.ChangeBatch, b.Changes[i].ToProto())
	}
	return batch
}
