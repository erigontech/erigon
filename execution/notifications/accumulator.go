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
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// Accumulator collects state changes per block in native Go types.
// Protobuf conversion happens only at the gRPC boundary via ToProto methods.
type Accumulator struct {
	plainStateID       uint64
	changes            []StateChange
	latestChange       *StateChange
	accountChangeIndex map[common.Address]int // For the latest change, allows finding account change by address
	storageChangeIndex map[common.Address]map[common.Hash]int
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

func (a *Accumulator) Changes() []StateChange {
	return a.changes
}

func (a *Accumulator) Reset(plainStateID uint64) {
	a.changes = nil
	a.latestChange = nil
	a.accountChangeIndex = nil
	a.storageChangeIndex = nil
	a.plainStateID = plainStateID
}

// SendAndReset dispatches accumulated changes to both remote (gRPC/protobuf) and
// local (in-process/native) consumers, then resets the accumulator.
func (a *Accumulator) SendAndReset(ctx context.Context, c StateChangeConsumer, pendingBaseFee uint64, pendingBlobFee uint64, blockGasLimit uint64, finalizedBlock uint64) {
	if a == nil || len(a.changes) == 0 {
		return
	}
	batch := &BlockBatchNotification{
		StateVersionID:      a.plainStateID,
		Changes:             a.changes,
		PendingBlockBaseFee: pendingBaseFee,
		PendingBlobFee:      pendingBlobFee,
		BlockGasLimit:       blockGasLimit,
		FinalizedBlock:      finalizedBlock,
	}
	if c != nil {
		c.SendStateChanges(ctx, batch.ToProtoBatch())
	}
	a.Reset(0) // reset here for GC, but there will be another Reset with correct viewID
}

func (a *Accumulator) SetStateID(stateID uint64) {
	a.plainStateID = stateID
}

// StartChange begins accumulation of changes for a new block.
func (a *Accumulator) StartChange(h *types.Header, txs [][]byte, unwind bool) {
	a.changes = append(a.changes, StateChange{
		BlockHeight: h.Number.Uint64(),
		BlockHash:   h.Hash(),
		BlockTime:   h.Time,
	})
	a.latestChange = &a.changes[len(a.changes)-1]
	if unwind {
		a.latestChange.Direction = DirectionUnwind
	}
	a.accountChangeIndex = make(map[common.Address]int)
	a.storageChangeIndex = make(map[common.Address]map[common.Hash]int)
	if txs != nil {
		a.latestChange.Txs = make([][]byte, len(txs))
		for i := range txs {
			a.latestChange.Txs[i] = common.Copy(txs[i])
		}
	}
}

// ChangeAccount adds modification of account balance or nonce (or both) to the latest change.
func (a *Accumulator) ChangeAccount(address common.Address, incarnation uint64, data []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok || incarnation > a.latestChange.Changes[i].Incarnation {
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, AccountChange{Address: address})
		a.accountChangeIndex[address] = i
		delete(a.storageChangeIndex, address)
	}
	accountChange := &a.latestChange.Changes[i]
	switch accountChange.Action {
	case ActionStorage:
		accountChange.Action = ActionUpsert
	case ActionCode:
		accountChange.Action = ActionUpsertCode
	case ActionRemove:
		//panic("")
	}
	accountChange.Incarnation = incarnation
	accountChange.Data = data
}

// DeleteAccount marks account as deleted.
func (a *Accumulator) DeleteAccount(address common.Address) {
	i, ok := a.accountChangeIndex[address]
	if !ok {
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, AccountChange{Address: address})
		a.accountChangeIndex[address] = i
	}
	accountChange := &a.latestChange.Changes[i]
	if accountChange.Action != ActionStorage {
		panic("")
	}
	accountChange.Data = nil
	accountChange.Code = nil
	accountChange.StorageChanges = nil
	accountChange.Action = ActionRemove
	delete(a.storageChangeIndex, address)
}

// ChangeCode adds code to the latest change.
func (a *Accumulator) ChangeCode(address common.Address, incarnation uint64, code []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok || incarnation > a.latestChange.Changes[i].Incarnation {
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, AccountChange{Address: address, Action: ActionCode})
		a.accountChangeIndex[address] = i
		delete(a.storageChangeIndex, address)
	}
	accountChange := &a.latestChange.Changes[i]
	switch accountChange.Action {
	case ActionStorage:
		accountChange.Action = ActionCode
	case ActionUpsert:
		accountChange.Action = ActionUpsertCode
	case ActionRemove:
		//panic("")
	}
	accountChange.Incarnation = incarnation
	accountChange.Code = code
}

// ChangeStorage adds a storage slot change to the latest change.
func (a *Accumulator) ChangeStorage(address common.Address, incarnation uint64, location common.Hash, data []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok || incarnation > a.latestChange.Changes[i].Incarnation {
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, AccountChange{Address: address, Action: ActionStorage})
		a.accountChangeIndex[address] = i
		delete(a.storageChangeIndex, address)
	}
	accountChange := &a.latestChange.Changes[i]
	accountChange.Incarnation = incarnation
	si, ok1 := a.storageChangeIndex[address]
	if !ok1 {
		si = make(map[common.Hash]int)
		a.storageChangeIndex[address] = si
	}
	j, ok2 := si[location]
	if !ok2 {
		j = len(accountChange.StorageChanges)
		accountChange.StorageChanges = append(accountChange.StorageChanges, StorageChange{})
		si[location] = j
	}
	accountChange.StorageChanges[j].Location = location
	accountChange.StorageChanges[j].Data = data
}

// CopyAndReset moves accumulated data from this instance into target, then resets this instance.
func (a *Accumulator) CopyAndReset(target *Accumulator) {
	target.changes = a.changes
	a.changes = nil
	target.latestChange = a.latestChange
	a.latestChange = nil
	target.accountChangeIndex = a.accountChangeIndex
	a.accountChangeIndex = nil
	target.storageChangeIndex = a.storageChangeIndex
	a.storageChangeIndex = nil
}
