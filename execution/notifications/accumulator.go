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
	"bytes"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// Accumulator collects state changes per block in native Go types.
type Accumulator struct {
	changes                []StateChange
	stateChangesIncomplete bool
	latestChange           *StateChange
	accountChangeIndex     map[common.Address]int // For the latest change, allows finding account change by address
	storageChangeIndex     map[common.Address]map[common.Hash]int
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

func (a *Accumulator) Changes() []StateChange {
	return a.changes
}

// An incomplete batch still needs dispatch without recorded block changes.
// Before sending it, the dispatcher adds an entry for the executed head so
// consumers can identify the announced state.
func (a *Accumulator) HasPendingChanges() bool {
	return a != nil && (len(a.changes) > 0 || a.stateChangesIncomplete)
}

func (a *Accumulator) Reset() {
	*a = Accumulator{}
}

// Checkpoint returns a rollback to completed block changes. Later writes must
// start with StartChange so the saved changes remain immutable.
func (a *Accumulator) Checkpoint() (restore func()) {
	a.latestChange = nil
	a.accountChangeIndex = nil
	a.storageChangeIndex = nil
	saved := *a
	return func() { *a = saved }
}

// Batch includes the supplied executed head without adding synthetic entries to recorded changes.
// It shares recorded changes, which must stay unchanged until publication finishes.
func (a *Accumulator) Batch(head *types.Header) *BlockBatchNotification {
	batch := &BlockBatchNotification{
		Changes:                slices.Clip(a.changes),
		StateChangesIncomplete: a.stateChangesIncomplete,
	}
	if len(batch.Changes) == 0 || batch.Changes[len(batch.Changes)-1].BlockHeight < head.Number.Uint64() {
		batch.Changes = append(batch.Changes, stateChangeForHeader(head))
	}
	return batch
}

// MarkIncomplete records omitted state changes for the whole batch. Later
// complete blocks cannot restore completeness; only a reset starts a new batch.
func (a *Accumulator) MarkIncomplete() {
	a.stateChangesIncomplete = true
}

// StartChange begins accumulation of changes for a new block.
func (a *Accumulator) StartChange(h *types.Header, txs [][]byte, unwind bool) {
	a.changes = append(a.changes, stateChangeForHeader(h))
	a.latestChange = &a.changes[len(a.changes)-1]
	if unwind {
		a.latestChange.Direction = DirectionUnwind
	}
	a.accountChangeIndex = make(map[common.Address]int)
	a.storageChangeIndex = make(map[common.Address]map[common.Hash]int)
	if txs != nil {
		a.latestChange.Txs = make([][]byte, len(txs))
		for i := range txs {
			a.latestChange.Txs[i] = bytes.Clone(txs[i])
		}
	}
}

func (a *Accumulator) StartIncompleteRange(firstBlock uint64, lastHeader *types.Header) {
	a.StartChange(lastHeader, nil, false)
	a.latestChange.FirstBlockHeight = firstBlock
	a.MarkIncomplete()
}

func (a *Accumulator) HasAccountChange(address common.Address) bool {
	i, ok := a.accountChangeIndex[address]
	return ok && a.latestChange.Changes[i].Action != ActionStorage
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

// AppendAndReset appends accumulated changes to target, then resets this instance.
// Existing changes must stay before the appended changes, including any unwind.
func (a *Accumulator) AppendAndReset(target *Accumulator) {
	if len(a.changes) > 0 {
		target.changes = append(target.changes, a.changes...)
		target.latestChange = &target.changes[len(target.changes)-1]
		target.accountChangeIndex = a.accountChangeIndex
		target.storageChangeIndex = a.storageChangeIndex
	}
	target.stateChangesIncomplete = target.stateChangesIncomplete || a.stateChangesIncomplete
	a.Reset()
}
