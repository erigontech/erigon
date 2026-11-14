// Copyright 2026 The Erigon Authors
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

package stagedsync

import (
	"slices"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func (cc *commitmentCalculator) emit(view *state.VersionMapView, updates *commitment.Updates) error {
	if cc.branchPrefetch != nil {
		return cc.emitTouchedFeed(view)
	}
	return cc.emitTouched(view, updates)
}

func (cc *commitmentCalculator) prefetchWrites(writes state.WriteSetView) {
	seen := map[accounts.Address][]byte{}
	account := func(a accounts.Address) []byte {
		hash, ok := seen[a]
		if !ok {
			address := a.Value()
			hash = cc.touchHash(address[:])
			seen[a] = hash
			cc.queueBranch(hash, nil)
		}
		return hash
	}
	for a := range writes.AccountChanges() {
		account(a)
	}
	for a, slots := range writes.StorageChanges() {
		hash := account(a)
		address := a.Value()
		for k := range slots {
			slot := k.Value()
			cc.queueBranch(hash, cc.touchHash(slices.Concat(address[:], slot[:])))
		}
	}
}

type feedBuffer struct {
	commitment.Feed
	updates []commitment.Update
	slots   []commitment.FeedSlot
	values  []byte
}

func (cc *commitmentCalculator) emitTouchedFeed(view *state.VersionMapView) error {
	if cc.baseErr != nil {
		return cc.baseErr
	}
	f := &cc.feed
	slots := 0
	for _, m := range cc.touchedStorage {
		slots += len(m)
	}
	cc.parentReads = commitmentdb.ParentReadsWanted()
	f.Keys = len(cc.touchedAccounts) + slots
	f.Accounts = slices.Grow(f.Accounts[:0], len(cc.touchedAccounts))
	f.updates = slices.Grow(f.updates[:0], len(cc.touchedAccounts))
	f.slots = slices.Grow(f.slots[:0], slots)
	f.values = slices.Grow(f.values[:0], length.Hash*slots)
	for addr, hashed := range cc.touchedAccounts {
		f.updates = append(f.updates, cc.accountUpdate(view, addr))
		u := &f.updates[len(f.updates)-1]
		start := len(f.slots)
		for slot, slotHash := range cc.touchedStorage[addr] {
			at := len(f.values)
			if val, ok := view.GetStorage(addr, slot); ok && !val.IsZero() {
				n := val.ByteLen()
				f.values = f.values[:at+n]
				val.WriteToSlice(f.values[at:])
			}
			end := len(f.values)
			f.slots = append(f.slots, commitment.FeedSlot{Hash: [32]byte(slotHash), Value: f.values[at:end:end]})
		}
		end := len(f.slots)
		f.Accounts = append(f.Accounts, commitment.FeedAccount{Hash: [32]byte(hashed), Update: u, Slots: f.slots[start:end:end]})
	}
	return view.Err()
}
