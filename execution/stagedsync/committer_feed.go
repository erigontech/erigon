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
	"github.com/erigontech/erigon/execution/state"
)

func (cc *commitmentCalculator) feeds() bool {
	return cc.doms.GetCommitmentContext().AcceptsFeed()
}

func (cc *commitmentCalculator) emit(view *state.VersionMapView, updates *commitment.Updates) error {
	if cc.feeds() {
		return cc.emitTouchedFeed(view)
	}
	cc.deleted = nil
	return cc.emitTouched(view, updates)
}

func (cc *commitmentCalculator) setInput(updates *commitment.Updates) {
	if cc.feeds() {
		cc.doms.GetCommitmentContext().SetFeed(&cc.feed.Feed)
		return
	}
	cc.doms.GetCommitmentContext().SetUpdates(updates)
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
	cc.deleted = [][]byte{}
	f.Keys = len(cc.touchedAccounts) + slots
	f.Accounts = slices.Grow(f.Accounts[:0], len(cc.touchedAccounts))
	f.updates = slices.Grow(f.updates[:0], len(cc.touchedAccounts))
	f.slots = slices.Grow(f.slots[:0], slots)
	f.values = slices.Grow(f.values[:0], length.Hash*slots)
	for addr, hashed := range cc.touchedAccounts {
		address := addr.Value()
		f.updates = append(f.updates, cc.accountUpdate(view, addr))
		u := &f.updates[len(f.updates)-1]
		if u.Deleted() {
			cc.deleted = append(cc.deleted, address[:])
		}
		start := len(f.slots)
		for slot, slotHash := range cc.touchedStorage[addr] {
			at := len(f.values)
			if val, ok := view.GetStorage(addr, slot); ok && !val.IsZero() {
				n := val.ByteLen()
				f.values = f.values[:at+n]
				val.WriteToSlice(f.values[at:])
			} else {
				s := slot.Value()
				cc.deleted = append(cc.deleted, slices.Concat(address[:], s[:]))
			}
			end := len(f.values)
			f.slots = append(f.slots, commitment.FeedSlot{Hash: [32]byte(slotHash), Value: f.values[at:end:end]})
		}
		end := len(f.slots)
		f.Accounts = append(f.Accounts, commitment.FeedAccount{Hash: [32]byte(hashed), Update: u, Slots: f.slots[start:end:end]})
	}
	return view.Err()
}
