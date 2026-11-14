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
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type readSetParentState struct {
	accounts map[accounts.Address]*accounts.Account
	storage  map[accounts.Address]map[accounts.StorageKey][]byte
}

func parentStateFromReads(io *state.VersionedIO) *readSetParentState {
	p := &readSetParentState{accounts: map[accounts.Address]*accounts.Account{}, storage: map[accounts.Address]map[accounts.StorageKey][]byte{}}
	for tx := -1; tx < io.Len()-1; tx++ {
		reads := io.ReadSet(tx)
		reads.RangeFullHeaders(func(addr accounts.Address, path state.AccountPath, key accounts.StorageKey, hdr state.ReadHeader) bool {
			if hdr.Source != state.StorageRead {
				return true
			}
			switch path {
			case state.AddressPath:
				if _, ok := p.accounts[addr]; ok {
					return true
				}
				r, _ := reads.GetAddress(addr)
				var acc *accounts.Account
				if r.Val != nil && !r.Val.IsNil() {
					c := *r.Val.Account()
					acc = &c
				}
				p.accounts[addr] = acc
			case state.StoragePath:
				slots := p.storage[addr]
				if slots == nil {
					slots = map[accounts.StorageKey][]byte{}
					p.storage[addr] = slots
				}
				if _, ok := slots[key]; ok {
					return true
				}
				r, _ := reads.GetStorage(addr, key)
				slots[key] = r.Val.Bytes()
			}
			return true
		})
	}
	return p
}

func (p *readSetParentState) GetCommittedAccount(addr accounts.Address) (*accounts.Account, bool) {
	acc, ok := p.accounts[addr]
	return acc, ok
}

func (p *readSetParentState) GetCommittedStorage(addr accounts.Address, key accounts.StorageKey) ([]byte, bool) {
	val, ok := p.storage[addr][key]
	return val, ok
}
