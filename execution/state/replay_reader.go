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

package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// ReplayReader memoizes committed reads for a request that replays several transactions on
// one versioned state, as the materialized state objects did between transactions.
type ReplayReader struct {
	StateReader
	accounts map[accounts.Address]*accounts.Account
	storage  map[replaySlot]replaySlotValue
	code     map[accounts.Address][]byte
}

type replaySlot struct {
	addr accounts.Address
	key  accounts.StorageKey
}

type replaySlotValue struct {
	val uint256.Int
	ok  bool
}

func NewReplayReader(r StateReader) *ReplayReader {
	return &ReplayReader{
		StateReader: r,
		accounts:    make(map[accounts.Address]*accounts.Account),
		storage:     make(map[replaySlot]replaySlotValue),
		code:        make(map[accounts.Address][]byte),
	}
}

func (r *ReplayReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if acc, ok := r.accounts[addr]; ok {
		return acc, nil
	}
	acc, err := r.StateReader.ReadAccountData(addr)
	if err != nil {
		return nil, err
	}
	r.accounts[addr] = acc
	return acc, nil
}

func (r *ReplayReader) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	slot := replaySlot{addr: addr, key: key}
	if v, ok := r.storage[slot]; ok {
		return v.val, v.ok, nil
	}
	val, ok, err := r.StateReader.ReadAccountStorage(addr, key)
	if err != nil {
		return uint256.Int{}, false, err
	}
	r.storage[slot] = replaySlotValue{val: val, ok: ok}
	return val, ok, nil
}

func (r *ReplayReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	if code, ok := r.code[addr]; ok {
		return code, nil
	}
	code, err := r.StateReader.ReadAccountCode(addr)
	if err != nil {
		return nil, err
	}
	r.code[addr] = code
	return code, nil
}

func (r *ReplayReader) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	if code, ok := r.code[addr]; ok {
		return len(code), nil
	}
	return r.StateReader.ReadAccountCodeSize(addr)
}
