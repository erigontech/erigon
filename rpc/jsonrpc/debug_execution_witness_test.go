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

package jsonrpc

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// fakeStateReader is a minimal state.StateReader backed by an in-memory account
// map, used to exercise RecordingState predicates without a database.
type fakeStateReader struct {
	accounts map[common.Address]*accounts.Account
}

func (r *fakeStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	return r.accounts[address.Value()], nil
}
func (r *fakeStateReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.accounts[address.Value()], nil
}
func (r *fakeStateReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (r *fakeStateReader) HasStorage(address accounts.Address) (bool, error)         { return false, nil }
func (r *fakeStateReader) ReadAccountCode(address accounts.Address) ([]byte, error)  { return nil, nil }
func (r *fakeStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) { return 0, nil }
func (r *fakeStateReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}
func (r *fakeStateReader) SetTrace(_ bool, _ string) {}
func (r *fakeStateReader) Trace() bool               { return false }
func (r *fakeStateReader) TracePrefix() string       { return "" }

func TestRecordingState_accountExists(t *testing.T) {
	existing := common.HexToAddress("0x1111111111111111111111111111111111111111")
	missing := common.HexToAddress("0x2222222222222222222222222222222222222222")
	created := common.HexToAddress("0x3333333333333333333333333333333333333333")
	deleted := common.HexToAddress("0x4444444444444444444444444444444444444444")
	createdThenDeleted := common.HexToAddress("0x5555555555555555555555555555555555555555")

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		existing:           {Nonce: 1},
		deleted:            {Nonce: 1},
		createdThenDeleted: nil,
	}}
	rs := NewRecordingState(inner)

	// created in-block: present in the write overlay
	rs.accountOverlay[created] = &accounts.Account{Nonce: 1}
	// deleted in-block
	rs.DeletedAccounts[deleted] = struct{}{}
	// created in-block then deleted: overlay cleared, recorded as deleted
	rs.DeletedAccounts[createdThenDeleted] = struct{}{}

	cases := []struct {
		name string
		addr common.Address
		want bool
	}{
		{"exists in inner", existing, true},
		{"nonexistent", missing, false},
		{"created in overlay", created, true},
		{"deleted", deleted, false},
		{"created then deleted", createdThenDeleted, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rs.accountExists(tc.addr); got != tc.want {
				t.Fatalf("accountExists(%s) = %v, want %v", tc.addr.Hex(), got, tc.want)
			}
		})
	}
}

// TestCollectAccessedState_KeysOnlyExistingAccounts asserts that a 20-byte address
// key is emitted only for accounts that exist post-state; accessed-but-nonexistent
// addresses are excluded, while their accessed storage slots are still emitted.
func TestCollectAccessedState_KeysOnlyExistingAccounts(t *testing.T) {
	existing := common.HexToAddress("0x1111111111111111111111111111111111111111")
	missing := common.HexToAddress("0x2222222222222222222222222222222222222222")
	slot := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000aa")

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		existing: {Nonce: 1},
	}}
	rs := NewRecordingState(inner)
	rs.AccessedAccounts[existing] = struct{}{}
	rs.AccessedAccounts[missing] = struct{}{}
	rs.AccessedStorage[missing] = map[common.Hash]struct{}{slot: {}}

	accessed := collectAccessedState(rs, witnessModeLegacy)

	var sawExisting, sawMissing, sawSlot bool
	for _, k := range accessed.WitnessKeys {
		switch {
		case len(k) == 20 && common.BytesToAddress(k) == existing:
			sawExisting = true
		case len(k) == 20 && common.BytesToAddress(k) == missing:
			sawMissing = true
		case len(k) == 32 && common.BytesToHash(k) == slot:
			sawSlot = true
		}
	}
	if !sawExisting {
		t.Error("expected key for existing account, missing")
	}
	if sawMissing {
		t.Error("address key emitted for nonexistent account; should be excluded")
	}
	if !sawSlot {
		t.Error("expected storage slot key to still be emitted for nonexistent account")
	}
}
