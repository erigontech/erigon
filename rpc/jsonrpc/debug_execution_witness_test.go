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
	"bytes"
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

// hasEmptyCode reports whether the legacy code set carries the single empty `0x`
// bytecode entry.
func hasEmptyCode(accessed *accessedState) bool {
	for _, c := range accessed.SortedCodes {
		if len(c) == 0 {
			return true
		}
	}
	return false
}

// TestEmptyCodeTrigger asserts when the legacy empty-"0x" entry is emitted and
// when it is not. The trigger fires only when an account transitions from
// non-existent (None) to existing-with-empty-code — matching Reth's
// has_new_contract() — or when empty bytecode is explicitly deployed via
// UpdateAccountCode. Reading an existing EOA must NOT trigger the entry.
func TestEmptyCodeTrigger(t *testing.T) {
	existingEOA := common.HexToAddress("0x1111111111111111111111111111111111111111")
	newAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	contractAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		existingEOA: {Nonce: 1},
	}}

	// Reading an existing EOA must NOT emit "0x" (regression: old code set
	// emptyCodeAccessed on every empty-code account read, producing spurious entries).
	t.Run("existing EOA data read does not trigger", func(t *testing.T) {
		rs := NewRecordingState(inner)
		if _, err := rs.ReadAccountData(accounts.InternAddress(existingEOA)); err != nil {
			t.Fatal(err)
		}
		if hasEmptyCode(collectAccessedState(rs, witnessModeLegacy)) {
			t.Error("existing EOA read must not emit 0x in the legacy witness")
		}
	})

	t.Run("existing EOA code read does not trigger", func(t *testing.T) {
		rs := NewRecordingState(inner)
		if _, err := rs.ReadAccountCode(accounts.InternAddress(existingEOA)); err != nil {
			t.Fatal(err)
		}
		if hasEmptyCode(collectAccessedState(rs, witnessModeLegacy)) {
			t.Error("existing EOA code read must not emit 0x in the legacy witness")
		}
	})

	// UpdateAccountCode with empty bytecode (e.g. CREATE2 deploying nothing) triggers.
	t.Run("UpdateAccountCode empty code triggers", func(t *testing.T) {
		rs := NewRecordingState(inner)
		if err := rs.UpdateAccountCode(accounts.InternAddress(contractAddr), 1, accounts.EmptyCodeHash, []byte{}); err != nil {
			t.Fatal(err)
		}
		if !hasEmptyCode(collectAccessedState(rs, witnessModeLegacy)) {
			t.Error("0x entry missing after UpdateAccountCode with empty bytecode")
		}
	})

	// ETH transfer / coinbase receiving fees for the first time: account is new
	// (not in pre-state) and ends up with empty code in the overlay.
	t.Run("new account in overlay triggers", func(t *testing.T) {
		rs := NewRecordingState(inner)
		// newAddr is not in inner (pre-state); simulate its creation via the public API.
		if err := rs.UpdateAccountData(accounts.InternAddress(newAddr), nil, &accounts.Account{Nonce: 0}); err != nil {
			t.Fatal(err)
		}
		if !hasEmptyCode(collectAccessedState(rs, witnessModeLegacy)) {
			t.Error("0x entry missing for a brand-new account with empty code in the overlay")
		}
	})

	// An existing account modified in-block keeps its empty code but must NOT
	// trigger an extra "0x" — it already existed in the pre-state.
	t.Run("existing account updated in overlay does not trigger", func(t *testing.T) {
		rs := NewRecordingState(inner)
		original := &accounts.Account{Nonce: 1}
		if err := rs.UpdateAccountData(accounts.InternAddress(existingEOA), original, &accounts.Account{Nonce: 2}); err != nil {
			t.Fatal(err)
		}
		if hasEmptyCode(collectAccessedState(rs, witnessModeLegacy)) {
			t.Error("updating an existing EOA in the overlay must not emit 0x")
		}
	})

	// CREATE that deploys empty bytecode detected via CreatedContracts loop
	// (Erigon's SetCode skips UpdateAccountCode when code is nil/empty).
	t.Run("CreateContract with empty code overlay triggers", func(t *testing.T) {
		rs := NewRecordingState(inner)
		if err := rs.CreateContract(accounts.InternAddress(contractAddr)); err != nil {
			t.Fatal(err)
		}
		if err := rs.UpdateAccountData(accounts.InternAddress(contractAddr), nil, &accounts.Account{}); err != nil {
			t.Fatal(err)
		}
		if !hasEmptyCode(collectAccessedState(rs, witnessModeLegacy)) {
			t.Error("0x entry missing for a CREATE with empty code detected via CreatedContracts")
		}
	})
}

// TestCollectAccessedState_Legacy7702Designator asserts the legacy code set carries the
// EIP-7702 delegation designator. For a delegated account the designator is read first
// (AccessedCode[A]=designator) then overwritten by the resolved target code on the second
// read (AccessedCode[A]=target, last-write-wins); the designator survives only in
// PreStateCode. Without it, stateless verification of a block touching the delegated
// account recomputes the wrong state root.
func TestCollectAccessedState_Legacy7702Designator(t *testing.T) {
	eoa := common.HexToAddress("0x1111111111111111111111111111111111111111")
	target := common.HexToAddress("0x3fc8c2b6e4ea901721aef251c67bc7c2591a1e1f")
	designator := append([]byte{0xef, 0x01, 0x00}, target[:]...)
	targetCode := []byte{0x60, 0x00, 0x60, 0x00, 0xfd}

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		eoa: {Nonce: 1},
	}}
	rs := NewRecordingState(inner)
	rs.PreStateCode[eoa] = designator
	rs.AccessedCode[eoa] = targetCode

	accessed := collectAccessedState(rs, witnessModeLegacy)

	var sawDesignator bool
	for _, c := range accessed.SortedCodes {
		if bytes.Equal(c, designator) {
			sawDesignator = true
		}
	}
	if !sawDesignator {
		t.Fatal("legacy witness codes missing the EIP-7702 designator (present only in PreStateCode)")
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

func TestResolveWitnessMode(t *testing.T) {
	str := func(s string) *string { return &s }

	t.Run("param selects mode", func(t *testing.T) {
		got, err := resolveWitnessMode(str("legacy"))
		if err != nil {
			t.Fatal(err)
		}
		if got != witnessModeLegacy {
			t.Errorf("param legacy should resolve to legacy mode, got %v", got)
		}

		got, err = resolveWitnessMode(str("canonical"))
		if err != nil {
			t.Fatal(err)
		}
		if got != witnessModeCanonical {
			t.Errorf("param canonical, got %v", got)
		}
	})

	t.Run("unknown param rejected", func(t *testing.T) {
		if _, err := resolveWitnessMode(str("bogus")); err == nil {
			t.Error("expected error for unknown mode param")
		}
	})

	t.Run("legacy default when param nil", func(t *testing.T) {
		got, err := resolveWitnessMode(nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != witnessModeLegacy {
			t.Errorf("default should be legacy, got %v", got)
		}
	})
}
