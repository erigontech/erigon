package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestVersionedAccountView_MatchesMaterialized pins the lazy composing view
// against the proven materialized versionedStateReader.ReadAccountData: for
// every scenario the per-field accessors must yield exactly what the
// materialized account exposes (and zero/empty when the account doesn't exist).
func TestVersionedAccountView_MatchesMaterialized(t *testing.T) {
	t.Parallel()

	const txIdx = 10
	code := accounts.NewCode([]byte{0x60, 0x00}).Hash

	cases := []struct {
		name  string
		setup func(vm *VersionMap, addr accounts.Address)
	}{
		{"empty - nothing written", func(vm *VersionMap, addr accounts.Address) {}},
		{"address-path base only", func(vm *VersionMap, addr accounts.Address) {
			vm.WriteAddress(addr, Version{TxIndex: 1}, &accounts.Account{Nonce: 3, Balance: *uint256.NewInt(100), Incarnation: 1, CodeHash: code}, true)
		}},
		{"address base + balance overlay", func(vm *VersionMap, addr accounts.Address) {
			vm.WriteAddress(addr, Version{TxIndex: 1}, &accounts.Account{Nonce: 3, Balance: *uint256.NewInt(100)}, true)
			writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 4}, *uint256.NewInt(250), true)
		}},
		{"field-only (synth) balance+nonce", func(vm *VersionMap, addr accounts.Address) {
			writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, *uint256.NewInt(7), true)
			writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 2}, uint64(1), true)
		}},
		{"self-destruct, no revival", func(vm *VersionMap, addr accounts.Address) {
			vm.WriteAddress(addr, Version{TxIndex: 1}, &accounts.Account{Nonce: 3, Balance: *uint256.NewInt(100)}, true)
			writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 5}, true, true)
		}},
		{"self-destruct then re-create (metamorphic)", func(vm *VersionMap, addr accounts.Address) {
			vm.WriteAddress(addr, Version{TxIndex: 1}, &accounts.Account{Nonce: 3, Balance: *uint256.NewInt(100)}, true)
			writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 5}, true, true)
			vm.WriteAddress(addr, Version{TxIndex: 6}, &accounts.Account{Nonce: 1, Balance: *uint256.NewInt(9), Incarnation: 2}, true)
		}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vm := NewVersionMap(nil)
			addr := getAddress(1)
			tc.setup(vm, addr)

			view := NewVersionedAccountView(addr, txIdx, vm, nil)
			mat, err := NewVersionedStateReader(txIdx, ReadSet{}, vm, nil).ReadAccountData(addr)
			require.NoError(t, err)

			if mat == nil {
				bal := view.GetBalance()
				require.True(t, bal.IsZero(), "balance")
				require.Equal(t, uint64(0), view.GetNonce(), "nonce")
				require.Equal(t, uint64(0), view.GetIncarnation(), "incarnation")
				require.True(t, view.IsEmptyCodeHash(), "codehash")
				return
			}
			require.Equal(t, mat.Balance, view.GetBalance(), "balance")
			require.Equal(t, mat.Nonce, view.GetNonce(), "nonce")
			require.Equal(t, mat.Incarnation, view.GetIncarnation(), "incarnation")
			require.Equal(t, mat.CodeHash, view.GetCodeHash(), "codehash")
		})
	}
}

// TestVersionedAccountView_GetCode_MatchesMaterialized pins the composed
// GetCode against versionedStateReader.ReadAccountCode across base-only,
// this-tx CodePath write, write-wins-over-base, and destroyed scenarios.
func TestVersionedAccountView_GetCode_MatchesMaterialized(t *testing.T) {
	t.Parallel()

	const txIdx = 10
	baseCode := []byte{0x60, 0x00, 0x60, 0x00}
	txCode := []byte{0x60, 0x01, 0x60, 0x01}

	cases := []struct {
		name  string
		setup func(vm *VersionMap, addr accounts.Address)
		base  bool
	}{
		{"empty - nothing written", func(vm *VersionMap, addr accounts.Address) {}, false},
		{"base only", func(vm *VersionMap, addr accounts.Address) {}, true},
		{"this-tx CodePath write, no base", func(vm *VersionMap, addr accounts.Address) {
			vm.WriteCode(addr, Version{TxIndex: 4}, accounts.NewCode(txCode), true)
		}, false},
		{"this-tx CodePath write wins over base", func(vm *VersionMap, addr accounts.Address) {
			vm.WriteCode(addr, Version{TxIndex: 4}, accounts.NewCode(txCode), true)
		}, true},
		{"write above txIdx not visible, base wins", func(vm *VersionMap, addr accounts.Address) {
			vm.WriteCode(addr, Version{TxIndex: 20}, accounts.NewCode(txCode), true)
		}, true},
		{"self-destruct, no revival", func(vm *VersionMap, addr accounts.Address) {
			vm.WriteAddress(addr, Version{TxIndex: 1}, &accounts.Account{Nonce: 3}, true)
			writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 5}, true, true)
		}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vm := NewVersionMap(nil)
			addr := getAddress(1)
			tc.setup(vm, addr)

			var base StateReader
			if tc.base {
				base = &codeReader{addr: addr, account: &accounts.Account{Nonce: 1, CodeHash: accounts.NewCode(baseCode).Hash}, code: baseCode}
			}

			view := NewVersionedAccountView(addr, txIdx, vm, base)
			want, err := NewVersionedStateReader(txIdx, ReadSet{}, vm, base).ReadAccountCode(addr)
			require.NoError(t, err)

			got, err := view.GetCode()
			require.NoError(t, err)
			require.Equal(t, want, got, "code")
		})
	}
}

// TestVersionedAccountView_GetCode_ReviveComposesConsistently pins the one
// property the single composing view guarantees but the materialized reader
// does not: after a cross-tx self-destruct + re-create, GetCode composes the
// revived code (not nil) and stays consistent with GetCodeHash. The
// materialized versionedStateReader.ReadAccountCode returns nil here (it checks
// only the latest SelfDestruct, never composing the revival that ReadAccountData
// and GetCodeHash do) — a phasing desync the composing view removes.
func TestVersionedAccountView_GetCode_ReviveComposesConsistently(t *testing.T) {
	t.Parallel()

	txCode := []byte{0x60, 0x01, 0x60, 0x01}
	vm := NewVersionMap(nil)
	addr := getAddress(1)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 5}, true, true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 6}, accounts.NewCode(txCode).Hash, true)
	vm.WriteCode(addr, Version{TxIndex: 6}, accounts.NewCode(txCode), true)

	view := NewVersionedAccountView(addr, 10, vm, nil)

	code, err := view.GetCode()
	require.NoError(t, err)
	require.Equal(t, txCode, code, "revived code")
	require.Equal(t, crypto.Keccak256Hash(code), view.GetCodeHash().Value(), "code/codehash consistency")
}
