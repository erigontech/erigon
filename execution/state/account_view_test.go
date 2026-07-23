package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

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
