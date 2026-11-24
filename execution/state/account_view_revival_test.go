package state

import (
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// After a destruct and a revival in earlier txs, the view of a later tx composes the fresh nonce 0.
// createObject records a fresh whole-account Address cell (nonce 0) on revival, and the view
// composes over it, so the committed nonce of the prior incarnation does not come back.
func TestVersionedAccountView_RevivalDropsPreDestructNonce(t *testing.T) {
	t.Parallel()
	addr := getAddress(8)

	base := accounts.NewAccount()
	base.Nonce = 5 // pre-destruct committed nonce
	reader := &fixedAccountReader{acc: &base}

	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true) // tx1: destruct
	// tx2: revival, as createObject records it.
	fresh := accounts.NewAccount()
	vm.WriteAddress(addr, Version{TxIndex: 2}, &fresh, true)
	vm.WriteCodeHash(addr, Version{TxIndex: 2}, accounts.EmptyCodeHash, true)
	vm.WriteBalance(addr, Version{TxIndex: 2}, uint256.Int{}, true)

	// A later tx composes its pre-write base from the version map.
	view := NewVersionedAccountView(addr, 3, vm, reader)

	require.EqualValues(t, 0, view.GetNonce(), "revived account must not resurrect the pre-destruct nonce")
	acc := view.Account()
	require.NotNil(t, acc)
	require.EqualValues(t, 0, acc.Nonce, "composed account nonce must be the fresh 0, not 5")
}

// x has nonce 5, committed or written by tx 0. Tx 1 destroys x and tx 2 credits it, which creates
// a fresh account. Tx 2 itself and tx 3 read nonce 0, through the IBS, the interpreter, the
// account view and the versioned state reader.
func TestCreditAfterADestructReadsAFreshNonce(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		committed bool
	}{
		{"committed nonce", true},
		{"nonce cell", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := getAddress(9)
			old := accounts.NewAccount()
			old.Nonce = 5
			reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
			vm := NewVersionMap(nil)
			if tc.committed {
				reader.accounts[x] = &old
			} else {
				vm.WriteAddress(x, Version{TxIndex: 0}, &old, true)
				vm.WriteNonce(x, Version{TxIndex: 0}, old.Nonce, true)
			}
			removal := &WriteSet{}
			removal.SetContractRemoval(x, Version{TxIndex: 1})
			vm.FlushVersionedWrites(removal, true)

			credit := NewWithVersionMap(reader, vm)
			defer credit.Close()
			credit.SetTxContext(1, 2)
			require.NoError(t, credit.AddBalance(x, *uint256.NewInt(1), tracing.BalanceChangeTransfer))
			ownNonce, err := credit.GetNonce(x)
			require.NoError(t, err)
			require.Zero(t, ownNonce)
			credit.EndTransaction()
			require.Equal(t, VersionValid, vm.ValidateReadSet(2, credit.VersionedReads(), validateEqualVersion, false, ""))
			vm.FlushVersionedWrites(credit.VersionedWrites(), true)

			ibs := NewWithVersionMap(reader, vm)
			defer ibs.Close()
			ibs.SetTxContext(1, 3)
			nonce, err := ibs.GetNonce(x)
			require.NoError(t, err)
			require.Zero(t, nonce)
			require.Equal(t, VersionValid, vm.ValidateReadSet(3, ibs.VersionedReads(), validateEqualVersion, false, ""))
			acc, ok, _ := vm.ViewAtTxIndex(3, reader).GetAccount(x)
			require.True(t, ok)
			require.Zero(t, acc.Nonce)
			view := NewVersionedAccountView(x, 3, vm, reader).Account()
			require.NotNil(t, view)
			require.Zero(t, view.Nonce)
			applied, err := NewVersionedStateReader(3, ReadSet{}, vm, reader).ReadAccountData(x)
			require.NoError(t, err)
			require.NotNil(t, applied)
			require.Zero(t, applied.Nonce)
		})
	}
}

// Tx 0 creates x, with or without a nonce, and tx 1 destroys it. Tx 2 creates x again by crediting
// it or by setting its code, and writes the same cells in both cases. The tx does not record the
// nonce cell below as a read, so a write that depends on it would outlive a re-run of tx 0 that
// changes the cell.
func TestCreationWritesDoNotDependOnANonceCellBelow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		recreate func(*IntraBlockState, accounts.Address) error
	}{
		{"credit", func(ibs *IntraBlockState, x accounts.Address) error {
			return ibs.AddBalance(x, *uint256.NewInt(1), tracing.BalanceChangeTransfer)
		}},
		{"set code", func(ibs *IntraBlockState, x accounts.Address) error {
			return ibs.SetCode(x, []byte{0x00}, tracing.CodeChangeUnspecified)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := getAddress(10)
			writes := func(nonceCell bool) []AccountPath {
				reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
				vm := NewVersionMap(nil)
				created := accounts.NewAccount()
				vm.WriteAddress(x, Version{TxIndex: 0}, &created, true)
				if nonceCell {
					vm.WriteNonce(x, Version{TxIndex: 0}, 1, true)
				}
				removal := &WriteSet{}
				removal.SetContractRemoval(x, Version{TxIndex: 1})
				vm.FlushVersionedWrites(removal, true)

				ibs := NewWithVersionMap(reader, vm)
				defer ibs.Close()
				ibs.SetTxContext(1, 2)
				require.NoError(t, tc.recreate(ibs, x))
				ibs.EndTransaction()
				var paths []AccountPath
				for h := range ibs.VersionedWrites().AllHeaders() {
					if h.Address == x {
						paths = append(paths, h.Path)
					}
				}
				slices.Sort(paths)
				return paths
			}
			require.NotContains(t, writes(true), NoncePath)
			require.Equal(t, writes(false), writes(true))
		})
	}
}
