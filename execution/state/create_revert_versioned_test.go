package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// createObjectChange.revert must drop the account-record cells that createObject
// emitted (AddressPath/CodeHashPath) so a reverted creation leaves no versioned
// write behind on the noMaterialize path — the journal keeps the write-set in
// step on its own, without dirties re-processing.
func TestNoMaterialize_CreateRevertDropsCells(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x11})
	ibs := NewWithVersionMap(&emptyReader{}, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	_, created := ibs.VersionedWrites().GetAddress(addr)
	require.True(t, created, "create records an AddressPath write")

	ibs.RevertToSnapshot(snap, nil)

	_, ok := ibs.VersionedWrites().GetAddress(addr)
	require.False(t, ok, "reverting the creation must drop the AddressPath write")
	exist, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exist, "the account must not exist after the creation is reverted")
}

// resetObjectChange.revert must restore, via restoreCreateFields, the
// account-record cells the recreation overwrote. createObject stamps the
// CodeHashPath to EmptyCodeHash, so a CreateAccount over a live contract clears
// its code hash; reverting must bring the prior hash back on the noMaterialize
// path (the journal keeps the write-set in step on its own).
func TestNoMaterialize_RecreateRevertRestoresPrior(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x22})
	code := []byte("live-contract-code")
	committed := accounts.NewAccount()
	committed.Nonce = 5
	committed.Balance = *uint256.NewInt(1000)
	committed.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	reader := &fieldReader{addr: addr, account: &committed, code: code}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true)) // stamps CodeHashPath -> empty
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, ch, "recreation clears the code hash")

	ibs.RevertToSnapshot(snap, nil)

	ch, err = ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, committed.CodeHash, ch, "reverting the recreation restores the prior code hash")
}
