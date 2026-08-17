package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Reverting a creation must drop the AddressPath/CodeHashPath cells createObject emitted — the journal alone keeps the write-set in step.
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

// Reverting a recreate over a live contract must restore the code hash that createObject's CodeHashPath=empty stamp overwrote.
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
	require.NoError(t, ibs.CreateAccount(addr, true))
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, ch, "recreation clears the code hash")

	ibs.RevertToSnapshot(snap, nil)

	ch, err = ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, committed.CodeHash, ch, "reverting the recreation restores the prior code hash")
}
