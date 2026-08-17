package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Revert must restore a pre-destruct balance write, not delete the cell outright, or a legitimate prior write is lost.
func TestSelfdestructVersioned_RevertPreservesPriorBalanceWrite(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x5DBA1"))
	reader := newAccountStateReader()
	vm := NewVersionMap(nil)
	ibs := New(reader)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(1000), tracing.BalanceChangeUnspecified))
	pre, ok := ibs.versionedWrites.GetBalance(addr)
	require.True(t, ok, "precondition: a versioned balance write exists")
	require.Equal(t, uint64(1000), pre.Val.Uint64())

	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)

	ibs.RevertToSnapshot(snap, nil)

	got, ok := ibs.versionedWrites.GetBalance(addr)
	require.True(t, ok, "reverting the self-destruct must not delete the pre-snapshot balance write")
	require.Equal(t, uint64(1000), got.Val.Uint64(), "the pre-snapshot balance value must be restored")
}

// EIP-8246: versioned self-destruct must not clear the nonce/code cells — a same-tx re-create reads them for its collision check.
func TestEIP8246_SelfdestructVersioned_PreservesBumpedNonce(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246F"))
	reader := newAccountStateReader()
	vm := NewVersionMap(nil)
	ibs := New(reader)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	ibs.eip8246 = true

	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetCode(addr, []byte("twenty-two-byte-code!!"), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetNonce(addr, 2, tracing.NonceChangeUnspecified))

	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(addr, true)
	require.NoError(t, err)
	n, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(2), n, "preserve-balance self-destruct must not clobber the bumped nonce")

	ibs.RevertToSnapshot(snap, nil)

	n, err = ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(2), n, "reverting the self-destruct leaves the bumped nonce intact")
}
