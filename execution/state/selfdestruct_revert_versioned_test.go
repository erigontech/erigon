package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSelfdestructVersioned_RevertPreservesPriorBalanceWrite pins that reverting
// a SELFDESTRUCT does not delete a balance write that predates the snapshot. The
// self-destruct records BalancePath=0; the revert must restore the pre-destruct
// versioned balance write rather than delete the cell outright (which would also
// wipe a legitimate prior balance write and diverge the trie root / BAL). This
// mirrors the pre-destruct incarnation-cell restore.
func TestSelfdestructVersioned_RevertPreservesPriorBalanceWrite(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x5DBA1"))
	reader := newAccountStateReader()
	vm := NewVersionMap(nil)
	ibs := New(reader)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	// Balance written BEFORE the snapshot; no prior SelfDestructPath write, so the
	// revert takes the wasCommited branch that previously deleted the balance cell.
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(1000), tracing.BalanceChangeUnspecified))
	pre, ok := ibs.versionedWrites.GetBalance(addr)
	require.True(t, ok, "precondition: a versioned balance write exists")
	require.Equal(t, uint64(1000), pre.Val.Uint64())

	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(addr, false /* burn balance */)
	require.NoError(t, err)

	ibs.RevertToSnapshot(snap, nil)

	got, ok := ibs.versionedWrites.GetBalance(addr)
	require.True(t, ok, "reverting the self-destruct must not delete the pre-snapshot balance write")
	require.Equal(t, uint64(1000), got.Val.Uint64(), "the pre-snapshot balance value must be restored")
}

// A same-tx-created contract bumps its nonce (e.g. it CREATEs a child) and then
// SELFDESTRUCTs (EIP-8246 preserve-balance). The versioned self-destruct must
// NOT clear the nonce/code cells: the account is alive until finalize, and a
// same-tx re-creation at the address reads those cells for its collision check —
// zeroing them there is invisible to the collision but zeroing the versioned
// WRITE made the re-creation abort with a phantom collision. It must also survive
// a revert with the bumped nonce intact.
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
	_, err := ibs.Selfdestruct(addr, true /* preserveBalance */)
	require.NoError(t, err)
	n, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(2), n, "preserve-balance self-destruct must not clobber the bumped nonce")

	ibs.RevertToSnapshot(snap, nil)

	n, err = ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(2), n, "reverting the self-destruct leaves the bumped nonce intact")
}
