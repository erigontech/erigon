package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// A reverted touch on an empty account must not publish its BalancePath=0 write, even though the address stays in
// journal.dirties — else EIP-161 Normalize would delete an account whose touch was rolled back (the RIPEMD wrong-root case).
func TestNoMaterialize_RevertedTouchNotPublished(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x11, 0x22, 0x33})
	empty := accounts.NewAccount()
	reader := &fieldReader{addr: addr, account: &empty}

	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 5)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.TouchAccount(addr))
	_, touched := ibs.VersionedWrites().GetBalance(addr)
	require.True(t, touched, "touch records a BalancePath write while in effect")

	ibs.RevertToSnapshot(snap, nil)

	_, ok := ibs.VersionedWrites().GetBalance(addr)
	require.False(t, ok, "a touch reverted before commit must not publish a BalancePath write")
}
