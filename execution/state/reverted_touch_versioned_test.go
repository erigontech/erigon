package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// A touch of an existing empty account records a BalancePath=0 versioned write
// paired only with a touchAccount journal entry whose revert is a no-op. If the
// frame is reverted, the address leaves journal.dirties but the write must not
// survive into the published write set — otherwise Normalize's EIP-161 pass would
// delete an account whose touch was rolled back (the ripeMD wrong-root shape).
func TestNoMaterialize_RevertedTouchNotPublished(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x11, 0x22, 0x33})
	empty := accounts.NewAccount() // existing but EIP-161-empty: nonce 0, balance 0, no code
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
