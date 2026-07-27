package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// A self-destruct clears the versioned incarnation write, so a revert must put
// the pre-destruct one back. The pre-destruct incarnation write can exist
// without a matching balance write, which selfdestructChangeVersioned records
// separately — this pins that combination, the one an account-wide revert would
// otherwise leave holding the cleared incarnation.
func TestSelfdestructRevertRestoresIncarnationWithoutBalanceWrite(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x5d, 0x01})
	committed := accounts.NewAccount()
	committed.Balance = *uint256.NewInt(1000)
	committed.Incarnation = 1

	ibs := NewWithVersionMap(&fieldReader{addr: addr, account: &committed}, NewVersionMap(nil))
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	require.NoError(t, ibs.SetIncarnation(addr, 7))

	_, hasIncarnation := ibs.VersionedWrites().GetIncarnation(addr)
	require.True(t, hasIncarnation, "SetIncarnation records an IncarnationPath write")
	_, hasBalance := ibs.VersionedWrites().GetBalance(addr)
	require.False(t, hasBalance, "no BalancePath write yet: this is the combination under test")

	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)

	ibs.RevertToSnapshot(snap, nil)

	vw, ok := ibs.VersionedWrites().GetIncarnation(addr)
	require.True(t, ok, "reverting the self-destruct must restore the IncarnationPath write")
	require.Equal(t, uint64(7), vw.Val)
}
