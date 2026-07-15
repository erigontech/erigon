package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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
