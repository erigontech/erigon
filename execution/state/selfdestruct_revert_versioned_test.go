package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// A same-tx-created contract bumps its nonce (e.g. it CREATEs a child), then
// SELFDESTRUCTs (EIP-8246 preserve: recordWriteNonce(0) OVERWRITES the existing
// nonce versionedWrite 2->0), then the destruction is REVERTED. selfdestructChange.revert
// must restore the pre-destruct nonce (2), not leave the clobbered 0 — otherwise the
// reverted account's nonce is wrong in state root and BAL (bal_dirty_account_selfdestruct
// [...destruction_reverts]).
func TestEIP8246_SelfdestructVersioned_RevertRestoresBumpedNonce(t *testing.T) {
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
	require.Equal(t, uint64(0), n, "nonce is cleared while the self-destruct is in effect")

	ibs.RevertToSnapshot(snap, nil)

	n, err = ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(2), n, "reverting the self-destruct must restore the pre-destruct bumped nonce, not the clobbered 0")
}
