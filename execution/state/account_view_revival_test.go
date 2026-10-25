package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// awskii #8 (rw_v3.go:230): after a destruct + revival in earlier txs, does the
// view compose the pre-destruct nonce for a later credit? createObject records a
// fresh whole-account Address cell (nonce 0) AND a fresh nonce=0 cell on revival
// (intra_block_state.go), precisely so the version-map floor can't resurrect the
// prior incarnation's nonce. This pins that the view composes the fresh 0.
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
	vm.WriteNonce(addr, Version{TxIndex: 2}, 0, true)
	vm.WriteCodeHash(addr, Version{TxIndex: 2}, accounts.EmptyCodeHash, true)

	// A later tx composes its pre-write base from the version map.
	view := NewVersionedAccountView(addr, 3, vm, reader)

	require.EqualValues(t, 0, view.GetNonce(), "revived account must not resurrect the pre-destruct nonce")
	acc := view.Account()
	require.NotNil(t, acc)
	require.EqualValues(t, 0, acc.Nonce, "composed account nonce must be the fresh 0, not 5")
}
