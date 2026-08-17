package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// EIP-6780: a same-tx self-destructed contract is alive until finalize, so EXTCODEHASH must still return the real
// code hash — GetCodeHash must recompute it, since the versioned path clears the CodeHashPath cell for later-tx reads.
func TestEIP6780_SelfdestructVersioned_CodeHashSameTx(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x82474"))
	vm := NewVersionMap(nil)
	ibs := New(newAccountStateReader())
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	ibs.eip8246 = true

	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetCode(addr, []byte("contract-runtime-code"), tracing.CodeChangeUnspecified))
	want, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)

	_, err = ibs.Selfdestruct(addr, true)
	require.NoError(t, err)

	got, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, want, got, "same-tx EXTCODEHASH of a self-destructed contract must return the real code hash (EIP-6780 alive), not the cleared empty")
}
