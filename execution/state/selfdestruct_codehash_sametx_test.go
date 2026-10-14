package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// EIP-6780: a contract self-destructed in the CURRENT tx is alive until end-of-tx
// cleanup, so EXTCODEHASH within the same tx must return its real code hash. On the
// versioned path the self-destruct clears the CodeHashPath cell (for later-tx reads),
// but the code is still present — GetCodeHash must recompute the real hash, else the
// eip1052/eip6780 EXTCODEHASH-of-self-destructed-contract zkevm fixtures store the
// empty hash and diverge (wrong BAL / state root).
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

	_, err = ibs.Selfdestruct(addr, true /* preserveBalance */)
	require.NoError(t, err)

	got, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, want, got, "same-tx EXTCODEHASH of a self-destructed contract must return the real code hash (EIP-6780 alive), not the cleared empty")
}
