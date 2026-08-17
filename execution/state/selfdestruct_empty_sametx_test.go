package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// EIP-6780: a same-tx self-destructed contract stays alive until finalize, so Empty() must still report it non-empty
// even though the versioned path clears its balance/nonce/codehash cells.
func TestEIP6780_SelfdestructVersioned_NotEmptySameTx(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x82472"))
	vm := NewVersionMap(nil)
	ibs := New(newAccountStateReader())
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	ibs.eip8246 = true

	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetCode(addr, []byte("contract-runtime-code"), tracing.CodeChangeUnspecified))
	_, err := ibs.Selfdestruct(addr, true)
	require.NoError(t, err)

	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.False(t, empty, "a same-tx self-destructed contract is alive until finalize (EIP-6780) and must not read as empty")
}
