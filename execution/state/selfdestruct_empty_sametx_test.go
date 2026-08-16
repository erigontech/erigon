package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// EIP-6780: a contract self-destructed in the CURRENT tx stays alive until
// end-of-tx cleanup, so Empty() must report it as NON-empty within the same tx
// (it had code — it executed SELFDESTRUCT). On the versioned path the SD clears
// the nonce/code-hash/balance cells, so emptyFromVersionedFields wrongly returns
// empty=true -> a same-tx CALL to it charges params.StateGasNewAccount (183600),
// over-charging gas (the eip8246/eip6780 post_send_opcode_CALL zkevm failures).
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
	_, err := ibs.Selfdestruct(addr, true /* preserveBalance */)
	require.NoError(t, err)

	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.False(t, empty, "a same-tx self-destructed contract is alive until finalize (EIP-6780) and must not read as empty")
}
