package execmodule_test

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

// A block rebuilt from its own pre-executed body seals under the hash of the block it returns, typed transactions
// included: newPayload of that block is accepted as this node's own seal.
func TestABlockRebuiltFromItsTypedBodySealsUnderTheHashItReturns(t *testing.T) {
	h := newRoundHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)

	to := common.Address{0x42}
	price := uint256.NewInt(h.m.Genesis.BaseFee().Uint64())
	stx, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: 0, GasLimit: 50_000, To: &to},
		ChainID:  *h.m.ChainConfig.ChainID, TipCap: *price, FeeCap: *price,
	}, *types.LatestSignerForChainID(h.m.ChainConfig.ChainID), h.keys[0])
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, stx.MarshalBinary(&buf))

	params, in := h.attrs(head)
	_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in, [][]byte{buf.Bytes()})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)

	body, _, _, err := h.m.ExecModule.GetPreExecutedBody(h.ctx)
	require.NoError(t, err)
	require.Equal(t, [][]byte{buf.Bytes()}, body.Transactions, "the pre-executed body holds each transaction's canonical encoding")
	vr, err = h.m.ExecModule.RebuildFlashblock(h.ctx, in, body.Transactions)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	br, err := h.m.ExecModule.SealBlock(h.ctx, params, false)
	require.NoError(t, err)

	r, err := h.m.ExecModule.ValidateChain(h.ctx, br.Block)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, r.ValidationStatus, r.ValidationError)
	require.Equal(t, br.Block.Root(), r.ComputedRoot)
}
