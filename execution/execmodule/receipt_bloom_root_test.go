package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

// emitsALog is creation code that emits one LOG0 and deploys nothing.
var emitsALog = common.FromHex("60006000a000")

// A block whose transactions emit logs, sealed from several rounds, carries the receipt root an independent node
// computes when it executes the block in one pass.
func TestABlockWithLogsSealsTheReceiptRootAFullExecutionComputes(t *testing.T) {
	h := newRoundHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)
	params, in := h.attrs(head)

	rounds := [][][]byte{
		{h.signed(0, 0, nil, 100_000, emitsALog), h.signed(1, 0, nil, 100_000, emitsALog)},
		{h.signed(0, 1, nil, 100_000, emitsALog)},
	}
	txs := 0
	for i, r := range rounds {
		_, _, vr, rerr := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in, r)
		require.NoError(t, rerr, "round %d", i+1)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "round %d: %s", i+1, vr.ValidationError)
		txs += len(r)
	}
	b := h.sealOnProducer(params, txs)
	require.NoError(t, insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, []*types.Block{b}),
		"an independent node executing the block accepts its receipt root")
}
