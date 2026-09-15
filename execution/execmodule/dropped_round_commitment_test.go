package execmodule_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
)

// A round that is dropped must leave the block's commitment exactly as it was. A round computes its commitment as
// it executes, and whatever that computation touches — the trie, the key updates, the reader it folds through — must
// be the round's own; if it is the block's, the round changes the block whether it is kept or not, and the block
// seals a root it cannot back (live46: "empty branch data read during unfold" at the close, chain stalled ~100s).
//
// The block keeps two rounds and drops one that ran in full between them. The sealed block is executed from scratch
// by an independent node, which accepts it only if the root the producer sealed is the real one. The damage does not
// show on every run, so the scenario is repeated on fresh nodes.
func TestDroppedRoundLeavesTheBlockCommitmentIntact(t *testing.T) {
	for run := 1; run <= 15; run++ {
		h := newRoundHarness(t)
		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)
		other := h.independentNode()

		params, in := h.attrs(head)
		require.NoError(t, h.round(h.ctx, in, 0, 0), "run %d round 1", run)
		lost := execmodule.WithRoundCommit(h.ctx, func() bool { return false })
		require.ErrorIs(t, h.round(lost, in, 1, 0), execmodule.ErrRoundAbandoned, "run %d round 2 is dropped", run)
		require.NoError(t, h.round(h.ctx, in, 0, 1), "run %d round 3", run)
		hdr := h.sealAndCheckElsewhere(other, params, 2, run)

		// The chain goes on from it: the dropped round's transaction lands in the next block.
		params, in = h.attrs(hdr)
		require.NoError(t, h.round(h.ctx, in, 1, 0), "run %d next block", run)
		h.sealAndAdopt(params, 1)
	}
}

// independentNode is a second node with the same genesis as the harness.
func (h *roundHarness) independentNode() *execmoduletester.ExecModuleTester {
	funds := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config: &cfg,
		Alloc: types.GenesisAlloc{
			crypto.PubkeyToAddress(h.keys[0].PublicKey): {Balance: funds},
			crypto.PubkeyToAddress(h.keys[1].PublicKey): {Balance: funds},
		},
	}
	return execmoduletester.New(h.t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(h.keys[0]))
}

// sealAndCheckElsewhere seals the block, adopts it, and has the independent node execute it from scratch.
func (h *roundHarness) sealAndCheckElsewhere(other *execmoduletester.ExecModuleTester, params *builder.Parameters, wantTxs, block int) *types.Header {
	h.t.Helper()
	br, err := h.m.ExecModule.SealBlock(h.ctx, params, false)
	require.NoError(h.t, err, "block %d: the close", block)
	require.NotNil(h.t, br)
	require.Len(h.t, br.Block.Transactions(), wantTxs)
	hdr := br.Block.Header()
	vr, err := validateChain(h.ctx, h.m.ExecModule, hdr)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	ur, err := updateForkChoice(h.ctx, h.m.ExecModule, hdr)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, ur.Status)
	require.NoError(h.t, insertValidateAndUfc1By1(h.ctx, other.ExecModule, []*types.Block{br.Block}),
		"block %d: the sealed block does not re-execute to the root it carries", block)
	return hdr
}
