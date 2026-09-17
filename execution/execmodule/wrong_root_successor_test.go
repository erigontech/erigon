package execmodule_test

import (
	"crypto/ecdsa"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

// roundSpec is one round of a block: which of the harness's two senders, and its nonce.
type roundSpec struct {
	key   int
	nonce uint64
}

// The producer seals a block of two rounds, then its successor of one round. Two independent nodes check both: one
// validates block 1, runs its fork choice, then validates block 2 (as a follower would); the other inserts both
// before validating either. Both reject block 2 with a wrong trie root in about one run in two — the producer's
// successor root is wrong, not the checkers. Present at 63bcb69999 and every commit since; unseen live because a
// producer accepts its own sealed blocks without re-executing them.
func TestKeptRoundsSealTheRealRoot(t *testing.T) {
	checkScenario(t, [][]roundSpec{{{0, 0}, {0, 1}}, {{1, 0}}})
}

// The same, with one round per block: does the wrong root need a block the producer built over several rounds?
func TestSingleRoundBlocksSealTheRealRoot(t *testing.T) {
	checkScenario(t, [][]roundSpec{{{0, 0}}, {{1, 0}}})
}

// The same as the two-round case, but the successor's transaction comes from the sender that already spent in block
// 1: does it need an account the block's own state has not touched?
func TestSameSenderSuccessorSealsTheRealRoot(t *testing.T) {
	checkScenario(t, [][]roundSpec{{{0, 0}, {0, 1}}, {{0, 2}}})
}

// checkScenario builds the blocks on a fresh producer and has two independent nodes re-execute them: one block by
// block (a follower keeping up), one inserting every block before validating any (a node syncing after the fact).
// Repeated on fresh nodes, because the damage does not show on every run.
func checkScenario(t *testing.T, blocks [][]roundSpec) {
	t.Helper()
	for run := 1; run <= 15; run++ {
		h := newRoundHarness(t)
		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)

		sealed := make([]*types.Block, 0, len(blocks))
		for i, rounds := range blocks {
			params, in := h.attrs(head)
			for j, r := range rounds {
				require.NoError(t, h.round(h.ctx, in, r.key, r.nonce), "run %d block %d round %d", run, i+1, j+1)
			}
			b := h.sealOnProducer(params, len(rounds))
			sealed = append(sealed, b)
			head = b.Header()
		}

		seq := h.independentNode()
		var errSeq error
		for _, b := range sealed {
			if errSeq = insertValidateAndUfc1By1(h.ctx, seq.ExecModule, []*types.Block{b}); errSeq != nil {
				break
			}
		}
		errBatch := insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, sealed)
		if errSeq != nil || errBatch != nil {
			t.Logf("CHECK run=%d sequential=%v batch=%v", run, errSeq, errBatch)
		}
		require.NoError(t, errSeq, "run %d: sequential checker", run)
		require.NoError(t, errBatch, "run %d: batch checker", run)
	}
}

// sealOnProducer seals the block and runs the producer's newPayload and fork choice for it.
func (h *roundHarness) sealOnProducer(params *builder.Parameters, wantTxs int) *types.Block {
	h.t.Helper()
	br, err := h.m.ExecModule.SealBlock(h.ctx, params, false)
	require.NoError(h.t, err, "the close")
	require.NotNil(h.t, br)
	require.Len(h.t, br.Block.Transactions(), wantTxs)
	hdr := br.Block.Header()
	vr, err := validateChain(h.ctx, h.m.ExecModule, hdr)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	ur, err := updateForkChoice(h.ctx, h.m.ExecModule, hdr)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, ur.Status)
	return br.Block
}

// The successor's wrong root depends on the senders' ADDRESSES, not on timing: the trie's shape follows them.
// These keys reproduce it on every run (found by trying random pairs; roughly one pair in a few fails).
func TestSuccessorSealsTheRealRootForKnownFailingKeys(t *testing.T) {
	k0, err := crypto.HexToECDSA("9a0403b2fd3dd4db2707248142663710ece5564293a95514a8579027a5efb268")
	require.NoError(t, err)
	k1, err := crypto.HexToECDSA("26759fa7c6292fda56502220980bfd4e2338bfa7546909667ef85762c2ffc30b")
	require.NoError(t, err)
	h := newRoundHarnessWithKeys(t, [2]*ecdsa.PrivateKey{k0, k1})

	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)
	var sealed []*types.Block
	for i, rounds := range [][]roundSpec{{{0, 0}, {0, 1}}, {{1, 0}}} {
		params, in := h.attrs(head)
		for j, r := range rounds {
			require.NoError(t, h.round(h.ctx, in, r.key, r.nonce), "block %d round %d", i+1, j+1)
		}
		b := h.sealOnProducer(params, len(rounds))
		sealed = append(sealed, b)
		head = b.Header()
	}
	require.NoError(t, insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, sealed),
		"an independent node re-executing the producer's blocks must accept them")
}
