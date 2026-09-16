package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
)

// A block whose WITHDRAWALS are re-stamped after its body has executed must still seal the state that body
// produced.
//
// The CL can deliver a withdrawals list that differs from the one the block was opened under. Alone among the
// payload attributes withdrawals do not affect transaction execution — they credit accounts at block-end,
// after every transaction has run — so reconcileForAssembleLocked RE-STAMPS the open block rather than
// abandoning its generation and re-executing the body (block_building.go: executionAttrsMatchParams is true
// exactly when the execution-affecting attrs match AND the withdrawals differ).
//
// Measured on live48, that re-stamp is a near-necessary precondition for a block that seals a body its own
// state transition never applied: 643 blocks were re-stamped, 451 of them non-empty, and 378 of those raised
// "[BODY-AUDIT] state nonce disagrees with the sealed body" — while only 2 audited blocks were NOT re-stamped.
// On block 216 the round executed the transaction (root 0x5a101c76, receipts=1, gasUsed=2268597), the
// withdrawals were re-stamped, and the sealed block then carried that transaction in its body and receipts
// while the canonical nonce for its sender stayed at 93 and the sender's balance was byte-identical to the
// previous block's. The block-end DID apply — the first withdrawal recipient's balance rose across it — so the
// block sealed as "parent state + block-end" with a body describing work that never happened.
//
// The consequence is a duplicate transaction: unapplied and still genuinely pending, it is re-delivered and
// sealed again into the next block that takes transactions (20 duplicated hashes over 5,270 blocks). A
// duplicate key then makes the transactions snapshot index unbuildable and recsplit retries forever.
//
// The assertion here is the property that actually matters and the one the production symptom violates: an
// independent node must be able to re-execute the sealed block to the root it carries. A block whose body did
// not apply cannot.
func TestRestampedWithdrawalsKeepTheBodysState(t *testing.T) {
	// Repeated: the divergence did not show on every live block that was re-stamped (73 of 451 non-empty
	// re-stamps stayed clean), so a single run is not evidence of absence.
	for run := 1; run <= 10; run++ {
		h := newRoundHarness(t)
		other := h.independentNode()

		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)

		// Opened, and the body executed, under an EMPTY withdrawals list.
		params, in := h.attrs(head)
		require.NoError(t, h.round(h.ctx, in, 0, 0), "run %d: the round", run)

		// The CL now asks for the same block under a DIFFERENT withdrawals list. Every execution-affecting
		// attribute is unchanged, so this is the re-stamp path and not a re-open.
		params.Withdrawals = []*types.Withdrawal{{
			Index:     1,
			Validator: 1,
			Address:   crypto.PubkeyToAddress(h.keys[1].PublicKey),
			Amount:    1,
		}}

		h.sealAndCheckElsewhere(other, params, 1, 1)
	}
}

// CONTROL for the test above: the same block, sealed under the withdrawals it was OPENED with, must
// re-execute on an independent node.
//
// Without this the test above proves nothing about the re-stamp. The producer's sealed root is separately
// suspect — wrong_root_successor_test.go builds blocks with no withdrawals change at all and its independent
// checkers reject them in roughly one run in two — so a failure that reproduces with or without the
// withdrawals change is that defect, not this one. This control has to pass repeatedly for the re-stamp to be
// the cause rather than the occasion.
func TestUnrestampedBlockKeepsTheBodysState(t *testing.T) {
	for run := 1; run <= 10; run++ {
		h := newRoundHarness(t)
		other := h.independentNode()

		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)

		params, in := h.attrs(head)
		require.NoError(t, h.round(h.ctx, in, 0, 0), "run %d: the round", run)

		// Sealed under the SAME withdrawals list the block was opened with: no re-stamp, no re-open.
		h.sealAndCheckElsewhere(other, params, 1, 1)
	}
}

// Does the defect need the EMPTY -> non-empty transition, or just a change? Live blocks are opened under a
// real list and re-stamped to a different real list, so this is the closer analogue of production.
func TestRestampedBetweenTwoNonEmptyWithdrawalListsKeepsTheBodysState(t *testing.T) {
	for run := 1; run <= 10; run++ {
		h := newRoundHarness(t)
		other := h.independentNode()

		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)

		params, in := h.attrs(head)
		// Opened under a real list, on BOTH the params and the inputs the round executes under.
		opened := []*types.Withdrawal{{
			Index: 1, Validator: 1,
			Address: crypto.PubkeyToAddress(h.keys[1].PublicKey), Amount: 1,
		}}
		params.Withdrawals = opened
		in.Withdrawals = opened

		require.NoError(t, h.round(h.ctx, in, 0, 0), "run %d: the round", run)

		// The CL now asks for a DIFFERENT real list.
		params.Withdrawals = []*types.Withdrawal{{
			Index: 2, Validator: 1,
			Address: crypto.PubkeyToAddress(h.keys[1].PublicKey), Amount: 2,
		}}

		h.sealAndCheckElsewhere(other, params, 1, 1)
	}
}

// Is the RE-STAMP actually the discriminator, or just the circumstance?
//
// TestUnrestampedBlockKeepsTheBodysState seals under an EMPTY withdrawals list, so its block-end credits
// nothing — measured, its close leaves the SD root exactly where the round left it. That control therefore
// removes withdrawal crediting as well as the re-stamp, and cannot separate the two. This one opens AND seals
// under the SAME non-empty list: withdrawals are credited at the close, but nothing is re-stamped.
//
// If this fails too, the fault is in crediting withdrawals at the close and the re-stamp is only what made it
// visible; if it passes, the re-stamp is genuinely causal.
func TestWithdrawalsCreditedWithoutARestampKeepTheBodysState(t *testing.T) {
	for run := 1; run <= 10; run++ {
		h := newRoundHarness(t)
		other := h.independentNode()

		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)

		params, in := h.attrs(head)
		credited := []*types.Withdrawal{{
			Index: 1, Validator: 1,
			Address: crypto.PubkeyToAddress(h.keys[1].PublicKey), Amount: 1,
		}}
		params.Withdrawals = credited
		in.Withdrawals = credited

		require.NoError(t, h.round(h.ctx, in, 0, 0), "run %d: the round", run)

		// Sealed under the SAME list it was opened with: no re-stamp, but a block-end that credits.
		h.sealAndCheckElsewhere(other, params, 1, 1)
	}
}

// Does it need a BODY? An empty block is re-stamped the same way (192 of the 643 live re-stamps were empty),
// and if an empty one also fails to re-execute then the fault is not about the body at all.
func TestRestampedEmptyBlockKeepsItsState(t *testing.T) {
	for run := 1; run <= 10; run++ {
		h := newRoundHarness(t)
		other := h.independentNode()

		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)

		// Open the block with no round at all, then re-stamp its withdrawals.
		params, _ := h.attrs(head)
		params.Withdrawals = []*types.Withdrawal{{
			Index: 1, Validator: 1,
			Address: crypto.PubkeyToAddress(h.keys[1].PublicKey), Amount: 1,
		}}

		h.sealAndCheckElsewhere(other, params, 0, 1)
	}
}
