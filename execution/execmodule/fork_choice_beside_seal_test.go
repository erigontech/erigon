package execmodule_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

// A follower's fork choice can trail its own seal: the next block is sealed and its newPayload taken before the
// fork choice reaches the parent.
func TestForkChoiceBehindTheSuccessorsNewPayloadCanonicalisesTheParent(t *testing.T) {
	h := newRoundHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)
	counter := types.CreateAddress(crypto.PubkeyToAddress(h.keys[0].PublicKey), 0)

	paramsA, inA := h.attrs(head)
	a := h.sealRounds(paramsA, inA, [][][]byte{{h.signed(0, 0, nil, 200_000, counterInit)}})
	paramsB, inB := h.attrs(a.Header())
	b := h.sealRounds(paramsB, inB, [][][]byte{{h.signed(0, 1, &counter, 100_000, nil)}})

	for _, blk := range []*types.Block{a, b} {
		ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, blk.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, "fork choice to block %d: %s", blk.NumberU64(), ur.ValidationError)
	}
	require.NoError(t, insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, []*types.Block{a, b}))
}

// The same, with the next block opened and filled over several rounds before the fork choice reaches its parent —
// the frontier two deep, as a producer runs when its consensus is slower than its blocks.
func TestRoundsOnAnUncanonicalParentSealTheRealRoot(t *testing.T) {
	for run := 1; run <= 5; run++ {
		h := newRoundHarness(t)
		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)
		sender := crypto.PubkeyToAddress(h.keys[0].PublicKey)
		counter := types.CreateAddress(sender, 0)

		paramsA, inA := h.attrs(head)
		blockA := [][][]byte{
			{h.signed(0, 0, nil, 200_000, counterInit)},
			{h.signed(0, 1, &counter, 100_000, nil), h.signed(1, 0, nil, 200_000, counterInit)},
			{h.signed(0, 2, &counter, 100_000, nil)},
		}
		a := h.sealRounds(paramsA, inA, blockA)

		paramsB, inB := h.attrs(a.Header())
		blockB := [][][]byte{{
			h.signed(0, 3, nil, 200_000, counterInit),
			h.signed(0, 4, &counter, 100_000, nil),
			h.signed(0, 5, nil, 200_000, counterInit),
		}}
		for n := uint64(6); n < 13; n++ {
			blockB = append(blockB, [][]byte{h.signed(0, n, &counter, 100_000, nil)})
		}
		blockB = append(blockB, [][]byte{h.signed(1, 1, &counter, 100_000, nil)})
		b := h.sealRounds(paramsB, inB, blockB)

		for _, blk := range []*types.Block{a, b} {
			ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, blk.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, "run %d fork choice to block %d: %s", run, blk.NumberU64(), ur.ValidationError)
		}
		require.NoError(t, insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, []*types.Block{a, b}),
			"run %d: an independent node re-executing the blocks must accept their roots", run)
	}
}

// A consensus client re-sends the head's fork choice before it asks for the next payload, so the seal of B can run
// beside a repeated fork choice to its already-canonical parent A.
func TestSealBesideARepeatedParentForkChoiceSealsTheRealRoot(t *testing.T) {
	sealBesideParentForkChoice(t, true)
}

// The same, with A's FIRST fork choice — the one that merges its pre-executed state — running beside B's seal.
func TestSealBesideTheParentsFirstForkChoiceSealsTheRealRoot(t *testing.T) {
	sealBesideParentForkChoice(t, false)
}

func sealBesideParentForkChoice(t *testing.T, parentCanonical bool) {
	for run := 1; run <= 15; run++ {
		h := newRoundHarness(t)
		head, err := h.m.ExecModule.CurrentHeader(h.ctx)
		require.NoError(t, err)
		sender := crypto.PubkeyToAddress(h.keys[0].PublicKey)
		counter := types.CreateAddress(sender, 0)

		paramsA, inA := h.attrs(head)
		a := h.sealRounds(paramsA, inA, [][][]byte{
			{h.signed(0, 0, nil, 200_000, counterInit)},
			{h.signed(0, 1, &counter, 100_000, nil), h.signed(1, 0, nil, 200_000, counterInit)},
		})
		if parentCanonical {
			ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, a.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status)
		}

		paramsB, inB := h.attrs(a.Header())
		roundsB := [][][]byte{{
			h.signed(0, 2, nil, 200_000, counterInit),
			h.signed(0, 3, &counter, 100_000, nil),
		}}
		for n := uint64(4); n < 8; n++ {
			roundsB = append(roundsB, [][]byte{h.signed(0, n, &counter, 100_000, nil)})
		}
		txs := 0
		for _, r := range roundsB {
			_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, inB, r)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
			txs += len(r)
		}
		again := make(chan error, 1)
		go func() {
			ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, a.Header())
			if err == nil && ur.Status != execmodule.ExecutionStatusSuccess {
				err = fmt.Errorf("%v %s", ur.Status, ur.ValidationError)
			}
			again <- err
		}()
		br, err := h.m.ExecModule.SealBlock(h.ctx, paramsB, false)
		require.NoError(t, err)
		b := br.Block
		require.Len(t, b.Transactions(), txs)
		vr, err := validateChain(h.ctx, h.m.ExecModule, b)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "newPayload: %s", vr.ValidationError)
		require.NoError(t, <-again, "run %d: fork choice to A", run)
		ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, b.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, "run %d fork choice to B: %s", run, ur.ValidationError)

		require.NoError(t, insertValidateAndUfc1By1(h.ctx, h.independentNode().ExecModule, []*types.Block{a, b}),
			"run %d: an independent node re-executing the blocks must accept their roots", run)
	}
}
