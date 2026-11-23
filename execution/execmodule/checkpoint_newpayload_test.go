package execmodule_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

// A newPayload for the height this node is pre-executing is a checkpoint: it waits for the node's own seal of that
// block rather than executing it, and the chain then carries on from the sealed block.
func TestANewPayloadForTheHeightBeingPreExecutedWaitsForItsSeal(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	head, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	first := [][][]byte{{replica.signed(0, 0, nil, 200_000, counterInit)}, {replica.signed(1, 0, nil, 200_000, counterInit)}}
	paramsP1, inP1 := proposer.attrs(head)
	proposed1 := proposer.sealRounds(paramsP1, inP1, first)
	ur, err := forkChoiceTo(proposer.ctx, proposer.m.ExecModule, proposed1.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
	second := [][][]byte{{replica.signed(0, 1, nil, 200_000, counterInit)}}
	paramsP2, inP2 := proposer.attrs(proposed1.Header())
	proposed2 := proposer.sealRounds(paramsP2, inP2, second)

	params1, in1 := replica.attrs(head)
	_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in1, first[0])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	status, err := insertBlocks(replica.ctx, replica.m.ExecModule, []*types.Block{proposed1})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	answered := make(chan execmodule.ValidationResult, 1)
	go func() {
		r, verr := validateChain(replica.ctx, replica.m.ExecModule, proposed1)
		require.NoError(t, verr)
		answered <- r
	}()
	select {
	case r := <-answered:
		t.Fatalf("newPayload answered (%s) before the pre-exec block it checkpoints was sealed", r.ValidationStatus)
	case <-time.After(300 * time.Millisecond):
	}

	_, _, vr, err = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in1, first[1])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	br, err := replica.m.ExecModule.SealBlock(replica.ctx, params1, false)
	require.NoError(t, err)
	require.Equal(t, proposed1.Hash(), br.Block.Hash(), "the replica seals the proposer's block")

	r := <-answered
	require.Equal(t, execmodule.ExecutionStatusSuccess, r.ValidationStatus, r.ValidationError)
	require.Equal(t, proposed1.Root(), r.ComputedRoot)
	ur, err = forkChoiceTo(replica.ctx, replica.m.ExecModule, proposed1.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)

	require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, []*types.Block{proposed2}))
	tip, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	require.Equal(t, proposed2.Hash(), tip.Hash())
}

// The consensus layer asks once: a newPayload waiting on a block that is rebuilt to its marker before it seals keeps
// waiting through the rebuild, and is answered with the seal, not Busy.
func TestANewPayloadWaitsThroughARebuildOfTheBlockItCheckpoints(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	head, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	marked := [][]byte{replica.signed(0, 0, nil, 200_000, counterInit)}
	paramsP, inP := proposer.attrs(head)
	proposed1 := proposer.sealRounds(paramsP, inP, [][][]byte{marked})

	params, in := replica.attrs(head)
	for _, round := range [][][]byte{marked, {replica.signed(1, 0, nil, 200_000, counterInit)}} {
		_, _, vr, rerr := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, round)
		require.NoError(t, rerr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	}
	status, err := insertBlocks(replica.ctx, replica.m.ExecModule, []*types.Block{proposed1})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	answered := make(chan execmodule.ValidationResult, 1)
	go func() {
		r, verr := replica.m.ExecModule.ValidateChain(replica.ctx, proposed1)
		require.NoError(t, verr)
		answered <- r
	}()
	time.Sleep(100 * time.Millisecond)

	vr, err := replica.m.ExecModule.RebuildFlashblock(replica.ctx, in, marked)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	br, err := replica.m.ExecModule.SealBlock(replica.ctx, params, false)
	require.NoError(t, err)
	require.Equal(t, proposed1.Hash(), br.Block.Hash())

	r := <-answered
	require.Equal(t, execmodule.ExecutionStatusSuccess, r.ValidationStatus, r.ValidationError)
	require.Equal(t, proposed1.Root(), r.ComputedRoot)
}

// A node that gives up building the block a newPayload is waiting on abandons it, and the newPayload then
// executes the agreed block itself.
func TestANewPayloadExecutesTheBlockOnceItsPreExecIsAbandoned(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	head, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	first := [][][]byte{{replica.signed(0, 0, nil, 200_000, counterInit)}, {replica.signed(1, 0, nil, 200_000, counterInit)}}
	paramsP, inP := proposer.attrs(head)
	proposed1 := proposer.sealRounds(paramsP, inP, first)

	_, in := replica.attrs(head)
	_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, first[0])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	status, err := insertBlocks(replica.ctx, replica.m.ExecModule, []*types.Block{proposed1})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	answered := make(chan execmodule.ValidationResult, 1)
	go func() {
		r, verr := replica.m.ExecModule.ValidateChain(replica.ctx, proposed1)
		require.NoError(t, verr)
		answered <- r
	}()
	select {
	case r := <-answered:
		t.Fatalf("newPayload answered (%s) while the block was still being pre-executed", r.ValidationStatus)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, replica.m.ExecModule.AbandonFlashblock(replica.ctx))
	r := <-answered
	require.Equal(t, execmodule.ExecutionStatusSuccess, r.ValidationStatus, r.ValidationError)
	ur, err := forkChoiceTo(replica.ctx, replica.m.ExecModule, proposed1.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
}

// A block newPayload executed because nothing was pre-executing it is post-executed: a pre-exec round for that height
// on the same parent is refused, so the block is never executed both ways.
func TestAPreExecRoundIsRefusedForABlockNewPayloadAlreadyExecuted(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	head, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	first := [][][]byte{{replica.signed(0, 0, nil, 200_000, counterInit)}}
	paramsP, inP := proposer.attrs(head)
	proposed1 := proposer.sealRounds(paramsP, inP, first)

	status, err := insertBlocks(replica.ctx, replica.m.ExecModule, []*types.Block{proposed1})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	vr, err := validateChain(replica.ctx, replica.m.ExecModule, proposed1)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)

	_, in := replica.attrs(head)
	_, _, _, err = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, first[0])
	require.ErrorIs(t, err, execmodule.ErrPostExecuted)

	ur, err := forkChoiceTo(replica.ctx, replica.m.ExecModule, proposed1.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
}

// Blocks the consensus layer inserts before this node seals its own copy of each: after the fork choice the chain
// reads every one of them back, bodies included, and keeps going.
func TestBlocksInsertedBeforeTheirLocalSealStayReadableThroughTheForkChoice(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	parent, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	for n := uint64(0); n < 3; n++ {
		round := [][]byte{replica.signed(0, n, nil, 200_000, counterInit), replica.signed(1, n, nil, 200_000, counterInit)}
		paramsP, inP := proposer.attrs(parent)
		proposed := proposer.sealRounds(paramsP, inP, [][][]byte{round})
		ur, ferr := forkChoiceTo(proposer.ctx, proposer.m.ExecModule, proposed.Header())
		require.NoError(t, ferr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)

		params, in := replica.attrs(parent)
		_, _, vr, rerr := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, round)
		require.NoError(t, rerr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
		status, ierr := insertBlocks(replica.ctx, replica.m.ExecModule, []*types.Block{proposed})
		require.NoError(t, ierr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, status)
		vr, rerr = replica.m.ExecModule.RebuildFlashblock(replica.ctx, in, round)
		require.NoError(t, rerr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
		br, serr := replica.m.ExecModule.SealBlock(replica.ctx, params, false)
		require.NoError(t, serr)
		require.Equal(t, proposed.Hash(), br.Block.Hash())
		r, verr := validateChain(replica.ctx, replica.m.ExecModule, proposed)
		require.NoError(t, verr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, r.ValidationStatus, r.ValidationError)
		ur, ferr = forkChoiceTo(replica.ctx, replica.m.ExecModule, proposed.Header())
		require.NoError(t, ferr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, "block %d: %s", n+1, ur.ValidationError)
		parent = proposed.Header()
	}
	tip, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	require.Equal(t, parent.Hash(), tip.Hash())
}

// A fork choice to a block this node is still pre-executing, on top of one it sealed but has not made canonical, is a
// checkpoint like newPayload: it lands once the block seals, and the chain reaches it with every block in between.
func TestAForkChoiceToABlockStillBeingPreExecutedLandsOnceItSeals(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	parent, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	var proposed []*types.Block
	var rounds [][][]byte
	for n := uint64(0); n < 2; n++ {
		round := [][]byte{replica.signed(0, n, nil, 200_000, counterInit), replica.signed(1, n, nil, 200_000, counterInit)}
		paramsP, inP := proposer.attrs(parent)
		b := proposer.sealRounds(paramsP, inP, [][][]byte{round})
		ur, ferr := forkChoiceTo(proposer.ctx, proposer.m.ExecModule, b.Header())
		require.NoError(t, ferr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
		proposed, rounds, parent = append(proposed, b), append(rounds, round), b.Header()
	}

	head, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	params1, in1 := replica.attrs(head)
	_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in1, rounds[0])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	br, err := replica.m.ExecModule.SealBlock(replica.ctx, params1, false)
	require.NoError(t, err)
	require.Equal(t, proposed[0].Hash(), br.Block.Hash())
	status, err := insertBlocks(replica.ctx, replica.m.ExecModule, proposed)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	r, err := validateChain(replica.ctx, replica.m.ExecModule, proposed[0])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, r.ValidationStatus, r.ValidationError)

	params2, in2 := replica.attrs(proposed[0].Header())
	_, _, vr, err = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in2, rounds[1])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)

	landed := make(chan execmodule.ForkChoiceResult, 1)
	landedErr := make(chan error, 1)
	go func() {
		ur, ferr := replica.m.ExecModule.UpdateForkChoice(replica.ctx, proposed[1].Hash(), common.Hash{}, common.Hash{})
		landedErr <- ferr
		landed <- ur
	}()
	time.Sleep(200 * time.Millisecond)
	br, err = replica.m.ExecModule.SealBlock(replica.ctx, params2, false)
	require.NoError(t, err)
	require.Equal(t, proposed[1].Hash(), br.Block.Hash())

	require.NoError(t, <-landedErr, "the fork choice to the block being pre-executed")
	ur := <-landed
	require.NotEqual(t, execmodule.ExecutionStatusBadBlock, ur.Status, ur.ValidationError)
	ur, err = forkChoiceTo(replica.ctx, replica.m.ExecModule, proposed[1].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
	tip, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	require.Equal(t, proposed[1].Hash(), tip.Hash())
	require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, nil))
}

// Accepting a sealed block for the canonical path while the next block opens on it: both read the block's own
// overlay, so they must not rebind its transaction under each other.
func TestAcceptingASealedBlockRunsBesideTheNextBlocksOpen(t *testing.T) {
	h := newRoundHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)
	params, in := h.attrs(head)
	b := h.sealRounds(params, in, [][][]byte{{h.signed(0, 0, nil, 100_000, emitsALog)}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 4000 {
			_, _ = h.m.ExecModule.ValidateChain(h.ctx, b)
		}
	}()
	_, in2 := h.attrs(b.Header())
	for range 2000 {
		require.NoError(t, h.m.ExecModule.AbandonFlashblock(h.ctx))
		_, _, _, _ = h.m.ExecModule.PreExecuteFlashblock(h.ctx, in2, [][]byte{h.signed(1, 0, nil, 100_000, emitsALog)})
	}
	<-done
}

// Abandoning with no block in progress leaves the sealed blocks alone: the next block still opens on the last one.
func TestAbandonWithNothingInProgressKeepsTheSealedBlock(t *testing.T) {
	h := newRoundHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)
	params, in := h.attrs(head)
	b := h.sealRounds(params, in, [][][]byte{{h.signed(0, 0, nil, 100_000, emitsALog)}})

	require.NoError(t, h.m.ExecModule.AbandonFlashblock(h.ctx))
	_, in2 := h.attrs(b.Header())
	_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in2, [][]byte{h.signed(1, 0, nil, 100_000, emitsALog)})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
}
