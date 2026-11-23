package execmodule_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
