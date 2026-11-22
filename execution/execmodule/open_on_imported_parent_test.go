package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

// A replica part-way through its own block N takes the proposer's N by newPayload and fork choice, then builds N+1 on
// it: N+1 executes on the imported N, not on what the abandoned N had pre-executed.
func TestABlockOpenedOnAnImportedParentDoesNotSeeTheAbandonedPreExec(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	head, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	first := [][][]byte{{replica.signed(0, 0, nil, 200_000, counterInit)}, {replica.signed(1, 0, nil, 200_000, counterInit)}}
	paramsP, inP := proposer.attrs(head)
	proposed1 := proposer.sealRounds(paramsP, inP, first)
	ur, err := forkChoiceTo(proposer.ctx, proposer.m.ExecModule, proposed1.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)

	_, in := replica.attrs(head)
	_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, first[0])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, []*types.Block{proposed1}))

	second := [][][]byte{{replica.signed(0, 1, nil, 200_000, counterInit), replica.signed(1, 1, nil, 200_000, counterInit)}}
	paramsP2, inP2 := proposer.attrs(proposed1.Header())
	proposed2 := proposer.sealRounds(paramsP2, inP2, second)

	params2, in2 := replica.attrs(proposed1.Header())
	_, _, vr, err = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in2, second[0])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	br, err := replica.m.ExecModule.SealBlock(replica.ctx, params2, false)
	require.NoError(t, err)
	require.Len(t, br.Block.Transactions(), 2)
	require.Equal(t, proposed2.Hash(), br.Block.Hash(), "the replica builds the proposer's next block")
}

// The same when the replica had also sealed its own, different, block below the one it abandons: the next block reads
// the imported chain, not the replica's sealed block.
func TestABlockOpenedOnAnImportedChainDoesNotSeeTheReplicasOwnSealedFork(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	genesis, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	paramsR1, inR1 := replica.attrs(genesis)
	own1 := replica.sealRounds(paramsR1, inR1, [][][]byte{{replica.signed(0, 0, nil, 200_000, counterInit)}})

	paramsP1, inP1 := proposer.attrs(genesis)
	proposed1 := proposer.sealRounds(paramsP1, inP1, [][][]byte{{replica.signed(0, 0, nil, 200_000, counterInit), replica.signed(1, 0, nil, 200_000, counterInit)}})
	paramsP2, inP2 := proposer.attrs(proposed1.Header())
	proposed2 := proposer.sealRounds(paramsP2, inP2, [][][]byte{{replica.signed(0, 1, nil, 200_000, counterInit)}})

	_, inR2 := replica.attrs(own1.Header())
	_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, inR2, [][]byte{replica.signed(0, 1, nil, 200_000, counterInit)})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, []*types.Block{proposed1, proposed2}))

	third := [][][]byte{{replica.signed(1, 1, nil, 200_000, counterInit)}}
	paramsP3, inP3 := proposer.attrs(proposed2.Header())
	proposed3 := proposer.sealRounds(paramsP3, inP3, third)

	params3, in3 := replica.attrs(proposed2.Header())
	_, _, vr, err = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in3, third[0])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	require.Len(t, vr.RoundKeptHashes, 1)
	br, err := replica.m.ExecModule.SealBlock(replica.ctx, params3, false)
	require.NoError(t, err)
	require.Equal(t, proposed3.Hash(), br.Block.Hash(), "the replica builds the proposer's next block")
}
