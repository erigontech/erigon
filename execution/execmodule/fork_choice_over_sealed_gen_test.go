package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

func TestForkChoicesAcrossThreeSealedBlocksNoneAcceptedReachTheLast(t *testing.T) {
	for name, order := range map[string][]int{"each": {0, 1, 2}, "skipFirst": {1, 2}, "skipMiddle": {0, 2}, "last": {2}} {
		t.Run(name, func(t *testing.T) {
			replica := newRoundHarness(t)
			proposer := newRoundHarnessWithKeys(t, replica.keys)
			parent, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
			require.NoError(t, err)
			var proposed []*types.Block
			var rounds [][][]byte
			for n := uint64(0); n < 3; n++ {
				round := [][]byte{replica.signed(0, n, nil, 100_000, emitsALog)}
				paramsP, inP := proposer.attrs(parent)
				b := proposer.sealRounds(paramsP, inP, [][][]byte{round})
				proposed, rounds, parent = append(proposed, b), append(rounds, round), b.Header()
			}
			parent, err = replica.m.ExecModule.CurrentHeader(replica.ctx)
			require.NoError(t, err)
			for i, b := range proposed {
				params, in := replica.attrs(parent)
				_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, rounds[i])
				require.NoError(t, err)
				require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
				br, err := replica.m.ExecModule.SealBlock(replica.ctx, params, false)
				require.NoError(t, err)
				require.Equal(t, b.Hash(), br.Block.Hash())
				parent = b.Header()
			}
			status, err := insertBlocks(replica.ctx, replica.m.ExecModule, proposed)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			for _, i := range order {
				ur, err := forkChoiceTo(replica.ctx, replica.m.ExecModule, proposed[i].Header())
				require.NoError(t, err)
				require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
				require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, nil))
			}
			tip, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
			require.NoError(t, err)
			require.Equal(t, proposed[2].Hash(), tip.Hash())
		})
	}
}

func TestAForkChoicePastADroppedBuildOnAnUnacceptedSealedBlockCanonicalisesAll(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	parent, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	var proposed []*types.Block
	var rounds [][][]byte
	for n := uint64(0); n < 3; n++ {
		round := [][]byte{replica.signed(0, n, nil, 100_000, emitsALog)}
		paramsP, inP := proposer.attrs(parent)
		b := proposer.sealRounds(paramsP, inP, [][][]byte{round})
		proposed, rounds, parent = append(proposed, b), append(rounds, round), b.Header()
	}

	genesis, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	params1, in1 := replica.attrs(genesis)
	_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in1, rounds[0])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	br, err := replica.m.ExecModule.SealBlock(replica.ctx, params1, false)
	require.NoError(t, err)
	require.Equal(t, proposed[0].Hash(), br.Block.Hash())
	_, in2 := replica.attrs(proposed[0].Header())
	_, _, vr, err = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in2, nil)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)

	status, err := insertBlocks(replica.ctx, replica.m.ExecModule, proposed)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	ur, err := forkChoiceTo(replica.ctx, replica.m.ExecModule, proposed[2].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
	require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, nil))
	tip, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	require.Equal(t, proposed[2].Hash(), tip.Hash())
}
