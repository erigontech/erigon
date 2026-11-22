package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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

// Once the abandoned block is dropped the round reads the canonical context, which newPayload rebinds under the
// module semaphore; the round holds only its own lock, so the two must not share a read transaction.
func TestARoundAfterAnAbandonedBlockRunsBesideNewPayload(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	head, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	first := [][][]byte{{replica.signed(0, 0, nil, 200_000, counterInit)}}
	paramsP, inP := proposer.attrs(head)
	proposed1 := proposer.sealRounds(paramsP, inP, first)
	ur, err := forkChoiceTo(proposer.ctx, proposer.m.ExecModule, proposed1.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
	var siblings []*types.Block
	for n := uint64(0); n < 200; n++ {
		p, in := proposer.attrs(proposed1.Header())
		p.Timestamp += n + 1
		in.Timestamp = p.Timestamp
		to := common.Address{0x42}
		siblings = append(siblings, proposer.sealRounds(p, in, [][][]byte{{replica.signed(1, 0, &to, 50_000, nil)}}))
		proposer.m.ExecModule.ResetFlashBodyForTest(0)
	}

	_, in := replica.attrs(head)
	_, _, _, err = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, first[0])
	require.NoError(t, err)
	require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, []*types.Block{proposed1}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, b := range siblings {
			_, _ = insertBlocks(replica.ctx, replica.m.ExecModule, []*types.Block{b})
			_, _ = validateChain(replica.ctx, replica.m.ExecModule, b)
		}
	}()
	_, in2 := replica.attrs(proposed1.Header())
	future := replica.signed(0, 1000, nil, 200_000, counterInit)
	for range 2000 {
		_, _, _, _ = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in2, [][]byte{future})
	}
	<-done
}

// A replica still taking rounds into its own block N while the proposer's N and N+1 are imported and canonicalised:
// the fork choice retires the replica's generation for N, which the round is executing into.
func TestARoundIntoABlockTheImportOvertakesRunsBesideItsRetirement(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	parent, err := proposer.m.ExecModule.CurrentHeader(proposer.ctx)
	require.NoError(t, err)
	to := common.Address{0x42}
	var chain []*types.Block
	for n := uint64(0); n < 60; n++ {
		p, in := proposer.attrs(parent)
		b := proposer.sealRounds(p, in, [][][]byte{{replica.signed(1, n, &to, 50_000, nil)}})
		ur, err := forkChoiceTo(proposer.ctx, proposer.m.ExecModule, b.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)
		chain = append(chain, b)
		parent = b.Header()
	}

	head, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	for i := 0; i+1 < len(chain); i += 2 {
		_, in := replica.attrs(head)
		_, _, _, err = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, [][]byte{replica.signed(0, 0, &to, 50_000, nil)})
		require.NoError(t, err)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for nonce := uint64(1); nonce < 30; nonce++ {
				_, _, _, _ = replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, [][]byte{replica.signed(0, nonce, &to, 50_000, nil)})
			}
		}()
		require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, chain[i:i+2]))
		<-done
		head = chain[i+1].Header()
	}
}
