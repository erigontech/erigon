package stages

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/phase1/forkchoice"
)

func TestRememberBlockAfterProcess(t *testing.T) {
	require.True(t, rememberBlockAfterProcess(nil))
	require.True(t, rememberBlockAfterProcess(errors.New("invalid block")))
	require.False(t, rememberBlockAfterProcess(fmt.Errorf("retry parent envelope: %w", forkchoice.ErrParentEnvelopePending)))
}

// ChainTipSync can wait a slot or longer for the next block, and validators attest to the head the beacon API serves
// meanwhile, so a fork choice head imported by ForwardSync or ChainTipSync must be published first.
func TestCatchUpPublishesForkChoiceHeadBeforeChainTipSync(t *testing.T) {
	behind := Args{hasDownloaded: true, peers: 1, seenSlot: 65, seenEpoch: 2, targetSlot: 68, targetEpoch: 1}
	unpublished := behind
	unpublished.headUnpublished = true
	noPeers := unpublished
	noPeers.peers = 0
	atTip := unpublished
	atTip.seenSlot = 68
	epochsBehind := unpublished
	epochsBehind.seenSlot, epochsBehind.seenEpoch, epochsBehind.targetEpoch = 3, 0, 1

	tests := []struct {
		name  string
		stage string
		args  Args
		err   error
		want  string
	}{
		{name: "forward sync, head unpublished", stage: ForwardSync, args: unpublished, want: ForkChoice},
		{name: "forward sync, no peers, head unpublished", stage: ForwardSync, args: noPeers, want: ForkChoice},
		{name: "forward sync reached the target, head unpublished", stage: ForwardSync, args: atTip, want: ChainTipSync},
		{name: "forward sync, head published", stage: ForwardSync, args: behind, want: ChainTipSync},
		{name: "forward sync, epochs behind", stage: ForwardSync, args: epochsBehind, want: ForwardSync},
		{name: "chain tip timeout, head unpublished", stage: ChainTipSync, args: unpublished, err: context.DeadlineExceeded, want: ForkChoice},
		{name: "chain tip timeout, head published", stage: ChainTipSync, args: behind, err: context.DeadlineExceeded, want: ChainTipSync},
		{name: "chain tip reached the target", stage: ChainTipSync, args: atTip, want: ForkChoice},
		// ForkChoice may fail to publish; going straight back to ForkChoice would stop fetching blocks.
		{name: "fork choice, head still unpublished", stage: ForkChoice, args: unpublished, want: ChainTipSync},
	}
	stages := ConsensusClStages().Stages
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, stages[tt.stage].TransitionFunc(&Cfg{}, tt.args, tt.err))
		})
	}
}
