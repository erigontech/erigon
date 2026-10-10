package stages

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

func TestRememberBlockAfterProcess(t *testing.T) {
	require.True(t, rememberBlockAfterProcess(nil))
	require.True(t, rememberBlockAfterProcess(errors.New("invalid block")))
	require.False(t, rememberBlockAfterProcess(fmt.Errorf("retry parent envelope: %w", forkchoice.ErrParentEnvelopePending)))
}

// failingRequestSentinel fails every request, like a peer that keeps rate-limiting us.
type failingRequestSentinel struct {
	sentinelproto.SentinelClient
	calls atomic.Int64
}

func (s *failingRequestSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.calls.Add(1)
	return nil, errors.New("rate limited")
}

func (*failingRequestSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

// TestFetchBlocksFromReqRespPacesRetriesAfterRequestErrors checks that a failed block request is retried
// after a pause. Retrying at once floods the peers with requests until the stage deadline.
func TestFetchBlocksFromReqRespPacesRetriesAfterRequestErrors(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	sentinel := &failingRequestSentinel{}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &beaconCfg)
	cfg := &Cfg{rpc: rpc.NewBeaconRpcP2P(t.Context(), sentinel, &beaconCfg, clock, nil)}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	_, err := fetchBlocksFromReqResp(ctx, cfg, 100, 10)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, sentinel.calls.Load(), int64(10), "one second of failures must not turn into a request flood")
}

func TestStartFetchingBlocksMissedByGossipPacesSuccessfulPolls(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SecondsPerSlot = 1
	sentinel := &chainTipBatchEnvelopeSentinel{}
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-100, common.Hash{}, &beaconCfg)
	cfg := &Cfg{
		beaconCfg:  &beaconCfg,
		ethClock:   clock,
		forkChoice: &forkchoice.ForkChoiceStore{},
		rpc:        rpc.NewBeaconRpcP2P(t.Context(), sentinel, &beaconCfg, clock, nil),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1024)
	go func() {
		for range respCh {
		}
	}()

	startFetchingBlocksMissedByGossipAfterSomeTime(ctx, cfg, Args{targetSlot: math.MaxUint64}, respCh, make(chan error, 1))
	close(respCh)

	require.LessOrEqual(t, sentinel.calls, 3, "polls that return no new block must be paced")
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
