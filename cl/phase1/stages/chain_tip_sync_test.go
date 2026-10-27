package stages

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client/block_collector"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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
	cfg := &Cfg{rpc: rpc.NewBeaconRpcP2P(t.Context(), sentinel, &beaconCfg, clock)}
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
		rpc:        rpc.NewBeaconRpcP2P(t.Context(), sentinel, &beaconCfg, clock),
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

func TestStartFetchingBlocksMissedByGossipReturnsAfterCancelWithoutErrorReader(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SecondsPerSlot = 1
	sentinel := &failingRequestSentinel{}
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-100, common.Hash{}, &beaconCfg)
	cfg := &Cfg{
		beaconCfg:  &beaconCfg,
		ethClock:   clock,
		forkChoice: &forkchoice.ForkChoiceStore{},
		rpc:        rpc.NewBeaconRpcP2P(t.Context(), sentinel, &beaconCfg, clock),
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1024)
	errCh := make(chan error)
	done := make(chan struct{})
	go func() {
		defer close(done)
		startFetchingBlocksMissedByGossipAfterSomeTime(ctx, cfg, Args{targetSlot: math.MaxUint64}, respCh, errCh)
	}()

	// Cancel only after a request has failed; cancelling during the initial wait returns before the error send.
	require.Eventually(t, func() bool {
		return sentinel.calls.Load() > 0
	}, 5*time.Second, 10*time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch goroutine did not return after cancellation")
	}
}

// ChainTipSync can wait a slot or longer for the next block, and validators attest to the head the beacon API serves
// meanwhile, so a fork choice head imported by ForwardSync or ChainTipSync, or one that changed when the slot started
// (proposer boost expiry), must be published first.
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
	epochsBehindAtTip := epochsBehind
	epochsBehindAtTip.atForwardSyncTip = true
	epochsBehindAtTipPublished := epochsBehindAtTip
	epochsBehindAtTipPublished.headUnpublished = false

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
		{name: "forward sync, epochs behind at tip", stage: ForwardSync, args: epochsBehindAtTip, want: ForkChoice},
		{name: "chain tip timeout, head unpublished", stage: ChainTipSync, args: unpublished, err: context.DeadlineExceeded, want: ForkChoice},
		{name: "chain tip timeout, head published", stage: ChainTipSync, args: behind, err: context.DeadlineExceeded, want: ChainTipSync},
		{name: "chain tip timeout, head published at forward sync tip", stage: ChainTipSync, args: epochsBehindAtTipPublished, err: context.DeadlineExceeded, want: ChainTipSync},
		{name: "chain tip reached the target", stage: ChainTipSync, args: atTip, want: ForkChoice},
		{name: "sleep ended at the slot start, head unpublished", stage: SleepForSlot, args: unpublished, want: ForkChoice},
		// ForkChoice may fail to publish; going straight back to ForkChoice would stop fetching blocks.
		{name: "fork choice, head still unpublished", stage: ForkChoice, args: unpublished, want: ChainTipSync},
		{name: "fork choice, epochs behind at forward sync tip", stage: ForkChoice, args: epochsBehindAtTip, want: ChainTipSync},
	}
	stages := ConsensusClStages().Stages
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, stages[tt.stage].TransitionFunc(&Cfg{}, tt.args, tt.err))
		})
	}
}

// The proposer boost expires when a slot starts, so the head can move without a block. The stage args must tick fork
// choice to the slot they target, even before the fork choice clock ticker reaches it.
func TestStageArgsTickForkChoiceToTheTargetSlot(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.GloasForkEpoch = math.MaxUint64
	const anchorSlot = 100
	genesisTime := uint64(time.Now().Unix()) - (anchorSlot+1)*beaconCfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, &beaconCfg)
	anchorState := state.New(&beaconCfg)
	anchorState.SetGenesisTime(genesisTime)
	require.NoError(t, anchorState.SetSlot(anchorSlot))
	anchorState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: anchorSlot})
	syncedData := synced_data.NewSyncedDataManager(&beaconCfg, true)
	graph, err := fork_graph.NewForkGraphDisk(anchorState, syncedData, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	store, err := forkchoice.NewForkChoiceStore(clock, anchorState, nil, pool.NewOperationsPool(&beaconCfg), graph,
		beaconevents.NewEventEmitter(), syncedData, nil, public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(), false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(anchorSlot), store.Slot())
	cfg := &Cfg{
		rpc:        rpc.NewBeaconRpcP2P(t.Context(), &statusSentinel{}, &beaconCfg, clock),
		ethClock:   clock,
		beaconCfg:  &beaconCfg,
		forkChoice: store,
		syncedData: syncedData,
	}

	args := ConsensusClStages().ArgsFunc(t.Context(), cfg)

	require.Equal(t, args.targetSlot, store.Slot())
}

func TestCfgCloseClosesBlockCollector(t *testing.T) {
	collector := block_collector.NewPersistentBlockCollector(log.New(), nil, &clparams.MainnetBeaconConfig, t.TempDir())
	require.NotNil(t, collector)
	cfg := &Cfg{blockCollector: collector}
	require.NoError(t, cfg.Close())
	block := cltypes.NewBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.ErrorContains(t, collector.AddBlock(block), "database not initialized")
}
