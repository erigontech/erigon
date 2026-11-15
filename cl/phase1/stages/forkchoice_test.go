package stages

import (
	"bytes"
	"context"
	"errors"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type pausingHeadView struct {
	synced_data.SyncedData
	entered chan struct{}
	release chan struct{}
}

type recordingHeadPublish struct {
	synced_data.SyncedData
	copyCalls  int
	ownedCalls int
	ownedState *state.CachingBeaconState
	ownedRoot  common.Hash
}

func (s *recordingHeadPublish) OnHeadStateWithBlockRoot(newState *state.CachingBeaconState, blockRoot common.Hash) error {
	s.copyCalls++
	return s.SyncedData.OnHeadStateWithBlockRoot(newState, blockRoot)
}

func (s *recordingHeadPublish) OnOwnedHeadStateWithBlockRoot(newState *state.CachingBeaconState, blockRoot common.Hash) error {
	s.ownedCalls++
	s.ownedState = newState
	s.ownedRoot = blockRoot
	return s.SyncedData.OnOwnedHeadStateWithBlockRoot(newState, blockRoot)
}

func (s *pausingHeadView) ViewHeadState(fn synced_data.ViewHeadStateFn) error {
	return s.SyncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		close(s.entered)
		<-s.release
		return fn(headState)
	})
}

type statusSentinel struct {
	sentinelproto.SentinelClient
}

func (*statusSentinel) SetStatus(context.Context, *sentinelproto.Status, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	return &sentinelproto.EmptyMessage{}, nil
}

func (*statusSentinel) GetPeers(context.Context, *sentinelproto.EmptyMessage, ...grpc.CallOption) (*sentinelproto.PeerCount, error) {
	return &sentinelproto.PeerCount{Active: 1}, nil
}

func (*statusSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

type noOpAttestationDataProducer struct {
	attestation_producer.AttestationDataProducer
}

func (noOpAttestationDataProducer) ProduceAndCacheAttestationData(*state.CachingBeaconState, common.Hash, uint64) (solid.AttestationData, error) {
	return solid.AttestationData{}, nil
}

func (noOpAttestationDataProducer) CachedAttestationData(common.Hash, uint64) (solid.AttestationData, bool) {
	return solid.AttestationData{}, false
}

type countingAttestationDataProducer struct {
	attestation_producer.AttestationDataProducer
	produceCalls atomic.Int32
}

func (p *countingAttestationDataProducer) ProduceAndCacheAttestationData(baseState *state.CachingBeaconState, baseStateBlockRoot common.Hash, slot uint64) (solid.AttestationData, error) {
	p.produceCalls.Add(1)
	return p.AttestationDataProducer.ProduceAndCacheAttestationData(baseState, baseStateBlockRoot, slot)
}

type lockOrderFixture struct {
	cfg        *Cfg
	syncedData *synced_data.SyncedDataManager
	headState  *state.CachingBeaconState
	headRoot   common.Hash
	view       *pausingHeadView
}

type blockingStateCopyForkGraph struct {
	fork_graph.ForkGraph
	armed                    atomic.Bool
	blocked                  atomic.Bool
	released                 atomic.Bool
	hideCurrentView          bool
	currentViewStateOverride *state.CachingBeaconState
	ownedCopies              atomic.Int32
	started                  chan struct{}
	release                  chan struct{}
}

func (g *blockingStateCopyForkGraph) CurrentStateLocked(root common.Hash) *state.CachingBeaconState {
	current := g.ForkGraph.CurrentStateLocked(root)
	pcs := make([]uintptr, 16)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(1, pcs)])
	viewCurrent := false
	getOwned := false
	for {
		frame, more := frames.Next()
		viewCurrent = viewCurrent || strings.HasSuffix(frame.Function, ".ViewCurrentStateAtBlockRoot")
		getOwned = getOwned || strings.HasSuffix(frame.Function, ".GetOwnedStateAtBlockRoot")
		if !more {
			break
		}
	}
	if getOwned && current != nil {
		g.ownedCopies.Add(1)
	}
	if !g.armed.Load() {
		return current
	}
	if viewCurrent {
		if g.hideCurrentView {
			return nil
		}
		if g.currentViewStateOverride != nil {
			return g.currentViewStateOverride
		}
	}
	if !getOwned || g.blocked.Swap(true) {
		return current
	}
	close(g.started)
	<-g.release
	return current
}

func (g *blockingStateCopyForkGraph) releaseCopy() {
	if g.released.CompareAndSwap(false, true) {
		close(g.release)
	}
}

func newLockOrderFixture(t *testing.T, slot uint64) lockOrderFixture {
	return newLockOrderFixtureWithConfig(t, slot, nil, common.Hash{}, false, nil)
}

func newLockOrderFixtureWithConfig(t *testing.T, slot uint64, engine execution_client.ExecutionEngine, executionHash common.Hash, gloas bool, wrapGraph func(fork_graph.ForkGraph) fork_graph.ForkGraph) lockOrderFixture {
	t.Helper()
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.GloasForkEpoch = math.MaxUint64
	if gloas {
		beaconCfg.AltairForkEpoch = 0
		beaconCfg.BellatrixForkEpoch = 0
		beaconCfg.CapellaForkEpoch = 0
		beaconCfg.DenebForkEpoch = 0
		beaconCfg.ElectraForkEpoch = 0
		beaconCfg.FuluForkEpoch = 0
		beaconCfg.GloasForkEpoch = 0
		beaconCfg.InitializeForkSchedule()
	}
	genesisTime := uint64(time.Now().Unix()) - slot*beaconCfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, &beaconCfg)
	headState := state.New(&beaconCfg)
	headState.SetGenesisTime(genesisTime)
	require.NoError(t, headState.SetSlot(slot))
	headState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: slot})
	if gloas {
		headState.SetVersion(clparams.GloasVersion)
	}
	if executionHash != (common.Hash{}) {
		if gloas {
			headState.SetLatestBlockHash(executionHash)
			headState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
				Slot:            slot,
				ParentBlockHash: executionHash,
			})
		} else {
			headState.SetVersion(clparams.FuluVersion)
			header := cltypes.NewEth1Header(clparams.FuluVersion)
			header.BlockHash = executionHash
			headState.SetLatestExecutionPayloadHeader(header)
		}
	}

	manager := synced_data.NewSyncedDataManager(&beaconCfg, true)
	emitter := beaconevents.NewEventEmitter()
	graph, err := fork_graph.NewForkGraphDisk(
		headState,
		manager,
		afero.NewMemMapFs(),
		beacon_router_configuration.RouterConfiguration{},
	)
	require.NoError(t, err)
	if wrapGraph != nil {
		graph = wrapGraph(graph)
	}
	store, err := forkchoice.NewForkChoiceStore(
		clock,
		headState,
		engine,
		pool.NewOperationsPool(&beaconCfg),
		graph,
		emitter,
		manager,
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)
	headRoot := graph.AnchorRoot()
	require.NoError(t, manager.OnHeadStateWithBlockRoot(headState, headRoot))

	viewEntered := make(chan struct{})
	releaseView := make(chan struct{})
	pausedView := &pausingHeadView{
		SyncedData: manager,
		entered:    viewEntered,
		release:    releaseView,
	}
	return lockOrderFixture{
		cfg: &Cfg{
			rpc:        rpc.NewBeaconRpcP2P(t.Context(), &statusSentinel{}, &beaconCfg, clock),
			ethClock:   clock,
			beaconCfg:  &beaconCfg,
			forkChoice: store,
			syncedData: pausedView,
			emitter:    emitter,
		},
		syncedData: manager,
		headState:  headState,
		headRoot:   headRoot,
		view:       pausedView,
	}
}

func startPostForkchoiceWithBlockedCopy(t *testing.T, blocker *blockingStateCopyForkGraph, configure func(lockOrderFixture)) (lockOrderFixture, <-chan *beaconevents.EventStream, <-chan error) {
	t.Helper()
	fixture := newLockOrderFixtureWithConfig(t, 1, nil, common.Hash{}, false, func(graph fork_graph.ForkGraph) fork_graph.ForkGraph {
		blocker.ForkGraph = graph
		return blocker
	})
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = noOpAttestationDataProducer{}
	fixture.cfg.dirs = datadir.New(t.TempDir())
	fixture.cfg.syncedData = fixture.syncedData
	if configure != nil {
		configure(fixture)
	}
	events := make(chan *beaconevents.EventStream, 3)
	subscription := fixture.cfg.emitter.State().Subscribe(events)
	t.Cleanup(subscription.Unsubscribe)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	blocker.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		done <- postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot)
		close(done)
	}()
	t.Cleanup(func() {
		blocker.releaseCopy()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("post-forkchoice operation did not stop")
		}
	})
	select {
	case <-blocker.started:
	case <-time.After(time.Second):
		t.Fatal("head-state copy did not start")
	}
	return fixture, events, done
}

func receiveStateEvent(t *testing.T, events <-chan *beaconevents.EventStream) *beaconevents.EventStream {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("state event was not emitted")
		return nil
	}
}

func requireNoHeadEvents(t *testing.T, events <-chan *beaconevents.EventStream) {
	t.Helper()
	for {
		select {
		case event := <-events:
			require.NotEqual(t, beaconevents.StateHead, event.Event)
			require.NotEqual(t, beaconevents.StateHeadV2, event.Event)
		default:
			return
		}
	}
}

func TestPostForkchoiceOperationsEmitsHeadEventsBeforeStateCopy(t *testing.T) {
	blocker := &blockingStateCopyForkGraph{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	fixture, events, done := startPostForkchoiceWithBlockedCopy(t, blocker, nil)
	stateRoot, err := fixture.headState.HashSSZ()
	require.NoError(t, err)
	var legacy *beaconevents.HeadData
	var v2 *beaconevents.HeadV2Data
	for range 2 {
		select {
		case event := <-events:
			switch event.Event {
			case beaconevents.StateHead:
				legacy = event.Data.(*beaconevents.HeadData)
			case beaconevents.StateHeadV2:
				v2 = event.Data.(*beaconevents.HeadV2Data)
			}
		case <-time.After(time.Second):
			t.Fatal("head events were not emitted before the head-state copy completed")
		}
	}
	require.Equal(t, &beaconevents.HeadData{
		Slot:                      1,
		Block:                     fixture.headRoot,
		State:                     stateRoot,
		EpochTransition:           false,
		PreviousDutyDependentRoot: common.Hash{},
		CurrentDutyDependentRoot:  common.Hash{},
		ExecutionOptimistic:       false,
	}, legacy)
	require.Equal(t, &beaconevents.HeadV2Data{
		Version: clparams.Phase0Version.String(),
		Data: beaconevents.HeadV2Content{
			Slot:                      1,
			Block:                     fixture.headRoot,
			State:                     stateRoot,
			PayloadStatus:             "full",
			EpochTransition:           false,
			CurrentEpochDependentRoot: common.Hash{},
			NextEpochDependentRoot:    common.Hash{},
			ExecutionOptimistic:       false,
		},
	}, v2)

	blocker.releaseCopy()
	require.NoError(t, <-done)
	requireNoHeadEvents(t, events)
}

func TestHeadEventBeforeStateCopyMatchesCopiedStateEvent(t *testing.T) {
	for _, test := range []struct {
		name              string
		gloasForkEpoch    uint64
		stateVersion      clparams.StateVersion
		headSlot          uint64
		wantPayloadStatus string
	}{
		{name: "pre-Gloas", gloasForkEpoch: 2, stateVersion: clparams.FuluVersion, headSlot: 31, wantPayloadStatus: "full"},
		{name: "Gloas state with pre-Gloas head slot", gloasForkEpoch: 1, stateVersion: clparams.GloasVersion, headSlot: 31, wantPayloadStatus: "full"},
		{name: "first Gloas head slot", gloasForkEpoch: 1, stateVersion: clparams.GloasVersion, headSlot: 32, wantPayloadStatus: "empty"},
		{name: "after first Gloas head slot", gloasForkEpoch: 1, stateVersion: clparams.GloasVersion, headSlot: 33, wantPayloadStatus: "empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			beaconCfg := clparams.MainnetBeaconConfig
			beaconCfg.AltairForkEpoch = 0
			beaconCfg.BellatrixForkEpoch = 0
			beaconCfg.CapellaForkEpoch = 0
			beaconCfg.DenebForkEpoch = 0
			beaconCfg.ElectraForkEpoch = 0
			beaconCfg.FuluForkEpoch = 0
			beaconCfg.GloasForkEpoch = test.gloasForkEpoch
			beaconCfg.InitializeForkSchedule()
			stateSlot := test.headSlot + 1
			genesisTime := uint64(time.Now().Unix()) - (stateSlot+1)*beaconCfg.SecondsPerSlot
			clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, &beaconCfg)
			anchorState := state.New(&beaconCfg)
			anchorState.SetGenesisTime(genesisTime)
			anchorState.SetVersion(test.stateVersion)
			require.NoError(t, anchorState.SetSlot(stateSlot))
			anchorState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: test.headSlot, Root: common.Hash{0xaa}})
			manager := synced_data.NewSyncedDataManager(&beaconCfg, true)
			graph, err := fork_graph.NewForkGraphDisk(anchorState, manager, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
			require.NoError(t, err)
			store, err := forkchoice.NewForkChoiceStore(
				clock,
				anchorState,
				nil,
				pool.NewOperationsPool(&beaconCfg),
				graph,
				beaconevents.NewEventEmitter(),
				manager,
				nil,
				public_keys_registry.NewInMemoryPublicKeysRegistry(),
				validator_params.NewValidatorParams(),
				false,
				nil,
			)
			require.NoError(t, err)
			head, headSlot, err := store.GetHeadNode()
			require.NoError(t, err)
			require.Equal(t, test.headSlot, headSlot)
			earlyEmitter := beaconevents.NewEventEmitter()
			earlyEvents := make(chan *beaconevents.EventStream, 2)
			earlySubscription := earlyEmitter.State().Subscribe(earlyEvents)
			defer earlySubscription.Unsubscribe()
			oldEmitter := beaconevents.NewEventEmitter()
			oldEvents := make(chan *beaconevents.EventStream, 2)
			oldSubscription := oldEmitter.State().Subscribe(oldEvents)
			defer oldSubscription.Unsubscribe()

			emitted := emitHeadEventBeforeStateCopy(&beaconCfg, store, earlyEmitter, headSlot, head.Root)
			require.True(t, emitted)
			copied, err := store.GetOwnedStateAtBlockRoot(head.Root)
			require.NoError(t, err)
			require.NoError(t, emitHeadEvent(&beaconCfg, store, oldEmitter, headSlot, head.Root, copied))

			require.Equal(t, receiveStateEvent(t, oldEvents), receiveStateEvent(t, earlyEvents))
			oldV2 := receiveStateEvent(t, oldEvents)
			earlyV2 := receiveStateEvent(t, earlyEvents)
			require.Equal(t, test.wantPayloadStatus, oldV2.Data.(*beaconevents.HeadV2Data).Data.PayloadStatus)
			require.Equal(t, oldV2, earlyV2)
		})
	}
}

func TestPostForkchoiceOperationsFallsBackToCopiedState(t *testing.T) {
	for _, test := range []struct {
		name              string
		hideCurrentView   bool
		zeroPreviousState bool
	}{
		{name: "head root is not current state", hideCurrentView: true},
		{name: "previous state root is zero", zeroPreviousState: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			blocker := &blockingStateCopyForkGraph{
				hideCurrentView: test.hideCurrentView,
				started:         make(chan struct{}),
				release:         make(chan struct{}),
			}
			var configure func(lockOrderFixture)
			if test.zeroPreviousState {
				configure = func(fixture lockOrderFixture) {
					viewState, err := fixture.headState.Copy()
					require.NoError(t, err)
					viewState.SetPreviousStateRoot(common.Hash{})
					blocker.currentViewStateOverride = viewState
				}
			}
			_, events, done := startPostForkchoiceWithBlockedCopy(t, blocker, configure)
			require.Empty(t, events)

			blocker.releaseCopy()
			require.NoError(t, <-done)
			require.Equal(t, beaconevents.StateHead, receiveStateEvent(t, events).Event)
			require.Equal(t, beaconevents.StateHeadV2, receiveStateEvent(t, events).Event)
			requireNoHeadEvents(t, events)
		})
	}
}

func TestHeadEventBeforeStateCopyDropsHeadChangedBeforeSend(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	headRoot := common.Hash{1}
	headState := state.New(&beaconCfg)
	require.NoError(t, headState.SetSlot(1))
	stateRoot, err := headState.HashSSZ()
	require.NoError(t, err)
	headState.SetPreviousStateRoot(stateRoot)
	store := &mock_services.ForkChoiceStorageMock{
		HeadVal:             headRoot,
		HeadSlotVal:         1,
		StateAtBlockRootVal: map[common.Hash]*state.CachingBeaconState{headRoot: headState},
	}
	store.ViewCurrentStateAtBlockRootFn = func(root common.Hash, fn func(*state.CachingBeaconState) error) error {
		err := fn(headState)
		store.HeadVal = common.Hash{2}
		return err
	}
	emitter := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 2)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()

	emitted := emitHeadEventBeforeStateCopy(&beaconCfg, store, emitter, 1, headRoot)
	require.False(t, emitted)
	require.Empty(t, events)
}

func requireLockOrderOperationCompletes(t *testing.T, fixture lockOrderFixture, operationDone <-chan error) {
	t.Helper()
	select {
	case err := <-operationDone:
		// An operation that never takes the synced-data lock cannot invert the order.
		require.NoError(t, err)
		return
	case <-fixture.view.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("operation neither completed nor entered the synced head-state view")
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- fixture.syncedData.OnHeadStateWithBlockRoot(fixture.headState, fixture.headRoot)
	}()
	time.Sleep(250 * time.Millisecond)

	slashingDone := make(chan error, 1)
	go func() {
		slashingDone <- fixture.cfg.forkChoice.OnAttesterSlashing(cltypes.NewAttesterSlashing(clparams.Phase0Version), true)
	}()
	select {
	case err := <-slashingDone:
		t.Fatalf("lock-order setup failed: attester slashing completed before the queued writer: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	close(fixture.view.release)
	select {
	case err := <-operationDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("operation deadlocked: synced-data read lock waited for fork choice while fork choice waited behind a queued synced-data writer")
	}
	require.NoError(t, <-writerDone)
	<-slashingDone
}

func TestComputeForkChoiceDoesNotInvertSyncedDataLockOrder(t *testing.T) {
	fixture := newLockOrderFixture(t, 0)
	done := make(chan error, 1)
	go func() {
		_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), fixture.cfg)
		done <- err
	}()

	requireLockOrderOperationCompletes(t, fixture, done)
}

func TestComputeForkChoiceFallsBackWithoutSyncedHeadState(t *testing.T) {
	fixture := newLockOrderFixture(t, 0)
	fixture.syncedData.UnsetHeadState()

	headSlot, headRoot, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), fixture.cfg)

	require.NoError(t, err)
	require.Equal(t, fixture.headState.Slot(), headSlot)
	require.Equal(t, fixture.headRoot, headRoot)
}

func TestComputeForkChoiceSendsPreGloasExecutionHeadEveryPass(t *testing.T) {
	executionHash := common.Hash{0x41}
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	fixture := newLockOrderFixtureWithConfig(t, 0, engine, executionHash, false, nil)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), executionHash, executionHash, executionHash, nil, clparams.Phase0Version).
		Return(nil, nil).Times(2)

	_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), fixture.cfg)
	require.NoError(t, err)
	require.Equal(t, executionHash, fixture.cfg.executionHead)
	fixture.cfg.forkChoice.OnTick(utils.SecondsToMilliseconds(fixture.cfg.ethClock.GenesisTime() + fixture.cfg.beaconCfg.SecondsPerSlot))

	_, _, err = computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), fixture.cfg)

	require.NoError(t, err)
	require.Equal(t, executionHash, fixture.cfg.executionHead)
}

func TestComputeForkChoiceRecordsExecutionHeadWithoutEngine(t *testing.T) {
	executionHash := common.Hash{0x41}
	fixture := newLockOrderFixtureWithConfig(t, 0, nil, executionHash, false, nil)

	_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), fixture.cfg)

	require.NoError(t, err)
	require.Equal(t, executionHash, fixture.cfg.executionHead)
}

func newGloasCanonicalForkChoiceFixture(t *testing.T, full bool) (*Cfg, *chainTipBatchForkGraph, common.Hash, *cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope, *testExecutionEngine) {
	t.Helper()
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusNotValidated, false)
	headBlock := graph.parents[headRoot]
	envelope := graph.envelopes[headRoot]
	delete(graph.parents, headRoot)
	if !full {
		delete(graph.envelopes, headRoot)
	}
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	if full {
		cfg.forkChoice.MarkPayloadStatus(headRoot, envelope.Message.Payload.BlockHash, execution_client.PayloadStatusNotValidated)
	}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GenesisTime().Return(uint64(0)).AnyTimes()
	clock.EXPECT().GetCurrentSlot().Return(headBlock.Block.Slot).AnyTimes()
	clock.EXPECT().GetCurrentEpoch().Return(headBlock.Block.Slot / cfg.beaconCfg.SlotsPerEpoch).AnyTimes()
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{}, nil).AnyTimes()
	clock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.GloasVersion).AnyTimes()
	cfg.ethClock = clock
	cfg.rpc = rpc.NewBeaconRpcP2P(t.Context(), &statusSentinel{}, cfg.beaconCfg, clock)
	return cfg, graph, headRoot, headBlock, envelope, engine
}

func TestComputeForkChoiceSendsGloasFullExecutionHeadWithoutVerification(t *testing.T) {
	cfg, _, headRoot, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, true)
	bid := headBlock.Block.Body.GetSignedExecutionPayloadBid().Message
	require.False(t, cfg.forkChoice.IsPayloadVerified(headRoot))

	headSlot, returnedRoot, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)

	require.NoError(t, err)
	require.Equal(t, headRoot, returnedRoot)
	require.Equal(t, headBlock.Block.Slot, headSlot)
	require.Equal(t, []common.Hash{bid.BlockHash}, engine.forkChoiceHeads)
	require.Equal(t, bid.BlockHash, cfg.executionHead)
}

func TestComputeForkChoiceSendsGloasEmptyExecutionHead(t *testing.T) {
	cfg, _, _, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, false)
	bid := headBlock.Block.Body.GetSignedExecutionPayloadBid().Message

	_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)

	require.NoError(t, err)
	require.Equal(t, []common.Hash{bid.ParentBlockHash}, engine.forkChoiceHeads)
	require.Equal(t, bid.ParentBlockHash, cfg.executionHead)
}

func TestComputeForkChoiceFallsBackForUnresolvedGloasHead(t *testing.T) {
	executionHash := common.Hash{0x41}
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	fixture := newLockOrderFixtureWithConfig(t, 0, engine, executionHash, true, nil)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), executionHash, executionHash, executionHash, nil, clparams.GloasVersion).
		Return(nil, nil)

	_, computedRoot, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), fixture.cfg)

	require.NoError(t, err)
	require.Equal(t, fixture.headRoot, computedRoot)
	require.Equal(t, executionHash, fixture.cfg.executionHead)
}

func TestComputeForkChoiceRecordsGloasExecutionHeadAfterTimeout(t *testing.T) {
	cfg, _, headRoot, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, true)
	bid := headBlock.Block.Body.GetSignedExecutionPayloadBid().Message
	previousWake := sleepForSlotWake{root: headRoot, executionHead: bid.ParentBlockHash, at: time.Now()}
	cfg.sleepForSlotLastWake = previousWake
	engine.forkChoiceErr = execution_client.ErrForkChoiceUpdateTimeout

	_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)

	require.NoError(t, err)
	require.Equal(t, bid.BlockHash, cfg.executionHead)
	previousWake.executionHead = bid.BlockHash
	require.Equal(t, previousWake, cfg.sleepForSlotLastWake)
}

type busyOnceExecutionModule struct {
	execmodule.ExecutionModule
	forkChoiceCalls int
}

func (m *busyOnceExecutionModule) UpdateForkChoice(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
	m.forkChoiceCalls++
	if m.forkChoiceCalls == 1 {
		return execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusBusy}, nil
	}
	return execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusSuccess}, nil
}

func TestGloasForkChoiceRejectedAsBusyIsResent(t *testing.T) {
	cfg, _, headRoot, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, true)
	bid := headBlock.Block.Body.GetSignedExecutionPayloadBid().Message
	module := &busyOnceExecutionModule{}
	direct, err := execution_client.NewExecutionClientDirect(chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil)
	require.NoError(t, err)
	engine.forkChoiceFn = direct.ForkChoiceUpdate

	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		previousExecutionHead := cfg.executionHead
		cfg.sleepForSlotLastWake = sleepForSlotWake{root: headRoot, executionHead: bid.ParentBlockHash, at: started}
		_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)
		require.NoError(t, err)
		require.Equal(t, 1, module.forkChoiceCalls)
		require.Equal(t, previousExecutionHead, cfg.executionHead, "a busy execution module did not apply the head")
		require.Equal(t, bid.BlockHash, cfg.sleepForSlotLastWake.executionHead)
		require.Equal(t, started, cfg.sleepForSlotLastWake.at)

		wake, changed, err := waitForNextSlotOrHeadChange(
			t.Context(), headBlock.Block.Slot+1, cfg.beaconCfg, cfg.forkChoice,
			sleepForSlotSyncedDataFake{head: headRoot},
			sleepForSlotClockFake{currentSlot: headBlock.Block.Slot, nextSlot: started.Add(2 * sleepForSlotRewakeInterval)},
			cfg.sleepForSlotLastWake, cfg.executionHead,
		)
		require.NoError(t, err)
		require.True(t, changed, "the unapplied execution head must wake fork choice before the next slot")
		require.Equal(t, bid.BlockHash, wake.executionHead)
		require.GreaterOrEqual(t, time.Since(started), sleepForSlotRewakeInterval, "the re-send must stay throttled")

		_, _, err = computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)
		require.NoError(t, err)
		require.Equal(t, 2, module.forkChoiceCalls)
		require.Equal(t, bid.BlockHash, cfg.executionHead)
	})
}

func TestComputeForkChoiceKeepsGloasExecutionHeadAfterFatalError(t *testing.T) {
	cfg, _, headRoot, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, true)
	bid := headBlock.Block.Body.GetSignedExecutionPayloadBid().Message
	previousHash := common.Hash{0x42}
	previousWake := sleepForSlotWake{root: headRoot, executionHead: bid.ParentBlockHash, at: time.Now()}
	wantErr := errors.New("fork choice failed")
	cfg.executionHead = previousHash
	cfg.sleepForSlotLastWake = previousWake
	engine.forkChoiceErr = wantErr

	_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, previousHash, cfg.executionHead)
	require.Equal(t, previousWake, cfg.sleepForSlotLastWake)
}

func TestGloasExecutionHeadChangeBypassesThrottleAfterDifferentStageDecision(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stageFull bool
	}{
		{"FULL wake, EMPTY stage, FULL poll", false},
		{"EMPTY wake, FULL stage, EMPTY poll", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _, headRoot, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, tc.stageFull)
			bid := headBlock.Block.Body.GetSignedExecutionPayloadBid().Message
			wakeHead, stageHead := bid.BlockHash, bid.ParentBlockHash
			if tc.stageFull {
				wakeHead, stageHead = stageHead, wakeHead
			}

			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				cfg.sleepForSlotLastWake = sleepForSlotWake{root: headRoot, executionHead: wakeHead, at: started}
				_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)
				require.NoError(t, err)
				require.Equal(t, []common.Hash{stageHead}, engine.forkChoiceHeads)
				require.Equal(t, stageHead, cfg.executionHead)
				require.Equal(t, headRoot, cfg.sleepForSlotLastWake.root)
				require.Equal(t, started, cfg.sleepForSlotLastWake.at)

				ready := true
				wake, changed, err := waitForNextSlotOrHeadChange(
					t.Context(), headBlock.Block.Slot+1, cfg.beaconCfg,
					sleepForSlotForkChoiceFake{head: headRoot, executionHead: wakeHead, ready: &ready},
					sleepForSlotSyncedDataFake{head: headRoot},
					sleepForSlotClockFake{currentSlot: headBlock.Block.Slot, nextSlot: started.Add(2 * sleepForSlotRewakeInterval)},
					cfg.sleepForSlotLastWake, cfg.executionHead,
				)
				require.NoError(t, err)
				require.True(t, changed)
				require.Equal(t, wakeHead, wake.executionHead)
				require.Equal(t, sleepForSlotHeadPollInterval, time.Since(started), "the next poll must compare with the stage's decision, not the triggering wake")
			})
		})
	}
}

func TestGloasExecutionHeadChangeWakesAndSendsFullHash(t *testing.T) {
	cfg, graph, headRoot, headBlock, envelope, engine := newGloasCanonicalForkChoiceFixture(t, false)
	bid := headBlock.Block.Body.GetSignedExecutionPayloadBid().Message

	_, _, err := computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)
	require.NoError(t, err)
	require.Equal(t, bid.ParentBlockHash, cfg.executionHead)
	require.NoError(t, graph.DumpEnvelopeOnDisk(headRoot, envelope))
	cfg.forkChoice.MarkPayloadStatus(headRoot, bid.BlockHash, execution_client.PayloadStatusNotValidated)

	wake, changed, err := waitForNextSlotOrHeadChange(
		t.Context(), headBlock.Block.Slot+1, sleepForSlotConfig(0), cfg.forkChoice,
		sleepForSlotSyncedDataFake{head: headRoot},
		sleepForSlotClockFake{currentEpoch: 0, currentSlot: headBlock.Block.Slot, nextSlot: time.Now().Add(5 * sleepForSlotHeadPollInterval)},
		sleepForSlotWake{}, cfg.executionHead,
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, bid.BlockHash, wake.executionHead)

	engine.forkChoiceHeads = nil
	_, _, err = computeAndNotifyServicesOfNewForkChoice(t.Context(), log.New(), cfg)
	require.NoError(t, err)
	require.Equal(t, []common.Hash{bid.BlockHash}, engine.forkChoiceHeads)
	require.Equal(t, bid.BlockHash, cfg.executionHead)
}

func TestPostForkchoiceOperationsDoesNotInvertSyncedDataLockOrder(t *testing.T) {
	fixture := newLockOrderFixture(t, clparams.MainnetBeaconConfig.SlotsPerEpoch*5)
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = noOpAttestationDataProducer{}
	fixture.cfg.dirs = datadir.New(t.TempDir())
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	done := make(chan error, 1)
	go func() {
		done <- postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot)
	}()

	requireLockOrderOperationCompletes(t, fixture, done)
	_, err = checkpoint_sync.ReadLocalFinalizedState(fixture.cfg.dirs, fixture.cfg.beaconCfg)
	require.NoError(t, err)
}

func TestPostForkchoiceOperationsPublishesOwnedHeadState(t *testing.T) {
	fixture := newLockOrderFixture(t, 1)
	fixture.cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	fixture.cfg.attestationDataProducer = noOpAttestationDataProducer{}
	fixture.cfg.dirs = datadir.New(t.TempDir())
	recorder := &recordingHeadPublish{SyncedData: fixture.syncedData}
	fixture.cfg.syncedData = recorder
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	require.NoError(t, postForkchoiceOperations(tx, log.New(), fixture.cfg, fixture.headState.Slot(), fixture.headRoot))

	require.Equal(t, 1, recorder.ownedCalls)
	require.Zero(t, recorder.copyCalls)
	require.Equal(t, fixture.headRoot, recorder.ownedRoot)
	require.NoError(t, fixture.cfg.forkChoice.ViewStateAtBlockRoot(fixture.headRoot, func(current *state.CachingBeaconState) error {
		require.NotSame(t, current, recorder.ownedState)
		return nil
	}))
	require.NoError(t, fixture.syncedData.ViewHeadState(func(head *state.CachingBeaconState) error {
		require.Same(t, recorder.ownedState, head)
		return nil
	}))
}

func newForkChoicePublicationFixture(t *testing.T) (*Cfg, *cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	cfg, _, block, _ := newForwardSyncPersistenceFixture(t)
	cfg.syncedData = cfg.syncedData.(*pausingHeadView).SyncedData
	cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	cfg.attestationDataProducer = noOpAttestationDataProducer{}
	cfg.dirs = datadir.New(t.TempDir())
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), block, false, true, false))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	head, _, err := cfg.forkChoice.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), head.Root)
	return cfg, block, root
}

func TestForkChoiceWakesOnIndexCommit(t *testing.T) {
	for _, delay := range []time.Duration{20 * time.Millisecond, 250 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg, block, root := newForkChoicePublicationFixture(t)
				require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, Args{}))
				require.True(t, cfg.forkChoiceWaitingForIndices)
				written := make(chan error, 1)
				go func() {
					time.Sleep(delay)
					written <- persistAcceptedBlock(t.Context(), cfg, block)
				}()

				started := time.Now()
				err := ConsensusClStages().Stages[SleepForSlot].ActionFunc(t.Context(), log.New(), cfg, Args{})
				elapsed := time.Since(started)

				require.NoError(t, <-written)
				require.NoError(t, err)
				require.Equal(t, delay, elapsed, "a committed write should wake publication without waiting for a timer")
				require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, Args{}))
				require.Equal(t, root, cfg.syncedData.HeadRoot())
				require.False(t, cfg.forkChoiceWaitingForIndices)
				require.NoError(t, cfg.indiciesDB.View(t.Context(), func(tx kv.Tx) error {
					canonical, err := beacon_indicies.ReadCanonicalBlockRoot(tx, block.Block.Slot)
					require.NoError(t, err)
					require.Equal(t, root, canonical)
					return nil
				}))
			})
		})
	}
}

func TestForkChoiceWaitsForHeadIndicesBeforePublishing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, block, root := newForkChoicePublicationFixture(t)
		parentRoot := cfg.syncedData.HeadRoot()
		require.Less(t, block.Version(), clparams.GloasVersion)
		events := make(chan *beaconevents.EventStream, 16)
		subscription := cfg.emitter.State().Subscribe(events)
		defer subscription.Unsubscribe()
		args := Args{hasDownloaded: true, peers: 1, seenSlot: block.Block.Slot, targetSlot: block.Block.Slot, headUnpublished: true}

		started := time.Now()
		require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, args), "missing indices are an expected wait")
		require.Zero(t, time.Since(started), "checking missing indices must not poll or sleep")
		require.Equal(t, parentRoot, cfg.syncedData.HeadRoot())
		require.Empty(t, events, "do not publish the head before its indices exist")

		stages := ConsensusClStages().Stages
		cfg.forkChoiceRerunAt = time.Now()
		require.Equal(t, CleanupAndPruning, stages[ForkChoice].TransitionFunc(cfg, args, nil))
		require.Equal(t, SleepForSlot, stages[CleanupAndPruning].TransitionFunc(cfg, args, nil))
		require.NoError(t, persistAcceptedBlock(t.Context(), cfg, block))
		ctx, cancel := context.WithTimeout(t.Context(), 2*sleepForSlotRewakeInterval)
		defer cancel()
		started = time.Now()
		require.NoError(t, stages[SleepForSlot].ActionFunc(ctx, log.New(), cfg, args), "retry before the next slot, including before Gloas")
		require.Zero(t, time.Since(started), "a commit between checking the indices and waiting must not be missed")
		require.Equal(t, ForkChoice, stages[SleepForSlot].TransitionFunc(cfg, args, nil))
		require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, args))
		require.Equal(t, root, cfg.syncedData.HeadRoot())
		require.NotEmpty(t, events)
		require.NoError(t, cfg.indiciesDB.View(t.Context(), func(tx kv.Tx) error {
			canonical, err := beacon_indicies.ReadCanonicalBlockRoot(tx, block.Block.Slot)
			require.NoError(t, err)
			require.Equal(t, root, canonical)
			return nil
		}))
	})
}

func TestForkChoiceCancelsHeadIndexWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, _, _ := newForkChoicePublicationFixture(t)
		parentRoot := cfg.syncedData.HeadRoot()
		require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, Args{}))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		started := time.Now()
		err := ConsensusClStages().Stages[SleepForSlot].ActionFunc(ctx, log.New(), cfg, Args{})
		elapsed := time.Since(started)
		<-ctx.Done()

		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 20*time.Millisecond, elapsed)
		require.Equal(t, parentRoot, cfg.syncedData.HeadRoot())
	})
}

func TestForkChoiceIndexWaitKeepsRetryDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, _, _ := newForkChoicePublicationFixture(t)
		require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, Args{}))
		started := time.Now()
		stages := ConsensusClStages().Stages
		require.NoError(t, stages[SleepForSlot].ActionFunc(t.Context(), log.New(), cfg, Args{}))
		require.Equal(t, time.Second, time.Since(started))
		require.Equal(t, ForkChoice, stages[SleepForSlot].TransitionFunc(cfg, Args{}, nil))
		require.Equal(t, CleanupAndPruning, stages[ForkChoice].TransitionFunc(cfg, Args{}, nil))
	})
}

type canonicalRoots struct {
	roots []common.Hash
	err   error
}

func readCanonicalRootsOnAnotherGoroutine(ctx context.Context, db kv.RoDB, slots ...uint64) canonicalRoots {
	done := make(chan canonicalRoots, 1)
	go func() {
		var res canonicalRoots
		res.err = db.View(ctx, func(tx kv.Tx) error {
			for _, slot := range slots {
				root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
				if err != nil {
					return err
				}
				res.roots = append(res.roots, root)
			}
			return nil
		})
		done <- res
	}()
	select {
	case res := <-done:
		return res
	case <-time.After(5 * time.Second):
		return canonicalRoots{err: errors.New("the read did not finish")}
	}
}

type branchFixture struct {
	cfg        *Cfg
	block      *cltypes.SignedBeaconBlock
	branchRoot common.Hash
	forkSlot   uint64
}

func newBranchOnCanonicalAnchorFixture(t *testing.T) branchFixture {
	t.Helper()
	cfg, block, branchRoot := newForkChoicePublicationFixture(t)
	forkSlot := block.Block.Slot - 1
	require.NoError(t, persistAcceptedBlock(t.Context(), cfg, block))
	require.NoError(t, cfg.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		writeTestBlockIndices(t, tx, block.Block.ParentRoot, common.Hash{}, common.Hash{0xa0}, forkSlot, true)
		return nil
	}))
	return branchFixture{cfg: cfg, block: block, branchRoot: branchRoot, forkSlot: forkSlot}
}

func newReorgOntoTwoBlockBranchFixture(t *testing.T) (f branchFixture, oldTipRoot common.Hash) {
	t.Helper()
	cfg, block, branchRoot := newForkChoicePublicationFixture(t)
	require.NoError(t, persistAcceptedBlock(t.Context(), cfg, block))
	f = branchFixture{cfg: cfg, block: block, branchRoot: branchRoot, forkSlot: block.Block.Slot - 2}
	forkRoot, branchParentRoot := common.Hash{0xa0}, common.Hash{0xa1}
	oldRoot1, oldRoot2, oldTipRoot := common.Hash{0xaa, 1}, common.Hash{0xaa, 2}, common.Hash{0xaa, 3}
	require.NoError(t, cfg.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		writeTestBlockIndices(t, tx, forkRoot, common.Hash{}, common.Hash{0xa2}, f.forkSlot, true)
		writeTestBlockIndices(t, tx, branchParentRoot, forkRoot, common.Hash{0xa3}, f.forkSlot+1, false)
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, branchRoot, branchParentRoot))
		writeTestBlockIndices(t, tx, oldRoot1, forkRoot, common.Hash{0xa4}, f.forkSlot+1, true)
		writeTestBlockIndices(t, tx, oldRoot2, oldRoot1, common.Hash{0xa5}, f.forkSlot+2, true)
		writeTestBlockIndices(t, tx, oldTipRoot, oldRoot2, common.Hash{0xa6}, f.forkSlot+3, true)
		return nil
	}))
	return f, oldTipRoot
}

func TestForkchoiceRoutineBackfillsTheNewCanonicalBranchAfterCommit(t *testing.T) {
	f, _ := newReorgOntoTwoBlockBranchFixture(t)
	headSlot := f.block.Block.Slot
	ctrl := gomock.NewController(t)
	peerDas := das_mock_services.NewMockPeerDas(ctrl)
	seenByCall := make(chan canonicalRoots, 1)
	peerDas.EXPECT().BackfillCustodyColumns(f.forkSlot, headSlot).Times(1).DoAndReturn(func(uint64, uint64) {
		seenByCall <- readCanonicalRootsOnAnotherGoroutine(t.Context(), f.cfg.indiciesDB, headSlot, headSlot+1)
	})
	f.cfg.peerDas = peerDas

	require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), f.cfg, Args{}))
	ctrl.Finish()

	seen := <-seenByCall
	require.NoError(t, seen.err)
	require.Equal(t, []common.Hash{f.branchRoot, {}}, seen.roots, "the backfill must be requested after the new canonical chain is committed")
}

func TestForkchoiceRoutineBackfillsAReorgWhenTheHeadNotificationFails(t *testing.T) {
	f, _ := newReorgOntoTwoBlockBranchFixture(t)
	wantErr := errors.New("execution layer rejected the head")
	f.cfg.executionClient.(*testExecutionEngine).forkChoiceErr = wantErr
	f.cfg.forkChoice.MarkPayloadStatus(f.branchRoot, f.block.Block.Body.ExecutionPayload.BlockHash, execution_client.PayloadStatusValidated)
	peerDas := das_mock_services.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().BackfillCustodyColumns(f.forkSlot, f.block.Block.Slot).Times(1)
	f.cfg.peerDas = peerDas

	err := doForkchoiceRoutine(t.Context(), log.New(), f.cfg, Args{})

	require.ErrorIs(t, err, wantErr)
	requireCanonicalRoot(t, f.cfg.indiciesDB, f.block.Block.Slot, f.branchRoot)
}

func TestForkchoiceRoutineDoesNotBackfillOnChainExtension(t *testing.T) {
	f := newBranchOnCanonicalAnchorFixture(t)
	f.cfg.peerDas = das_mock_services.NewMockPeerDas(gomock.NewController(t))

	require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), f.cfg, Args{}))

	requireCanonicalRoot(t, f.cfg.indiciesDB, f.block.Block.Slot, f.branchRoot)
}

func requireCanonicalRoot(t *testing.T, db kv.RoDB, slot uint64, want common.Hash) {
	t.Helper()
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		require.NoError(t, err)
		require.Equal(t, want, root)
		return nil
	}))
}

type failingStateRootReadTx struct {
	kv.RwTx
	root common.Hash
	err  error
}

func (tx failingStateRootReadTx) GetOne(table string, key []byte) ([]byte, error) {
	if table == kv.BlockRootToStateRoot && bytes.Equal(key, tx.root[:]) {
		return nil, tx.err
	}
	return tx.RwTx.GetOne(table, key)
}

func TestUpdateCanonicalChainReportsAReorgWhenTheEventCannotBeBuilt(t *testing.T) {
	for _, failOldTip := range []bool{true, false} {
		name := "new head state root"
		if failOldTip {
			name = "old head state root"
		}
		t.Run(name, func(t *testing.T) {
			f, oldTipRoot := newReorgOntoTwoBlockBranchFixture(t)
			failingRoot := f.branchRoot
			if failOldTip {
				failingRoot = oldTipRoot
			}

			require.NoError(t, f.cfg.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
				faultyTx := failingStateRootReadTx{RwTx: tx, root: failingRoot, err: errors.New("state root read failed")}
				forkSlot, reorged, err := updateCanonicalChainInTheDatabase(faultyTx, f.block.Block.Slot, f.branchRoot, f.cfg)
				require.NoError(t, err)
				require.True(t, reorged)
				require.Equal(t, f.forkSlot, forkSlot)
				root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, f.block.Block.Slot)
				require.NoError(t, err)
				require.Equal(t, f.branchRoot, root, "the canonical chain was changed")
				return nil
			}))
		})
	}
}

func TestPayloadAttributesStageOwnershipAtGloas(t *testing.T) {
	config := clparams.MainnetBeaconConfig
	config.AltairForkEpoch = 0
	config.BellatrixForkEpoch = 0
	config.CapellaForkEpoch = 0
	config.DenebForkEpoch = 0
	config.ElectraForkEpoch = 0
	config.FuluForkEpoch = 0
	config.GloasForkEpoch = 1
	config.InitializeForkSchedule()
	firstGloasSlot := config.GloasForkEpoch * config.SlotsPerEpoch
	for _, test := range []struct {
		name        string
		headSlot    uint64
		headVersion clparams.StateVersion
		wantEvents  int
	}{
		{name: "before boundary", headSlot: firstGloasSlot - 2, headVersion: clparams.FuluVersion, wantEvents: 1},
		{name: "at boundary", headSlot: firstGloasSlot - 1, headVersion: clparams.FuluVersion},
		{name: "after boundary", headSlot: firstGloasSlot, headVersion: clparams.GloasVersion},
	} {
		t.Run(test.name, func(t *testing.T) {
			headState := state.New(&config)
			headState.SetVersion(test.headVersion)
			require.NoError(t, headState.SetSlot(test.headSlot))
			emitter := beaconevents.NewEventEmitter()
			events := make(chan *beaconevents.EventStream, 1)
			subscription := emitter.State().Subscribe(events)
			defer subscription.Unsubscribe()
			cfg := &Cfg{
				beaconCfg: &config,
				ethClock:  eth_clock.NewEthereumClock(0, common.Hash{}, &config),
				emitter:   emitter,
			}

			require.NoError(t, emitNextPaylodAttributesEvent(cfg, test.headSlot, common.Hash{}, headState))
			require.Len(t, events, test.wantEvents)
		})
	}
}

func TestUpdateCanonicalChainReorgEvent(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx := mdbxtest.BeginRw(t, db)

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}

	state100 := common.Hash{0xa0}
	state101a := common.Hash{0xa1}
	state102a := common.Hash{0xa2}
	state101b := common.Hash{0xb1}
	state102b := common.Hash{0xb2}

	writeTestBlockIndices(t, tx, root100, common.Hash{0x99}, state100, 100, true)
	writeTestBlockIndices(t, tx, root101a, root100, state101a, 101, true)
	writeTestBlockIndices(t, tx, root102a, root101a, state102a, 102, true)

	writeTestBlockIndices(t, tx, root101b, root100, state101b, 101, false)
	writeTestBlockIndices(t, tx, root102b, root101b, state102b, 102, false)

	reorg := drainReorgEvent(t, tx, 102, root102b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(102), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(2), reorg.Depth, "reorg Depth should be oldHeadSlot - forkPointSlot")
	require.Equal(t, root102a, reorg.OldHeadBlock, "OldHeadBlock should be the previous canonical tip")
	require.Equal(t, root102b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state102a, reorg.OldHeadState, "OldHeadState should match old canonical tip's state root")
	require.Equal(t, state102b, reorg.NewHeadState, "NewHeadState")
}

func TestUpdateCanonicalChainReorgShorterFork(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx := mdbxtest.BeginRw(t, db)

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root103a := common.Hash{0x13, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}

	state100 := common.Hash{0xa0}
	state103a := common.Hash{0xa3}
	state102b := common.Hash{0xb2}

	writeTestBlockIndices(t, tx, root100, common.Hash{0x99}, state100, 100, true)
	writeTestBlockIndices(t, tx, root101a, root100, common.Hash{0xa1}, 101, true)
	writeTestBlockIndices(t, tx, root102a, root101a, common.Hash{0xa2}, 102, true)
	writeTestBlockIndices(t, tx, root103a, root102a, state103a, 103, true)

	writeTestBlockIndices(t, tx, root101b, root100, common.Hash{0xb1}, 101, false)
	writeTestBlockIndices(t, tx, root102b, root101b, state102b, 102, false)

	reorg := drainReorgEvent(t, tx, 102, root102b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(102), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(3), reorg.Depth, "reorg Depth: old tip 103 - fork point 100 = 3")
	require.Equal(t, root103a, reorg.OldHeadBlock, "OldHeadBlock must be the actual old tip at slot 103, not 102a")
	require.Equal(t, root102b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state103a, reorg.OldHeadState, "OldHeadState must match old tip's state root")
	require.Equal(t, state102b, reorg.NewHeadState, "NewHeadState")
}

func TestUpdateCanonicalChainReorgLongerFork(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx := mdbxtest.BeginRw(t, db)

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}
	root103b := common.Hash{0x13, 0xbb}

	state102a := common.Hash{0xa2}
	state103b := common.Hash{0xb3}

	writeTestBlockIndices(t, tx, root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeTestBlockIndices(t, tx, root101a, root100, common.Hash{0xa1}, 101, true)
	writeTestBlockIndices(t, tx, root102a, root101a, state102a, 102, true)

	writeTestBlockIndices(t, tx, root101b, root100, common.Hash{0xb1}, 101, false)
	writeTestBlockIndices(t, tx, root102b, root101b, common.Hash{0xb2}, 102, false)
	writeTestBlockIndices(t, tx, root103b, root102b, state103b, 103, false)

	reorg := drainReorgEvent(t, tx, 103, root103b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(103), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(2), reorg.Depth, "reorg Depth: old tip 102 - fork point 100 = 2")
	require.Equal(t, root102a, reorg.OldHeadBlock, "OldHeadBlock must be the old tip at slot 102")
	require.Equal(t, root103b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state102a, reorg.OldHeadState, "OldHeadState")
	require.Equal(t, state103b, reorg.NewHeadState, "NewHeadState")
}

func TestUpdateCanonicalChainNoReorg(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx := mdbxtest.BeginRw(t, db)

	root100 := common.Hash{0x10}
	root101 := common.Hash{0x11}
	root102 := common.Hash{0x12}

	writeTestBlockIndices(t, tx, root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeTestBlockIndices(t, tx, root101, root100, common.Hash{0xa1}, 101, true)

	writeTestBlockIndices(t, tx, root102, root101, common.Hash{0xa2}, 102, false)

	reorg := drainReorgEvent(t, tx, 102, root102)
	require.Nil(t, reorg, "chain extension should NOT emit a chain_reorg event")
}

func TestUpdateCanonicalChainReorgOneSlot(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx := mdbxtest.BeginRw(t, db)

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root101b := common.Hash{0x11, 0xbb}

	state101a := common.Hash{0xa1}
	state101b := common.Hash{0xb1}

	writeTestBlockIndices(t, tx, root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeTestBlockIndices(t, tx, root101a, root100, state101a, 101, true)
	writeTestBlockIndices(t, tx, root101b, root100, state101b, 101, false)

	reorg := drainReorgEvent(t, tx, 101, root101b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(101), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(1), reorg.Depth, "reorg Depth: old tip 101 - fork point 100 = 1")
	require.Equal(t, root101a, reorg.OldHeadBlock, "OldHeadBlock must be root_101a")
	require.Equal(t, root101b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state101a, reorg.OldHeadState, "OldHeadState")
	require.Equal(t, state101b, reorg.NewHeadState, "NewHeadState")
}

func TestUpdateCanonicalChainWaitsForBlockIndices(t *testing.T) {
	for _, pendingSlot := range []uint64{102, 101} {
		name := "head"
		if pendingSlot == 101 {
			name = "ancestor"
		}
		t.Run(name, func(t *testing.T) {
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			fixture := newLockOrderFixture(t, 100)
			cfg := fixture.cfg
			root100 := fixture.headRoot
			root101a := common.Hash{0x11, 0xaa}
			root102a := common.Hash{0x12, 0xaa}
			root101b := common.Hash{0x11, 0xbb}
			root102b := common.Hash{0x12, 0xbb}

			requireCanonical := func(root101, root102 common.Hash) {
				t.Helper()
				require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
					for slot, want := range map[uint64]common.Hash{100: root100, 101: root101, 102: root102} {
						root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
						require.NoError(t, err)
						require.Equal(t, want, root, "canonical root at slot %d", slot)
					}
					return nil
				}))
			}
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				writeTestBlockIndices(t, tx, root100, common.Hash{}, common.Hash{100}, 100, true)
				writeTestBlockIndices(t, tx, root101a, root100, common.Hash{101}, 101, true)
				writeTestBlockIndices(t, tx, root102a, root101a, common.Hash{102}, 102, true)
				if pendingSlot != 101 {
					writeTestBlockIndices(t, tx, root101b, root100, common.Hash{101}, 101, false)
				}
				if pendingSlot != 102 {
					writeTestBlockIndices(t, tx, root102b, root101b, common.Hash{102}, 102, false)
				}
				return nil
			}))

			err := db.Update(t.Context(), func(tx kv.RwTx) error {
				_, _, err := updateCanonicalChainInTheDatabase(tx, 102, root102b, cfg)
				return err
			})
			require.ErrorIs(t, err, errBlockIndicesPending, "canonical publication must wait for the pending block write")
			requireCanonical(root101a, root102a)

			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				if pendingSlot == 101 {
					writeTestBlockIndices(t, tx, root101b, root100, common.Hash{101}, 101, false)
				} else {
					writeTestBlockIndices(t, tx, root102b, root101b, common.Hash{102}, 102, false)
				}
				return nil
			}))
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				_, _, err := updateCanonicalChainInTheDatabase(tx, 102, root102b, cfg)
				return err
			}))
			requireCanonical(root101b, root102b)
		})
	}
}

func TestUpdateCanonicalChainFromAnchorWithoutIndices(t *testing.T) {
	for _, anchorSlot := range []uint64{0, 64} {
		name := "genesis"
		if anchorSlot != 0 {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newLockOrderFixture(t, anchorSlot)
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			tx := mdbxtest.BeginRw(t, db)

			_, _, err := updateCanonicalChainInTheDatabase(tx, anchorSlot, fixture.headRoot, fixture.cfg)
			require.NoError(t, err)
			root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, anchorSlot)
			require.NoError(t, err)
			require.Equal(t, fixture.headRoot, root)

			oldRoot, newRoot := common.Hash{0xaa}, common.Hash{0xbb}
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, anchorSlot+1, oldRoot))
			require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, newRoot, anchorSlot+2))
			require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, newRoot, fixture.headRoot))
			_, _, err = updateCanonicalChainInTheDatabase(tx, anchorSlot+2, newRoot, fixture.cfg)
			require.NoError(t, err)
			root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, anchorSlot+1)
			require.NoError(t, err)
			require.Equal(t, common.Hash{}, root, "the skipped slot must no longer be canonical")
			root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, anchorSlot+2)
			require.NoError(t, err)
			require.Equal(t, newRoot, root)
		})
	}
}

func writeTestBlockIndices(t *testing.T, tx kv.RwTx, root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
	t.Helper()
	require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
	require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
	require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
	if canonical {
		require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
	}
}

func drainReorgEvent(t *testing.T, tx kv.RwTx, headSlot uint64, headRoot common.Hash) *beaconevents.ChainReorgData {
	t.Helper()
	cfg := newLockOrderFixture(t, 0).cfg
	ch := make(chan *beaconevents.EventStream, 16)
	sub := cfg.emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()

	_, _, err := updateCanonicalChainInTheDatabase(tx, headSlot, headRoot, cfg)
	require.NoError(t, err)

	for {
		select {
		case evt := <-ch:
			if evt.Event == beaconevents.StateChainReorg {
				return evt.Data.(*beaconevents.ChainReorgData)
			}
		default:
			return nil
		}
	}
}

type invalidatingEventHeadStore struct {
	*mock_services.ForkChoiceStorageMock
	reads           int
	invalidateAfter int
}

func (s *invalidatingEventHeadStore) readHead() (forkchoice.ForkChoiceNode, uint64, error) {
	s.reads++
	s.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	head := forkchoice.ForkChoiceNode{Root: s.HeadVal, PayloadStatus: s.HeadPayloadStatusVal}
	// Invalidation after a head read must not change the status returned with that root.
	if s.reads == s.invalidateAfter {
		s.HeadPayloadStatusVal = cltypes.PayloadStatusPending
	}
	return head, s.HeadSlotVal, nil
}

func (s *invalidatingEventHeadStore) GetHead(*state.CachingBeaconState) (common.Hash, uint64, error) {
	head, slot, err := s.readHead()
	return head.Root, slot, err
}

func (s *invalidatingEventHeadStore) GetHeadNode() (forkchoice.ForkChoiceNode, uint64, error) {
	return s.readHead()
}

func TestEmitHeadEventKeepsPayloadStatusWithHeadSnapshot(t *testing.T) {
	for _, test := range []struct {
		name            string
		invalidateAfter int
	}{
		{name: "initial snapshot", invalidateAfter: 1},
		{name: "publication recheck", invalidateAfter: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			beaconCfg := clparams.MainnetBeaconConfig
			beaconCfg.AltairForkEpoch = 0
			beaconCfg.BellatrixForkEpoch = 0
			beaconCfg.CapellaForkEpoch = 0
			beaconCfg.DenebForkEpoch = 0
			beaconCfg.ElectraForkEpoch = 0
			beaconCfg.FuluForkEpoch = 0
			beaconCfg.GloasForkEpoch = 0
			headState := state.New(&beaconCfg)
			headState.SetVersion(clparams.GloasVersion)
			require.NoError(t, headState.SetSlot(1))
			require.NoError(t, headState.SetBlockRootAt(0, common.Hash{1}))
			store := &invalidatingEventHeadStore{
				ForkChoiceStorageMock: &mock_services.ForkChoiceStorageMock{
					HeadVal: common.Hash{2}, HeadSlotVal: 1,
				},
				invalidateAfter: test.invalidateAfter,
			}
			emitter := beaconevents.NewEventEmitter()
			events := make(chan *beaconevents.EventStream, 2)
			subscription := emitter.State().Subscribe(events)
			defer subscription.Unsubscribe()
			require.NoError(t, emitHeadEvent(&beaconCfg, store, emitter, store.HeadSlotVal, store.HeadVal, headState))

			require.Equal(t, 2, store.reads)
			require.Len(t, events, 2, "cache invalidation must not drop a matching FULL head event")
			require.Equal(t, beaconevents.StateHead, (<-events).Event)
			v2 := <-events
			require.Equal(t, beaconevents.StateHeadV2, v2.Event)
			require.Equal(t, "full", v2.Data.(*beaconevents.HeadV2Data).Data.PayloadStatus)
		})
	}
}

func TestEmitHeadEventReportsBlockStateRoot(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = 0
	newHeadState := func(t *testing.T) *state.CachingBeaconState {
		headState := state.New(&beaconCfg)
		headState.SetVersion(clparams.GloasVersion)
		require.NoError(t, headState.SetSlot(1))
		return headState
	}
	requireEventStateRoot := func(t *testing.T, headState *state.CachingBeaconState, want common.Hash) {
		store := &mock_services.ForkChoiceStorageMock{HeadVal: common.Hash{2}, HeadSlotVal: 1}
		emitter := beaconevents.NewEventEmitter()
		events := make(chan *beaconevents.EventStream, 2)
		subscription := emitter.State().Subscribe(events)
		defer subscription.Unsubscribe()

		require.NoError(t, emitHeadEvent(&beaconCfg, store, emitter, store.HeadSlotVal, store.HeadVal, headState))

		require.Equal(t, want, receiveStateEvent(t, events).Data.(*beaconevents.HeadData).State)
		require.Equal(t, want, receiveStateEvent(t, events).Data.(*beaconevents.HeadV2Data).Data.State)
	}
	t.Run("state changed by the envelope after the block", func(t *testing.T) {
		headState := newHeadState(t)
		blockStateRoot, err := headState.HashSSZ()
		require.NoError(t, err)
		headState.SetPreviousStateRoot(blockStateRoot)
		headState.SetLatestBlockHash(common.Hash{0xbb})
		postEnvelopeRoot, err := headState.HashSSZ()
		require.NoError(t, err)
		require.NotEqual(t, blockStateRoot, postEnvelopeRoot)
		requireEventStateRoot(t, headState, blockStateRoot)
	})
	t.Run("no saved state root", func(t *testing.T) {
		headState := newHeadState(t)
		stateRoot, err := headState.HashSSZ()
		require.NoError(t, err)
		requireEventStateRoot(t, headState, stateRoot)
	})
}

func TestEmitHeadEventsDropsStaleSnapshot(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}

	emitted, err := emitHeadEventsIfCurrent(emitter, &beaconevents.HeadV2Data{}, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return common.Hash{3}, 11, "pending", false, nil
	})
	require.NoError(t, err)
	require.False(t, emitted)
	require.Empty(t, ch)
}

func TestEmitHeadEventsDropsStalePayloadStatus(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{PayloadStatus: "pending"}}

	emitted, err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "full", false, nil
	})
	require.NoError(t, err)
	require.False(t, emitted)
	require.Empty(t, ch)
}

func TestEmitHeadEventsDoesNotHoldHeadEventLockWhileLegacySubscriberBlocks(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	legacyEvents := make(chan *beaconevents.EventStream)
	sub := emitter.State().Subscribe(legacyEvents)
	defer sub.Unsubscribe()

	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{PayloadStatus: "pending"}}
	validated := make(chan struct{})
	emitDone := make(chan error, 1)
	go func() {
		_, err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
			select {
			case <-validated:
			default:
				close(validated)
			}
			return headRoot, 10, "pending", false, nil
		})
		emitDone <- err
	}()
	<-validated

	lockAcquired := make(chan struct{})
	go emitter.WithHeadEventLock(func() { close(lockAcquired) })
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("slow legacy subscriber held the shared head event lock")
	}

	sub.Unsubscribe()
	require.NoError(t, <-emitDone)
}

func TestEmitHeadEventsKeepsPairOrderedAgainstConcurrentUpdate(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	fastEvents := make(chan *beaconevents.EventStream, 3)
	slowEvents := make(chan *beaconevents.EventStream)
	fastSub := emitter.State().Subscribe(fastEvents)
	defer fastSub.Unsubscribe()
	slowSub := emitter.State().Subscribe(slowEvents)
	defer slowSub.Unsubscribe()

	headRoot := common.Hash{1}
	newHeadRoot := common.Hash{2}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{
		Slot:          10,
		Block:         headRoot,
		PayloadStatus: "pending",
	}}
	newHeadEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{
		Slot:          11,
		Block:         newHeadRoot,
		PayloadStatus: "full",
	}}
	validated := make(chan struct{})
	var validationCalls atomic.Int32
	var updated atomic.Bool
	emitDone := make(chan error, 1)
	go func() {
		_, err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{3}, func() (common.Hash, uint64, string, bool, error) {
			if validationCalls.Add(1) == 1 {
				close(validated)
			}
			if updated.Load() {
				return newHeadRoot, 11, "full", false, nil
			}
			return headRoot, 10, "pending", false, nil
		})
		emitDone <- err
	}()
	<-validated

	first := <-fastEvents
	require.Equal(t, beaconevents.StateHead, first.Event)
	require.Equal(t, headRoot, first.Data.(*beaconevents.HeadData).Block)

	updated.Store(true)
	updateDone := make(chan struct{})
	go emitter.WithHeadEventLock(func() {
		emitter.State().SendHeadV2(newHeadEvent)
		close(updateDone)
	})
	select {
	case <-updateDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent head update did not complete")
	}

	second := <-fastEvents
	require.Equal(t, beaconevents.StateHeadV2, second.Event)
	require.Equal(t, headRoot, second.Data.(*beaconevents.HeadV2Data).Data.Block)

	slowSub.Unsubscribe()
	require.NoError(t, <-emitDone)
	third := <-fastEvents
	require.Equal(t, beaconevents.StateHeadV2, third.Event)
	require.Equal(t, newHeadRoot, third.Data.(*beaconevents.HeadV2Data).Data.Block)
}

func TestEmitHeadEventsPreservesLegacyHeadFields(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{
		PayloadStatus:             "pending",
		EpochTransition:           false,
		CurrentEpochDependentRoot: common.Hash{3},
		NextEpochDependentRoot:    common.Hash{4},
		ExecutionOptimistic:       true,
	}}

	emitted, err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "pending", true, nil
	})
	require.NoError(t, err)
	require.True(t, emitted)
	legacy := (<-ch).Data.(*beaconevents.HeadData)
	require.False(t, legacy.EpochTransition)
	require.True(t, legacy.ExecutionOptimistic)
	require.Equal(t, headEvent.Data.CurrentEpochDependentRoot, legacy.PreviousDutyDependentRoot)
	require.Equal(t, headEvent.Data.NextEpochDependentRoot, legacy.CurrentDutyDependentRoot)
}

func TestEmitHeadEventsPreGloasEmitsHeadV2AsFull(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{PayloadStatus: "full"}}

	emitted, err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "full", false, nil
	})
	require.NoError(t, err)
	require.True(t, emitted)
	require.Equal(t, beaconevents.StateHead, (<-ch).Event)
	select {
	case v2 := <-ch:
		require.Equal(t, beaconevents.StateHeadV2, v2.Event)
		require.Equal(t, "full", v2.Data.(*beaconevents.HeadV2Data).Data.PayloadStatus)
	case <-time.After(time.Second):
		t.Fatal("head_v2 event was not emitted")
	}
}

type sameRootForkGraph struct {
	*blockingStateCopyForkGraph
	hasEnvelope  atomic.Bool
	hideHeader   atomic.Bool
	failDumpOnce atomic.Bool
	dumpCalls    atomic.Int32
}

func (g *sameRootForkGraph) HasEnvelope(common.Hash) bool { return g.hasEnvelope.Load() }

func (g *sameRootForkGraph) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if g.hideHeader.Load() {
		return nil, false
	}
	return g.ForkGraph.GetHeader(root)
}

func (g *sameRootForkGraph) GetCurrentJustifiedCheckpoint(common.Hash) (solid.Checkpoint, bool) {
	return solid.Checkpoint{}, true
}

func (g *sameRootForkGraph) DumpBeaconStateOnDisk(root common.Hash, bs *state.CachingBeaconState, forced bool) error {
	g.dumpCalls.Add(1)
	if g.failDumpOnce.CompareAndSwap(true, false) {
		return errors.New("dump failed")
	}
	return g.ForkGraph.DumpBeaconStateOnDisk(root, bs, forced)
}

type failingHighestFinalizedTx struct {
	kv.RwTx
	err error
}

func (tx failingHighestFinalizedTx) Put(table string, k, v []byte) error {
	if table == kv.HighestFinalized {
		return tx.err
	}
	return tx.RwTx.Put(table, k, v)
}

// sameRootFixture is a real fork-choice store whose only block is the anchor, with a Gloas state at headSlot.
// Nothing is published to the synced data yet, so the first pass takes the copy-and-publish path.
type sameRootFixture struct {
	cfg        *Cfg
	graph      *sameRootForkGraph
	syncedData *synced_data.SyncedDataManager
	publish    *recordingHeadPublish
	events     chan *beaconevents.EventStream
	tx         kv.RwTx
	headState  *state.CachingBeaconState
	headRoot   common.Hash
	headSlot   uint64
	headResets byte
}

func newSameRootFixture(t *testing.T, gloasForkEpoch, headSlot uint64) *sameRootFixture {
	t.Helper()
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = gloasForkEpoch
	beaconCfg.InitializeForkSchedule()
	genesisTime := uint64(time.Now().Unix()) - headSlot*beaconCfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, &beaconCfg)
	headState := state.New(&beaconCfg)
	headState.SetGenesisTime(genesisTime)
	headState.SetVersion(clparams.GloasVersion)
	require.NoError(t, headState.SetSlot(headSlot))
	headState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: headSlot})

	manager := synced_data.NewSyncedDataManager(&beaconCfg, true)
	emitter := beaconevents.NewEventEmitter()
	baseGraph, err := fork_graph.NewForkGraphDisk(headState, manager, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := &sameRootForkGraph{blockingStateCopyForkGraph: &blockingStateCopyForkGraph{
		ForkGraph: baseGraph,
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}}
	store, err := forkchoice.NewForkChoiceStore(
		clock,
		headState,
		nil,
		pool.NewOperationsPool(&beaconCfg),
		graph,
		emitter,
		manager,
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)
	headRoot := baseGraph.AnchorRoot()
	store.MarkPayloadStatus(headRoot, common.Hash{0xe1}, execution_client.PayloadStatusNotValidated)

	events := make(chan *beaconevents.EventStream, 64)
	subscription := emitter.State().Subscribe(events)
	t.Cleanup(subscription.Unsubscribe)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	publish := &recordingHeadPublish{SyncedData: manager}
	return &sameRootFixture{
		cfg: &Cfg{
			ethClock:                clock,
			beaconCfg:               &beaconCfg,
			forkChoice:              store,
			syncedData:              publish,
			emitter:                 emitter,
			blobDownloader:          &network2.BlobHistoryDownloader{},
			attestationDataProducer: noOpAttestationDataProducer{},
			dirs:                    datadir.New(t.TempDir()),
		},
		graph:      graph,
		syncedData: manager,
		publish:    publish,
		events:     events,
		tx:         tx,
		headState:  headState,
		headRoot:   headRoot,
		headSlot:   headSlot,
	}
}

func (f *sameRootFixture) runPass() error {
	return postForkchoiceOperations(f.tx, log.New(), f.cfg, f.headSlot, f.headRoot)
}

// setHeadStatus makes fork choice select the anchor with the given payload status. The unrelated payload
// mark below only drops the cached head, so that the next read recomputes it.
func (f *sameRootFixture) setHeadStatus(t *testing.T, status cltypes.PayloadStatus) {
	t.Helper()
	f.graph.hideHeader.Store(status == cltypes.PayloadStatusPending)
	f.graph.hasEnvelope.Store(status == cltypes.PayloadStatusFull)
	f.headResets++
	unrelated := common.Hash{0xee, f.headResets}
	f.cfg.forkChoice.MarkPayloadStatus(unrelated, unrelated, execution_client.PayloadStatusNotValidated)
	head, slot, err := f.cfg.forkChoice.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, forkchoice.ForkChoiceNode{Root: f.headRoot, PayloadStatus: status}, head)
	require.Equal(t, f.headSlot, slot)
}

func (f *sameRootFixture) requireCopiesAndPublishes(t *testing.T, want int) {
	t.Helper()
	require.EqualValues(t, want, f.graph.ownedCopies.Load(), "owned head-state copies")
	require.Equal(t, want, f.publish.ownedCalls, "owned head-state publishes")
}

func (f *sameRootFixture) publishedHead(t *testing.T) *state.CachingBeaconState {
	t.Helper()
	var published *state.CachingBeaconState
	require.NoError(t, f.syncedData.ViewHeadState(func(head *state.CachingBeaconState) error {
		published = head
		return nil
	}))
	return published
}

func (f *sameRootFixture) drainHeadEvents() (headV2Statuses []string, headEvents int) {
	for {
		select {
		case event := <-f.events:
			switch event.Event {
			case beaconevents.StateHead:
				headEvents++
			case beaconevents.StateHeadV2:
				headV2Statuses = append(headV2Statuses, event.Data.(*beaconevents.HeadV2Data).Data.PayloadStatus)
			}
		default:
			return headV2Statuses, headEvents
		}
	}
}

func TestPostForkchoiceSameRootRerunKeepsPublishedState(t *testing.T) {
	const (
		empty   = cltypes.PayloadStatusEmpty
		full    = cltypes.PayloadStatusFull
		pending = cltypes.PayloadStatusPending
	)
	for _, test := range []struct {
		name     string
		from, to cltypes.PayloadStatus
	}{
		{name: "EMPTY to FULL", from: empty, to: full},
		{name: "FULL to EMPTY", from: full, to: empty},
		{name: "FULL to PENDING", from: full, to: pending},
		{name: "same status twice", from: empty, to: empty},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSameRootFixture(t, 0, 32)
			f.setHeadStatus(t, test.from)
			require.NoError(t, f.runPass())
			f.requireCopiesAndPublishes(t, 1)
			published := f.publishedHead(t)
			f.drainHeadEvents()

			f.setHeadStatus(t, test.to)
			require.NoError(t, f.runPass())

			f.requireCopiesAndPublishes(t, 1)
			require.Same(t, published, f.publishedHead(t))
			headV2Statuses, headEvents := f.drainHeadEvents()
			require.Equal(t, []string{beaconevents.PayloadStatusName(test.to)}, headV2Statuses)
			require.Equal(t, 1, headEvents)
		})
	}
}

func TestPostForkchoiceSameRootRerunFinishesFailedPass(t *testing.T) {
	finalizedSaveSlot := 5 * clparams.MainnetBeaconConfig.SlotsPerEpoch
	f := newSameRootFixture(t, 0, finalizedSaveSlot)
	f.graph.failDumpOnce.Store(true)

	require.ErrorContains(t, f.runPass(), "dump failed")
	require.EqualValues(t, 1, f.graph.dumpCalls.Load())
	f.requireCopiesAndPublishes(t, 1)
	requireNoFinalizedStateFile(t, f.cfg.dirs)

	require.NoError(t, f.runPass())

	require.EqualValues(t, 2, f.graph.dumpCalls.Load())
	f.requireCopiesAndPublishes(t, 1)
	require.FileExists(t, finalizedStateFilePath(f.cfg.dirs))
}

func TestPostForkchoiceSameRootAfterOtherWriterRunsPerPassSteps(t *testing.T) {
	f := newSameRootFixture(t, 0, 32)
	require.NoError(t, f.syncedData.OnHeadStateWithBlockRoot(f.headState, f.headRoot))
	require.False(t, f.cfg.forkChoice.Synced())

	require.NoError(t, f.runPass())

	f.requireCopiesAndPublishes(t, 0)
	require.True(t, f.cfg.forkChoice.Synced())
	require.Equal(t, f.headSlot, f.cfg.blobDownloader.HeadSlot())
	highestFinalized, err := beacon_indicies.ReadHighestFinalized(f.tx)
	require.NoError(t, err)
	require.Equal(t, f.cfg.forkChoice.FinalizedSlot(), highestFinalized)
	headV2Statuses, headEvents := f.drainHeadEvents()
	require.Equal(t, []string{"empty"}, headV2Statuses)
	require.Equal(t, 1, headEvents)
}

func TestPostForkchoiceSameRootAfterOtherWriterCachesAttestationData(t *testing.T) {
	f := newSameRootFixture(t, 0, 32)
	producer := attestation_producer.New(f.cfg.beaconCfg)
	f.cfg.attestationDataProducer = producer
	require.NoError(t, f.syncedData.OnHeadStateWithBlockRoot(f.headState, f.headRoot))

	require.NoError(t, f.runPass())

	f.requireCopiesAndPublishes(t, 0)
	data, ok := producer.CachedAttestationData(f.headRoot, f.headSlot)
	require.True(t, ok, "attestation data for the head slot is not cached")
	require.Equal(t, f.headRoot, data.BeaconBlockRoot)
}

func TestPostForkchoiceSameRootSkipsWarmUpWhenAttestationDataCached(t *testing.T) {
	f := newSameRootFixture(t, 0, 32)
	producer := &countingAttestationDataProducer{AttestationDataProducer: attestation_producer.New(f.cfg.beaconCfg)}
	f.cfg.attestationDataProducer = producer
	require.NoError(t, f.syncedData.OnHeadStateWithBlockRoot(f.headState, f.headRoot))

	require.NoError(t, f.runPass())
	require.EqualValues(t, 1, producer.produceCalls.Load(), "first pass warms the empty cache")

	require.NoError(t, f.runPass())
	require.EqualValues(t, 1, producer.produceCalls.Load(), "second pass must not warm a populated cache")
	f.requireCopiesAndPublishes(t, 0)
}

type headChangingSyncedData struct {
	synced_data.SyncedData
	publishOtherHead func()
}

func (s *headChangingSyncedData) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	s.publishOtherHead()
	return s.SyncedData.ViewHeadStateWithIdentity(fn)
}

func TestPostForkchoiceSameRootWarmsHeadSlotWhenHeadChangesBeforeWarmUp(t *testing.T) {
	slotsPerEpoch := clparams.MainnetBeaconConfig.SlotsPerEpoch
	for _, test := range []struct {
		name       string
		otherRoot  func(f *sameRootFixture) common.Hash
		slotOffset uint64
	}{
		{name: "different root at the same slot", otherRoot: func(*sameRootFixture) common.Hash { return common.Hash{0xbb} }},
		{name: "different root in the next epoch", otherRoot: func(*sameRootFixture) common.Hash { return common.Hash{0xbb} }, slotOffset: slotsPerEpoch},
		{name: "same root at a later slot", otherRoot: func(f *sameRootFixture) common.Hash { return f.headRoot }, slotOffset: slotsPerEpoch},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSameRootFixture(t, 0, 32)
			require.NoError(t, f.syncedData.OnHeadStateWithBlockRoot(f.headState, f.headRoot))
			otherHead, err := f.headState.Copy()
			require.NoError(t, err)
			require.NoError(t, otherHead.SetSlot(f.headSlot+test.slotOffset))
			otherHead.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Epoch: 1, Root: common.Hash{0xcc}})
			f.cfg.syncedData = &headChangingSyncedData{
				SyncedData: f.cfg.syncedData,
				publishOtherHead: func() {
					require.NoError(t, f.syncedData.OnHeadStateWithBlockRoot(otherHead, test.otherRoot(f)))
				},
			}
			producer := attestation_producer.New(f.cfg.beaconCfg)
			f.cfg.attestationDataProducer = producer

			require.NoError(t, f.runPass())

			f.requireCopiesAndPublishes(t, 0)
			data, ok := producer.CachedAttestationData(f.headRoot, f.headSlot)
			require.True(t, ok, "attestation data for the head slot is not cached")
			require.Equal(t, f.headRoot, data.BeaconBlockRoot)
			require.Equal(t, f.headState.CurrentJustifiedCheckpoint(), data.Source, "warmed from another head's state")
		})
	}
}

func TestPostForkchoiceRepublishesNewRootOrSlot(t *testing.T) {
	for _, test := range []struct {
		name      string
		otherRoot func(f *sameRootFixture) common.Hash
		otherSlot func(f *sameRootFixture) uint64
	}{
		{
			name:      "different root",
			otherRoot: func(*sameRootFixture) common.Hash { return common.Hash{0xbb} },
			otherSlot: func(f *sameRootFixture) uint64 { return f.headSlot },
		},
		{
			name:      "different slot",
			otherRoot: func(f *sameRootFixture) common.Hash { return f.headRoot },
			otherSlot: func(f *sameRootFixture) uint64 { return f.headSlot + 1 },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSameRootFixture(t, 0, 32)
			other, err := f.headState.Copy()
			require.NoError(t, err)
			require.NoError(t, other.SetSlot(test.otherSlot(f)))
			require.NoError(t, f.syncedData.OnHeadStateWithBlockRoot(other, test.otherRoot(f)))

			require.NoError(t, f.runPass())

			f.requireCopiesAndPublishes(t, 1)
			root, slot, ok := f.syncedData.StateHead()
			require.True(t, ok)
			require.Equal(t, f.headRoot, root)
			require.Equal(t, f.headSlot, slot)
		})
	}
}

func TestPostForkchoiceSameRootFallsBackWhenEventNotEmitted(t *testing.T) {
	f := newSameRootFixture(t, 0, 32)
	require.NoError(t, f.runPass())
	f.requireCopiesAndPublishes(t, 1)
	f.drainHeadEvents()

	f.graph.hideCurrentView = true
	f.graph.releaseCopy()
	f.graph.armed.Store(true)
	require.NoError(t, f.runPass())

	f.requireCopiesAndPublishes(t, 2)
	headV2Statuses, headEvents := f.drainHeadEvents()
	require.Equal(t, []string{"empty"}, headV2Statuses)
	require.Equal(t, 1, headEvents)
}

func TestPostForkchoiceGateFollowsHeadSlotFork(t *testing.T) {
	const gloasForkEpoch = 1
	gloasStart := gloasForkEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
	for _, test := range []struct {
		name       string
		headSlot   uint64
		wantCopies int
	}{
		{name: "before Gloas", headSlot: gloasStart - 1, wantCopies: 2},
		{name: "first Gloas slot", headSlot: gloasStart, wantCopies: 1},
		{name: "after first Gloas slot", headSlot: gloasStart + 1, wantCopies: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSameRootFixture(t, gloasForkEpoch, test.headSlot)
			// A hidden header makes fork choice return the anchor, which avoids building a checkpoint state.
			f.graph.hideHeader.Store(true)

			require.NoError(t, f.runPass())
			require.NoError(t, f.runPass())

			f.requireCopiesAndPublishes(t, test.wantCopies)
		})
	}
}

func TestPostForkchoiceHighestFinalizedFailureDoesNotPublish(t *testing.T) {
	f := newSameRootFixture(t, 0, 32)
	putErr := errors.New("put failed")
	f.tx = failingHighestFinalizedTx{RwTx: f.tx, err: putErr}

	require.ErrorIs(t, f.runPass(), putErr)

	f.requireCopiesAndPublishes(t, 0)
	require.Equal(t, f.headSlot, f.cfg.blobDownloader.HeadSlot())
}
