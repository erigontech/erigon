package stages

import (
	"context"
	"errors"
	"math"
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

func (*statusSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

type noOpAttestationDataProducer struct {
	attestation_producer.AttestationDataProducer
}

func (noOpAttestationDataProducer) ProduceAndCacheAttestationData(kv.Tx, *state.CachingBeaconState, common.Hash, uint64) (solid.AttestationData, error) {
	return solid.AttestationData{}, nil
}

type lockOrderFixture struct {
	cfg        *Cfg
	syncedData *synced_data.SyncedDataManager
	headState  *state.CachingBeaconState
	headRoot   common.Hash
	view       *pausingHeadView
}

func newLockOrderFixture(t *testing.T, slot uint64) lockOrderFixture {
	return newLockOrderFixtureWithConfig(t, slot, nil, common.Hash{}, false)
}

func newLockOrderFixtureWithConfig(t *testing.T, slot uint64, engine execution_client.ExecutionEngine, executionHash common.Hash, gloas bool) lockOrderFixture {
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
	if executionHash != (common.Hash{}) {
		if gloas {
			headState.SetVersion(clparams.GloasVersion)
			headState.SetLatestBlockHash(executionHash)
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
	fixture := newLockOrderFixtureWithConfig(t, 0, engine, executionHash, false)
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
	fixture := newLockOrderFixtureWithConfig(t, 0, nil, executionHash, false)

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
	fixture := newLockOrderFixtureWithConfig(t, 0, engine, executionHash, true)
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
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

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

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, state100, 100, true)
	writeBlock(root101a, root100, state101a, 101, true)
	writeBlock(root102a, root101a, state102a, 102, true)

	writeBlock(root101b, root100, state101b, 101, false)
	writeBlock(root102b, root101b, state102b, 102, false)

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
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root103a := common.Hash{0x13, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}

	state100 := common.Hash{0xa0}
	state103a := common.Hash{0xa3}
	state102b := common.Hash{0xb2}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, state100, 100, true)
	writeBlock(root101a, root100, common.Hash{0xa1}, 101, true)
	writeBlock(root102a, root101a, common.Hash{0xa2}, 102, true)
	writeBlock(root103a, root102a, state103a, 103, true)

	writeBlock(root101b, root100, common.Hash{0xb1}, 101, false)
	writeBlock(root102b, root101b, state102b, 102, false)

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
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}
	root103b := common.Hash{0x13, 0xbb}

	state102a := common.Hash{0xa2}
	state103b := common.Hash{0xb3}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101a, root100, common.Hash{0xa1}, 101, true)
	writeBlock(root102a, root101a, state102a, 102, true)

	writeBlock(root101b, root100, common.Hash{0xb1}, 101, false)
	writeBlock(root102b, root101b, common.Hash{0xb2}, 102, false)
	writeBlock(root103b, root102b, state103b, 103, false)

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
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101 := common.Hash{0x11}
	root102 := common.Hash{0x12}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101, root100, common.Hash{0xa1}, 101, true)

	writeBlock(root102, root101, common.Hash{0xa2}, 102, false)

	reorg := drainReorgEvent(t, tx, 102, root102)
	require.Nil(t, reorg, "chain extension should NOT emit a chain_reorg event")
}

func TestUpdateCanonicalChainReorgOneSlot(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root101b := common.Hash{0x11, 0xbb}

	state101a := common.Hash{0xa1}
	state101b := common.Hash{0xb1}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101a, root100, state101a, 101, true)
	writeBlock(root101b, root100, state101b, 101, false)

	reorg := drainReorgEvent(t, tx, 101, root101b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(101), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(1), reorg.Depth, "reorg Depth: old tip 101 - fork point 100 = 1")
	require.Equal(t, root101a, reorg.OldHeadBlock, "OldHeadBlock must be root_101a")
	require.Equal(t, root101b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state101a, reorg.OldHeadState, "OldHeadState")
	require.Equal(t, state101b, reorg.NewHeadState, "NewHeadState")
}

func drainReorgEvent(t *testing.T, tx kv.RwTx, headSlot uint64, headRoot common.Hash) *beaconevents.ChainReorgData {
	t.Helper()
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 16)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()

	cfg := &Cfg{
		emitter:   emitter,
		beaconCfg: &clparams.MainnetBeaconConfig,
	}

	err := updateCanonicalChainInTheDatabase(tx, headSlot, headRoot, cfg)
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

func TestEmitHeadEventsDropsStaleSnapshot(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}

	err := emitHeadEventsIfCurrent(emitter, &beaconevents.HeadV2Data{}, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return common.Hash{3}, 11, "pending", false, nil
	})
	require.NoError(t, err)
	require.Empty(t, ch)
}

func TestEmitHeadEventsDropsStalePayloadStatus(t *testing.T) {
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 2)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()
	headRoot := common.Hash{1}
	headEvent := &beaconevents.HeadV2Data{Data: beaconevents.HeadV2Content{PayloadStatus: "pending"}}

	err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "full", false, nil
	})
	require.NoError(t, err)
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
		emitDone <- emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
			select {
			case <-validated:
			default:
				close(validated)
			}
			return headRoot, 10, "pending", false, nil
		})
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
		emitDone <- emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{3}, func() (common.Hash, uint64, string, bool, error) {
			if validationCalls.Add(1) == 1 {
				close(validated)
			}
			if updated.Load() {
				return newHeadRoot, 11, "full", false, nil
			}
			return headRoot, 10, "pending", false, nil
		})
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

	err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "pending", true, nil
	})
	require.NoError(t, err)
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

	err := emitHeadEventsIfCurrent(emitter, headEvent, 10, headRoot, common.Hash{2}, func() (common.Hash, uint64, string, bool, error) {
		return headRoot, 10, "full", false, nil
	})
	require.NoError(t, err)
	require.Equal(t, beaconevents.StateHead, (<-ch).Event)
	select {
	case v2 := <-ch:
		require.Equal(t, beaconevents.StateHeadV2, v2.Event)
		require.Equal(t, "full", v2.Data.(*beaconevents.HeadV2Data).Data.PayloadStatus)
	case <-time.After(time.Second):
		t.Fatal("head_v2 event was not emitted")
	}
}
