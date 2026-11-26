package stages

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

type signalingHeadPublish struct {
	synced_data.SyncedData
	published chan common.Hash
}

func (s *signalingHeadPublish) OnOwnedHeadStateWithBlockRoot(newState *state.CachingBeaconState, blockRoot common.Hash) error {
	err := s.SyncedData.OnOwnedHeadStateWithBlockRoot(newState, blockRoot)
	s.published <- blockRoot
	return err
}

func withForkchoiceRoutineDeps(t *testing.T, cfg *Cfg, syncedData synced_data.SyncedData) <-chan common.Hash {
	t.Helper()
	published := make(chan common.Hash, 1)
	cfg.syncedData = &signalingHeadPublish{SyncedData: syncedData, published: published}
	cfg.indiciesDB = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	cfg.attestationDataProducer = noOpAttestationDataProducer{}
	cfg.dirs = datadir.New(t.TempDir())
	return published
}

func blockingForkChoice(entered chan<- struct{}, release <-chan struct{}) func(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
	return func(ctx context.Context, _, _, _ common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	}
}

func receiveWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal(what)
		var zero T
		return zero
	}
}

func TestForkchoiceRoutinePublishesValidatedHeadWhileExecutionAppliesIt(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	engine := &testExecutionEngine{forkChoiceFn: blockingForkChoice(entered, release)}
	fixture := newLockOrderFixtureWithConfig(t, 1, engine, common.Hash{0xee}, false, nil)
	fixture.cfg.forkChoice.MarkPayloadStatus(fixture.headRoot, common.Hash{0xee}, execution_client.PayloadStatusValidated)
	published := withForkchoiceRoutineDeps(t, fixture.cfg, fixture.syncedData)
	done := make(chan error, 1)
	go func() { done <- doForkchoiceRoutine(t.Context(), log.New(), fixture.cfg, Args{}) }()

	receiveWithin(t, entered, "the forkchoice update was not sent")
	root := receiveWithin(t, published, "the head was not published while the execution layer applied it")
	require.Equal(t, fixture.headRoot, root)
	close(release)
	require.NoError(t, receiveWithin(t, done, "the routine did not finish"))
}

// A forward-synced head skipped newPayload, so the forkchoice update is its first execution check.
func TestForkchoiceRoutineWaitsForExecutionOnHeadWithoutValidation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	engine := &testExecutionEngine{forkChoiceFn: blockingForkChoice(entered, release)}
	fixture := newLockOrderFixtureWithConfig(t, 1, engine, common.Hash{0xee}, false, nil)
	published := withForkchoiceRoutineDeps(t, fixture.cfg, fixture.syncedData)
	done := make(chan error, 1)
	go func() { done <- doForkchoiceRoutine(t.Context(), log.New(), fixture.cfg, Args{}) }()

	receiveWithin(t, entered, "the forkchoice update was not sent")
	select {
	case <-published:
		t.Fatal("a head without a validation verdict was published before the execution layer applied it")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	require.Equal(t, fixture.headRoot, receiveWithin(t, published, "the head was not published after the execution layer applied it"))
	require.NoError(t, receiveWithin(t, done, "the routine did not finish"))
}

func TestForkchoiceRoutineReturnsExecutionErrorAfterPublishingValidatedHead(t *testing.T) {
	wantErr := errors.New("execution layer rejected the head")
	engine := &testExecutionEngine{forkChoiceErr: wantErr}
	fixture := newLockOrderFixtureWithConfig(t, 1, engine, common.Hash{0xee}, false, nil)
	fixture.cfg.forkChoice.MarkPayloadStatus(fixture.headRoot, common.Hash{0xee}, execution_client.PayloadStatusValidated)
	published := withForkchoiceRoutineDeps(t, fixture.cfg, fixture.syncedData)

	err := doForkchoiceRoutine(t.Context(), log.New(), fixture.cfg, Args{})

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, fixture.headRoot, receiveWithin(t, published, "the head was not published"))
	// The canonical chain on disk follows the head the API already serves.
	tx, err := fixture.cfg.indiciesDB.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, fixture.headState.Slot())
	require.NoError(t, err)
	require.Equal(t, fixture.headRoot, root)
}

func TestPublishHeadBeforeExecutionOnlyForValidatedPreGloasHeads(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.GloasForkEpoch = beaconCfg.FuluForkEpoch + 10
	beaconCfg.InitializeForkSchedule()
	gloasSlot := beaconCfg.GloasForkEpoch * beaconCfg.SlotsPerEpoch
	for name, tc := range map[string]struct {
		slot    uint64
		status  execution_client.PayloadStatus
		verdict bool
		want    bool
	}{
		"validated pre-Gloas head":         {slot: gloasSlot - 1, status: execution_client.PayloadStatusValidated, verdict: true, want: true},
		"optimistic pre-Gloas head":        {slot: gloasSlot - 1, status: execution_client.PayloadStatusNotValidated, verdict: true},
		"invalidated pre-Gloas head":       {slot: gloasSlot - 1, status: execution_client.PayloadStatusInvalidated, verdict: true},
		"pre-Gloas head without a verdict": {slot: gloasSlot - 1},
		"Gloas head":                       {slot: gloasSlot, status: execution_client.PayloadStatusValidated, verdict: true},
	} {
		t.Run(name, func(t *testing.T) {
			store := mock_services.NewForkChoiceStorageMock(t)
			if tc.verdict {
				store.PayloadStatusByRootMap[common.Hash{1}] = tc.status
			}
			require.Equal(t, tc.want, publishHeadBeforeExecution(&beaconCfg, store, common.Hash{1}, tc.slot))
		})
	}
}

type publishingOnStateTake struct {
	synced_data.SyncedData
	publish func()
}

func (s *publishingOnStateTake) TakeSpareHeadState() *state.CachingBeaconState {
	if s.publish != nil {
		publish := s.publish
		s.publish = nil
		publish()
	}
	return s.SyncedData.TakeSpareHeadState()
}

func TestPostForkchoiceOperationsSkipsGloasInstallOfHeadPublicationReplaced(t *testing.T) {
	cfg, _, newHeadRoot, newHead, _, _ := newGloasCanonicalForkChoiceFixture(t, true)
	staleRoot := newHead.Block.ParentRoot
	cfg.ethClock.(*eth_clock.MockEthereumClock).EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(
		func(slot uint64) uint64 { return slot / cfg.beaconCfg.SlotsPerEpoch }).AnyTimes()
	manager := synced_data.NewSyncedDataManager(cfg.beaconCfg, true)
	cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	cfg.attestationDataProducer = noOpAttestationDataProducer{}
	cfg.dirs = datadir.New(t.TempDir())
	cfg.syncedData = &publishingOnStateTake{SyncedData: manager, publish: func() {
		unlock, err := cfg.forkChoice.LockExecutionHead(t.Context())
		require.NoError(t, err)
		defer unlock()
		newHeadState, err := cfg.forkChoice.GetStateAtBlockRoot(newHeadRoot)
		require.NoError(t, err)
		require.NoError(t, manager.OnHeadStateWithBlockRoot(newHeadState, newHeadRoot))
	}}
	tx, err := mdbxtest.NewTestDB(t, dbcfg.ChainDB).BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	require.ErrorIs(t, postForkchoiceOperations(t.Context(), tx, log.New(), cfg, newHead.Block.Slot-1, staleRoot), errHeadSuperseded)

	root, _, ok := manager.StateHead()
	require.True(t, ok)
	require.Equal(t, newHeadRoot, root, "the stage installed a head state that publication had already replaced")
}

func newSupersededInstallFixture(t *testing.T) (cfg *Cfg, staleRoot common.Hash, publish, importNewHead func()) {
	t.Helper()
	cfg, graph, newHeadRoot, _, _, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusNotValidated, false)
	newHead := graph.parents[newHeadRoot]
	envelope := graph.envelopes[newHeadRoot]
	delete(graph.parents, newHeadRoot)
	graph.insertOnAdd = true
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GenesisTime().Return(uint64(0)).AnyTimes()
	clock.EXPECT().GetCurrentSlot().Return(newHead.Block.Slot).AnyTimes()
	clock.EXPECT().GetCurrentEpoch().Return(newHead.Block.Slot / cfg.beaconCfg.SlotsPerEpoch).AnyTimes()
	clock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(
		func(slot uint64) uint64 { return slot / cfg.beaconCfg.SlotsPerEpoch }).AnyTimes()
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{}, nil).AnyTimes()
	clock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.GloasVersion).AnyTimes()
	cfg.ethClock = clock
	cfg.rpc = rpc.NewBeaconRpcP2P(t.Context(), &statusSentinel{}, cfg.beaconCfg, clock)
	manager := synced_data.NewSyncedDataManager(cfg.beaconCfg, true)
	cfg.syncedData = &publishingOnStateTake{SyncedData: manager}
	cfg.indiciesDB = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg.blobDownloader = &network2.BlobHistoryDownloader{}
	cfg.attestationDataProducer = noOpAttestationDataProducer{}
	cfg.dirs = datadir.New(t.TempDir())
	importNewHead = func() {
		require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), newHead, false, false, false))
		cfg.forkChoice.MarkPayloadStatus(newHeadRoot, envelope.Message.Payload.BlockHash, execution_client.PayloadStatusNotValidated)
	}
	publish = func() {
		unlock, err := cfg.forkChoice.LockExecutionHead(t.Context())
		require.NoError(t, err)
		defer unlock()
		importNewHead()
		newHeadState, err := cfg.forkChoice.GetStateAtBlockRoot(newHeadRoot)
		require.NoError(t, err)
		require.NoError(t, manager.OnHeadStateWithBlockRoot(newHeadState, newHeadRoot))
	}
	return cfg, newHead.Block.ParentRoot, publish, importNewHead
}

func TestForkchoiceRoutineRerunsWhenGloasHeadInstallIsSuperseded(t *testing.T) {
	cfg, staleRoot, publish, _ := newSupersededInstallFixture(t)
	cfg.syncedData.(*publishingOnStateTake).publish = publish
	head, headSlot, err := selectForkChoiceHead(cfg)
	require.NoError(t, err)
	require.Equal(t, staleRoot, head.Root)

	require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, Args{}))

	require.True(t, cfg.forkChoiceSuperseded, "the pass for the superseded head must ask for another run")
	require.Equal(t, common.Hash{}, cfg.committedRoot, "a rolled back pass must not mark its head committed")
	next := ConsensusClStages().Stages[ForkChoice].TransitionFunc(cfg, Args{hasDownloaded: true, atForwardSyncTip: true}, nil)
	require.Equal(t, ForkChoice, next)
	requireCanonicalRoot(t, cfg.indiciesDB, headSlot, common.Hash{})
}

func TestForkchoiceRoutineSendsNoChainReorgForASupersededGloasPass(t *testing.T) {
	cfg, staleRoot, publish, _ := newSupersededInstallFixture(t)
	cfg.syncedData.(*publishingOnStateTake).publish = publish
	_, headSlot, err := selectForkChoiceHead(cfg)
	require.NoError(t, err)
	oldTip := common.Hash{0xc0}
	require.NoError(t, cfg.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		writeTestBlockIndices(t, tx, oldTip, common.Hash{}, common.Hash{0xc1}, headSlot+1, true)
		return beacon_indicies.WriteStateRoot(tx, staleRoot, common.Hash{0xc2})
	}))
	events := make(chan *beaconevents.EventStream, 16)
	subscription := cfg.emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()

	require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, Args{}))

	require.True(t, cfg.forkChoiceSuperseded)
	requireCanonicalRoot(t, cfg.indiciesDB, headSlot+1, oldTip)
	for len(events) > 0 {
		require.NotEqual(t, beaconevents.StateChainReorg, (<-events).Event, "a rolled back pass announced a head that was never committed")
	}
}

func TestForkchoiceRoutineSendsTheChainReorgOfACommittedGloasPass(t *testing.T) {
	useFreshPrecacheValidatorCaches(t)
	t.Cleanup(func() {
		require.Eventually(t, func() bool { return !workingPreCacheNextShuffledValidatorSet.Load() }, 10*time.Second, time.Millisecond)
	})
	cfg, staleRoot, _, _ := newSupersededInstallFixture(t)
	_, headSlot, err := selectForkChoiceHead(cfg)
	require.NoError(t, err)
	require.NoError(t, cfg.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		writeTestBlockIndices(t, tx, common.Hash{0xc0}, common.Hash{}, common.Hash{0xc1}, headSlot+1, true)
		return beacon_indicies.WriteStateRoot(tx, staleRoot, common.Hash{0xc2})
	}))
	events := make(chan *beaconevents.EventStream, 16)
	subscription := cfg.emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()

	require.NoError(t, doForkchoiceRoutine(t.Context(), log.New(), cfg, Args{}))

	require.False(t, cfg.forkChoiceSuperseded)
	require.Equal(t, staleRoot, cfg.committedRoot)
	for len(events) > 0 {
		if evt := <-events; evt.Event == beaconevents.StateChainReorg {
			require.Equal(t, staleRoot, evt.Data.(*beaconevents.ChainReorgData).NewHeadBlock)
			return
		}
	}
	t.Fatal("a committed reorg must send its chain_reorg event")
}

func TestForkchoiceRoutineRerunsWhenTheGloasHeadChangesBeforeTheStageLocksTheExecutionHead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, _, _, importNewHead := newSupersededInstallFixture(t)
		engine := cfg.forkChoice.Engine().(*testExecutionEngine)
		stale, headSlot, err := selectForkChoiceHead(cfg)
		require.NoError(t, err)
		staleRoot := stale.Root

		unlock, err := cfg.forkChoice.LockExecutionHead(t.Context())
		require.NoError(t, err)
		routineErr := make(chan error, 1)
		go func() { routineErr <- doForkchoiceRoutine(t.Context(), log.New(), cfg, Args{}) }()
		synctest.Wait()
		importNewHead()
		unlock()
		synctest.Wait()

		require.NoError(t, <-routineErr)
		current, _, err := cfg.forkChoice.GetHeadNode()
		require.NoError(t, err)
		require.NotEqual(t, staleRoot, current.Root)
		require.True(t, cfg.forkChoiceSuperseded)
		require.Equal(t, ForkChoice, ConsensusClStages().Stages[ForkChoice].TransitionFunc(cfg, Args{hasDownloaded: true, atForwardSyncTip: true}, nil))
		require.Empty(t, engine.forkChoiceHeads)
		require.Equal(t, common.Hash{}, cfg.forkChoice.LastExecutionHead())
		requireCanonicalRoot(t, cfg.indiciesDB, headSlot, common.Hash{})
	})
}

func TestForkChoiceStageRerunsBeforeCatchUpWhenGloasHeadWasSuperseded(t *testing.T) {
	cfg := &Cfg{forkChoiceSuperseded: true}
	behind := Args{hasDownloaded: true, atForwardSyncTip: false, seenSlot: 9, targetSlot: 10, peers: 1}
	require.NotEmpty(t, MetaCatchingUp(behind))

	require.Equal(t, ForkChoice, ConsensusClStages().Stages[ForkChoice].TransitionFunc(cfg, behind, nil))
}

func TestNotifyServicesOfHeadSendsTheCurrentGloasPayloadStatus(t *testing.T) {
	cfg, _, headRoot, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, true)
	bid := headBlock.Block.Body.GetSignedExecutionPayloadBid().Message
	sampled := forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusEmpty}

	require.NoError(t, notifyServicesOfHead(t.Context(), log.New(), cfg, sampled, headBlock.Block.Slot))

	require.Equal(t, []common.Hash{bid.BlockHash}, engine.forkChoiceHeads)
}

func TestNotifyServicesOfHeadSendsNothingWhenTheGloasHeadChangedRoot(t *testing.T) {
	cfg, _, _, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, true)
	sampled := forkchoice.ForkChoiceNode{Root: headBlock.Block.ParentRoot, PayloadStatus: cltypes.PayloadStatusFull}

	err := notifyServicesOfHead(t.Context(), log.New(), cfg, sampled, headBlock.Block.Slot)

	require.ErrorIs(t, err, errHeadSuperseded)
	require.Empty(t, engine.forkChoiceHeads)
	require.Equal(t, common.Hash{}, cfg.forkChoice.LastExecutionHead())
}

func TestNotifyServicesOfHeadCancelledWhileWaitingForTheExecutionHeadLockSendsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, _, headRoot, headBlock, _, engine := newGloasCanonicalForkChoiceFixture(t, true)
		sampled := forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusEmpty}
		unlock, err := cfg.forkChoice.LockExecutionHead(t.Context())
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- notifyServicesOfHead(ctx, log.New(), cfg, sampled, headBlock.Block.Slot) }()
		synctest.Wait()
		require.Empty(t, done, "notifyServicesOfHead returned while the lock was held")

		cancel()
		synctest.Wait()

		require.ErrorIs(t, <-done, context.Canceled)
		require.Empty(t, engine.forkChoiceHeads)
		unlock()
		again, ok := cfg.forkChoice.TryLockExecutionHead()
		require.True(t, ok, "the cancelled stage must not hold the lock")
		again()
	})
}

type publishingOnInstall struct {
	synced_data.SyncedData
	publish func()
}

func (s *publishingOnInstall) OnOwnedHeadStateWithBlockRoot(newState *state.CachingBeaconState, blockRoot common.Hash) error {
	if s.publish != nil {
		publish := s.publish
		s.publish = nil
		publish()
	}
	return s.SyncedData.OnOwnedHeadStateWithBlockRoot(newState, blockRoot)
}

func TestInstallHeadStateIsOrderedWithGloasPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, _, headRoot, headBlock, _, _ := newGloasCanonicalForkChoiceFixture(t, true)
		publishedRoot := headBlock.Block.ParentRoot
		publishedState, err := cfg.forkChoice.GetStateAtBlockRoot(publishedRoot)
		require.NoError(t, err)
		headState, err := cfg.forkChoice.GetStateAtBlockRoot(headRoot)
		require.NoError(t, err)
		manager := synced_data.NewSyncedDataManager(cfg.beaconCfg, true)
		var publication chan error
		cfg.syncedData = &publishingOnInstall{SyncedData: manager, publish: func() {
			installed := make(chan error, 1)
			go func() {
				unlock, err := cfg.forkChoice.LockExecutionHead(t.Context())
				if err != nil {
					installed <- err
					return
				}
				defer unlock()
				installed <- manager.OnHeadStateWithBlockRoot(publishedState, publishedRoot)
			}()
			synctest.Wait()
			publication = installed
		}}

		require.NoError(t, installHeadState(t.Context(), cfg, headState, headBlock.Block.Slot, headRoot))
		synctest.Wait()

		require.NoError(t, <-publication)
		root, _, ok := manager.StateHead()
		require.True(t, ok)
		require.Equal(t, publishedRoot, root, "the stage's install overwrote a publication that ran after its head check")
	})
}
