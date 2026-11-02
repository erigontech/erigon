package stages

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
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
	fixture := newLockOrderFixtureWithConfig(t, 1, engine, common.Hash{0xee}, false)
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
	fixture := newLockOrderFixtureWithConfig(t, 1, engine, common.Hash{0xee}, false)
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
	fixture := newLockOrderFixtureWithConfig(t, 1, engine, common.Hash{0xee}, false)
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
