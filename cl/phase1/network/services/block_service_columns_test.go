package services

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestProcessMessageRetriesWhenStoredColumnArrives(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, block, processing, peerDas := newFuluColumnGossipFixture(t)
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		storage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), service.beaconCfg, service.emitter)
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).DoAndReturn(func(slot uint64, root common.Hash) (bool, error) {
			return storage.ColumnSidecarExists(t.Context(), slot, root, 0)
		}).Times(2)

		require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
		job := serviceJob(t, service, root)
		job.mu.Lock()
		require.True(t, job.waitsForColumns)
		job.mu.Unlock()
		processing.err = nil

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		go service.loop(ctx)
		synctest.Wait()
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		sidecar.SignedBlockHeader = block.SignedBeaconBlockHeader()
		require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 0, sidecar))
		synctest.Wait()

		require.Equal(t, start, time.Now(), "the ticker must not have fired")
		require.Equal(t, int32(2), processing.calls.Load())
		_, queued := service.blocksScheduledForLaterExecution.Load(root)
		require.False(t, queued)
	})
}

type afterOnBlockStore struct {
	*onBlockErrorStore
	after func()
}

func (s *afterOnBlockStore) OnBlock(ctx context.Context, block *cltypes.SignedBeaconBlock, newPayload, fullValidation, checkDataAvaiability bool) error {
	err := s.onBlockErrorStore.OnBlock(ctx, block, newPayload, fullValidation, checkDataAvaiability)
	if s.after != nil {
		s.after()
		s.after = nil
	}
	return err
}

func TestProcessMessageRetriesWhenLastColumnArrivesBeforeJobWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, block, processing, peerDas := newFuluColumnGossipFixture(t)
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		storage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), service.beaconCfg, service.emitter)
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).DoAndReturn(func(slot uint64, root common.Hash) (bool, error) {
			return storage.ColumnSidecarExists(t.Context(), slot, root, 0)
		})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go service.loop(ctx)
		synctest.Wait()
		service.forkchoiceStore = &afterOnBlockStore{onBlockErrorStore: processing, after: func() {
			sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
			sidecar.SignedBlockHeader = block.SignedBeaconBlockHeader()
			require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 0, sidecar))
			synctest.Wait()
			processing.err = nil
		}}

		start := time.Now()
		require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
		synctest.Wait()

		require.Equal(t, start, time.Now(), "the ticker must not have fired")
		require.Equal(t, int32(2), processing.calls.Load())
		_, queued := service.blocksScheduledForLaterExecution.Load(root)
		require.False(t, queued)
	})
}

func newFuluColumnGossipFixture(t *testing.T) (*blockService, *cltypes.SignedBeaconBlock, *onBlockErrorStore, *das_mock.MockPeerDas) {
	t.Helper()
	parentState, cfg := newUpgradedValidationState(t, clparams.FuluVersion, func(cfg *clparams.BeaconChainConfig, _ uint64) {
		cfg.AltairForkEpoch = 0
		cfg.BellatrixForkEpoch = 0
		cfg.CapellaForkEpoch = 0
		cfg.DenebForkEpoch = 0
		cfg.ElectraForkEpoch = 0
		cfg.FuluForkEpoch = 0
		cfg.GloasForkEpoch = cfg.FarFutureEpoch
	})
	parentSlot := parentState.Slot()
	childSlot := parentSlot + 1
	proposerState, err := parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(proposerState, childSlot))
	proposerIndex, err := proposerState.GetBeaconProposerIndexForSlot(childSlot)
	require.NoError(t, err)
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	validator, err := parentState.ValidatorForValidatorIndex(int(proposerIndex))
	require.NoError(t, err)
	var publicKey [48]byte
	copy(publicKey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	validator.SetPublicKey(publicKey)
	parentState.SetValidatorAtIndex(int(proposerIndex), validator)

	parent := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	parent.Block.Slot = parentSlot
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	block.Block.Slot = childSlot
	block.Block.ProposerIndex = proposerIndex
	block.Block.ParentRoot = parentRoot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{1})
	domain, err := parentState.GetDomain(cfg.DomainBeaconProposer, childSlot/cfg.SlotsPerEpoch)
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(block.Block, domain)
	require.NoError(t, err)
	copy(block.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())

	ctrl := gomock.NewController(t)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	syncedData := synced_data.NewSyncedDataManager(cfg, true)
	require.NoError(t, syncedData.OnHeadState(parentState))
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(childSlot).AnyTimes()
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	store := mock_services.NewForkChoiceStorageMock(t)
	peerDas := das_mock.NewMockPeerDas(ctrl)
	store.MockPeerDas = peerDas
	store.Headers[parentRoot] = parent.SignedBeaconBlockHeader().Header.Copy()
	store.Blocks[parentRoot] = parent
	store.StateAtBlockRootVal[parentRoot] = parentState
	emitter := beaconevents.NewEventEmitter()
	service := newBlockService(db, store, syncedData, clock, cfg, emitter)
	service.startELColumnsImport = func(context.Context, *cltypes.SignedBeaconBlock) {}
	processing := &onBlockErrorStore{ForkChoiceStorage: store, err: forkchoice.ErrEIP7594ColumnDataNotAvailable}
	service.forkchoiceStore = processing
	return service, block, processing, peerDas
}

func TestDrainDataColumnStoredBoundsConcurrentRefill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stored := make(chan common.Hash, 256)
		for i := range cap(stored) {
			stored <- common.Hash{byte(i)}
		}
		first := <-stored
		for range cap(stored) {
			go func() { stored <- common.Hash{0xff} }()
		}
		synctest.Wait()

		roots := drainDataColumnStored(stored, first)
		synctest.Wait()

		require.Contains(t, roots, [32]byte(first))
		require.Len(t, stored, cap(stored)-1)
	})
}

func TestScheduledBlockRetriesWhenCustodyColumnsAreStored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, _, peerDas, _ := setupELBlobsBlockService(t)
		block, root, _ := elBlobsTestBlock(t, clparams.FuluVersion, 1)
		processing := &onBlockErrorStore{
			ForkChoiceStorage: service.forkchoiceStore,
			err:               forkchoice.ErrEIP7594ColumnDataNotAvailable,
		}
		service.forkchoiceStore = processing
		service.emitter = beaconevents.NewEventEmitter()
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(true, nil)

		service.ScheduleBlockForLaterProcessing(block)
		job := serviceJob(t, service, root)
		service.processScheduledBlock(t.Context(), root, job, time.Now())
		require.Equal(t, int32(1), processing.calls.Load())
		processing.err = nil

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		go service.loop(ctx)
		synctest.Wait()
		service.emitter.NotifyDataColumnStored(root)
		synctest.Wait()

		require.Equal(t, start, time.Now(), "the ticker must not have fired")
		require.Equal(t, int32(2), processing.calls.Load())
		_, queued := service.blocksScheduledForLaterExecution.Load([32]byte(root))
		require.False(t, queued)
	})
}

func TestColumnNotificationsRetryOnlyOnceCustodyIsComplete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, _, peerDas, _ := setupELBlobsBlockService(t)
		block, root, _ := elBlobsTestBlock(t, clparams.FuluVersion, 1)
		processing := &onBlockErrorStore{
			ForkChoiceStorage: service.forkchoiceStore,
			err:               forkchoice.ErrEIP7594ColumnDataNotAvailable,
		}
		service.forkchoiceStore = processing
		service.emitter = beaconevents.NewEventEmitter()
		var available atomic.Bool
		var availabilityChecks atomic.Int32
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).DoAndReturn(func(uint64, common.Hash) (bool, error) {
			availabilityChecks.Add(1)
			return available.Load(), nil
		}).Times(2)

		service.ScheduleBlockForLaterProcessing(block)
		job := serviceJob(t, service, root)
		service.processScheduledBlock(t.Context(), root, job, time.Now())
		processing.err = nil

		for range 8 {
			service.emitter.NotifyDataColumnStored(root)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go service.loop(ctx)
		synctest.Wait()
		require.Equal(t, int32(1), processing.calls.Load())
		require.Equal(t, int32(1), availabilityChecks.Load())

		available.Store(true)
		for range 8 {
			service.emitter.NotifyDataColumnStored(root)
		}
		synctest.Wait()
		require.Equal(t, int32(2), processing.calls.Load())
		require.Equal(t, int32(2), availabilityChecks.Load())
	})
}

func TestColumnNotificationDoesNotRetryJobsNotWaitingForColumns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, _, _, _ := setupELBlobsBlockService(t)
		block, root, _ := elBlobsTestBlock(t, clparams.FuluVersion, 1)
		processing := &onBlockErrorStore{
			ForkChoiceStorage: service.forkchoiceStore,
			err:               errors.New("transient import error"),
		}
		service.forkchoiceStore = processing
		service.emitter = beaconevents.NewEventEmitter()

		service.ScheduleBlockForLaterProcessing(block)
		job := serviceJob(t, service, root)
		service.processScheduledBlock(t.Context(), root, job, time.Now())
		job.mu.Lock()
		require.False(t, job.waitsForColumns)
		job.mu.Unlock()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go service.loop(ctx)
		synctest.Wait()
		service.emitter.NotifyDataColumnStored(root)
		synctest.Wait()

		require.Equal(t, int32(1), processing.calls.Load())
	})
}
