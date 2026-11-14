package services

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

func elBlobsTestBlock(t *testing.T, version clparams.StateVersion, commitments int) (*cltypes.SignedBeaconBlock, common.Hash, []common.Hash) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, version)
	block.Block.Slot = 100
	for i := range commitments {
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{byte(i + 1)})
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return block, root, versionedHashesOf(t, block)
}

type recordingColumnsPublisher struct {
	published  [][]*cltypes.DataColumnSidecar
	contextErr []error
}

func (r *recordingColumnsPublisher) PublishColumns(ctx context.Context, sidecars []*cltypes.DataColumnSidecar) {
	r.published = append(r.published, sidecars)
	r.contextErr = append(r.contextErr, ctx.Err())
}

func setupELBlobsBlockService(t *testing.T) (*blockService, *execution_client.MockExecutionEngine, *das_mock.MockPeerDas, *recordingColumnsPublisher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	service, _, _, forkchoiceMock := setupBlockService(t, ctrl)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	peerDas := das_mock.NewMockPeerDas(ctrl)
	forkchoiceMock.MockPeerDas = peerDas
	columns := &recordingColumnsPublisher{}
	b := service.(*blockService)
	b.engine = engine
	b.dataColumns = columns
	return b, engine, peerDas, columns
}

func TestImportColumnsFromELBlobsPublishesStoredColumns(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			b, engine, peerDas, columns := setupELBlobsBlockService(t)
			block, root, hashes := elBlobsTestBlock(t, version, 2)
			blobs := [][]byte{{1}, {2}}
			proofs := [][][]byte{{{1}}, {{2}}}
			stored := []*cltypes.DataColumnSidecar{cltypes.NewDataColumnSidecarWithVersion(version)}

			peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
			engine.EXPECT().GetBlobs(gomock.Any(), hashes, version).Return(blobs, proofs, nil)
			peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs).Return(stored, nil)

			b.importColumnsFromELBlobs(t.Context(), block)

			require.Equal(t, [][]*cltypes.DataColumnSidecar{stored}, columns.published)
		})
	}
}

func TestImportColumnsFromELBlobsDoesNotPublishFailedImport(t *testing.T) {
	b, engine, peerDas, columns := setupELBlobsBlockService(t)
	block, root, hashes := elBlobsTestBlock(t, clparams.FuluVersion, 2)
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{1}}, {{2}}}

	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
	engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.FuluVersion).Return(blobs, proofs, nil)
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs).Return(nil, errors.New("incomplete blobs"))

	b.importColumnsFromELBlobs(t.Context(), block)

	require.Empty(t, columns.published)
}

func TestImportColumnsFromELBlobsSkipsBlockAboveBlobLimit(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			b, _, _, _ := setupELBlobsBlockService(t)
			maxBlobs := b.beaconCfg.GetBlobParameters(100 / b.beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock
			block, _, _ := elBlobsTestBlock(t, version, int(maxBlobs)+1)

			b.importColumnsFromELBlobs(t.Context(), block)
		})
	}
}

// The limit comes from the blob schedule entry of the block's epoch, not from an earlier fork.
func TestImportColumnsFromELBlobsUsesTheBlobLimitOfTheBlockEpoch(t *testing.T) {
	b, _, _, _ := setupELBlobsBlockService(t)
	cfg := *b.beaconCfg
	block, _, _ := elBlobsTestBlock(t, clparams.FuluVersion, 5)
	epoch := block.Block.Slot / cfg.SlotsPerEpoch
	require.Greater(t, cfg.GetBlobParameters(0).MaxBlobsPerBlock, uint64(5))
	cfg.BlobSchedule = append(slices.Clone(cfg.BlobSchedule), clparams.BlobParameters{Epoch: epoch, MaxBlobsPerBlock: 4})
	b.beaconCfg = &cfg

	b.importColumnsFromELBlobs(t.Context(), block)
}

func TestImportColumnsFromELBlobsPublishesAfterTheImportContextExpires(t *testing.T) {
	b, engine, peerDas, columns := setupELBlobsBlockService(t)
	cfg := *b.beaconCfg
	cfg.SecondsPerSlot = 0
	b.beaconCfg = &cfg
	block, root, hashes := elBlobsTestBlock(t, clparams.FuluVersion, 2)
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{1}}, {{2}}}
	built := []*cltypes.DataColumnSidecar{cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)}

	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
	engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.FuluVersion).Return(blobs, proofs, nil)
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs).Return(built, nil)

	b.importColumnsFromELBlobs(t.Context(), block)

	require.Equal(t, [][]*cltypes.DataColumnSidecar{built}, columns.published)
	require.Equal(t, []error{nil}, columns.contextErr)
}

func TestImportColumnsFromELBlobsSkipsWhenNotNeeded(t *testing.T) {
	t.Run("data available", func(t *testing.T) {
		b, _, peerDas, _ := setupELBlobsBlockService(t)
		block, root, _ := elBlobsTestBlock(t, clparams.FuluVersion, 2)
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(true, nil)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("gloas data available", func(t *testing.T) {
		b, _, peerDas, _ := setupELBlobsBlockService(t)
		block, root, _ := elBlobsTestBlock(t, clparams.GloasVersion, 2)
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(true, nil)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("no blobs", func(t *testing.T) {
		b, _, _, _ := setupELBlobsBlockService(t)
		block, _, _ := elBlobsTestBlock(t, clparams.FuluVersion, 0)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("gloas no blobs", func(t *testing.T) {
		b, _, _, _ := setupELBlobsBlockService(t)
		block, _, _ := elBlobsTestBlock(t, clparams.GloasVersion, 0)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("gloas without bid", func(t *testing.T) {
		b, _, _, _ := setupELBlobsBlockService(t)
		block, _, _ := elBlobsTestBlock(t, clparams.GloasVersion, 0)
		block.Block.Body.SignedExecutionPayloadBid = nil
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("before fulu", func(t *testing.T) {
		b, _, _, _ := setupELBlobsBlockService(t)
		block, _, _ := elBlobsTestBlock(t, clparams.ElectraVersion, 2)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("no engine", func(t *testing.T) {
		b, _, _, _ := setupELBlobsBlockService(t)
		b.engine = nil
		block, _, _ := elBlobsTestBlock(t, clparams.FuluVersion, 2)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("gloas no engine", func(t *testing.T) {
		b, _, _, _ := setupELBlobsBlockService(t)
		b.engine = nil
		block, _, _ := elBlobsTestBlock(t, clparams.GloasVersion, 2)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("engine error", func(t *testing.T) {
		b, engine, peerDas, _ := setupELBlobsBlockService(t)
		block, root, hashes := elBlobsTestBlock(t, clparams.FuluVersion, 2)
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
		engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.FuluVersion).Return(nil, nil, errors.New("unavailable"))
		b.importColumnsFromELBlobs(t.Context(), block)
	})
}

type elImportStart struct {
	root common.Hash
	// admitted reports whether fork choice already held the block when the import started.
	admitted bool
}

func recordELColumnsImportStarts(t *testing.T, b *blockService, forkChoice forkchoice.ForkChoiceStorage) *[]elImportStart {
	t.Helper()
	starts := &[]elImportStart{}
	b.startELColumnsImport = func(_ context.Context, block *cltypes.SignedBeaconBlock) {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		_, hasHeader := forkChoice.GetHeader(root)
		_, hasBlock := forkChoice.GetBlock(root)
		*starts = append(*starts, elImportStart{root: root, admitted: hasHeader && hasBlock})
	}
	return starts
}

func versionedHashesOf(t *testing.T, block *cltypes.SignedBeaconBlock) []common.Hash {
	t.Helper()
	commitments := block.GetBlobKzgCommitments()
	hashes := make([]common.Hash, commitments.Len())
	for i := range hashes {
		hash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*commitments.Get(i)))
		require.NoError(t, err)
		hashes[i] = hash
	}
	return hashes
}

// A queued block that fork choice rejects on the first attempt is admitted by a later one, and only that one starts the import.
func TestQueuedUnknownParentGloasBlockStartsELColumnsImportOnce(t *testing.T) {
	service, _, store, root, job := parkUnknownParentBlockWithBlobsForRetry(t, true, 2)
	transient := errors.New("transient")
	processing := &onBlockErrorStore{ForkChoiceStorage: store, err: transient}
	service.forkchoiceStore = processing
	starts := recordELColumnsImportStarts(t, service, processing)

	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, transient)
	require.Empty(t, *starts)

	processing.err = nil
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.NoError(t, job.lastAttempt.err)
	require.Equal(t, []elImportStart{{root: root, admitted: true}}, *starts)
}

// Equivocating blocks of one proposer and slot, queued on the same unknown parent, start a single EL import.
func TestQueuedEquivocatingGloasBlocksStartOneELColumnsImport(t *testing.T) {
	service, child, store, root, job := parkUnknownParentBlockWithBlobsForRetry(t, true, 2)
	processing := &onBlockErrorStore{ForkChoiceStorage: store}
	service.forkchoiceStore = processing
	encoded, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	other := cltypes.NewSignedBeaconBlock(service.beaconCfg, child.Version())
	require.NoError(t, other.DecodeSSZ(encoded, int(child.Version())))
	other.Block.Body.Graffiti = common.Hash{0x01}
	otherJob, _ := service.scheduleBlockForLaterProcessing(other, nil, false, false)
	otherRoot, err := other.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, child.Block.ProposerIndex, other.Block.ProposerIndex)
	require.NotEqual(t, root, common.Hash(otherRoot))
	starts := recordELColumnsImportStarts(t, service, processing)

	service.processScheduledBlock(t.Context(), root, job, time.Now())
	service.processScheduledBlock(t.Context(), otherRoot, otherJob, time.Now())

	require.NoError(t, otherJob.lastAttempt.err)
	require.Equal(t, []elImportStart{{root: root, admitted: true}}, *starts)
}

// A block whose blob count is out of range, e.g. queued on an unknown parent before gossip validation checked it, does
// not take the import of its proposer and slot from a valid block.
func TestBlockWithBlobCountOutOfRangeDoesNotTakeTheELColumnsImport(t *testing.T) {
	outOfRange := map[string]func(maxBlobs int) int{
		"no blobs":    func(int) int { return 0 },
		"above limit": func(maxBlobs int) int { return maxBlobs + 1 },
	}
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		for name, blobs := range outOfRange {
			t.Run(version.String()+"/"+name, func(t *testing.T) {
				b, _, _, _ := setupELBlobsBlockService(t)
				maxBlobs := int(b.beaconCfg.GetBlobParameters(100 / b.beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock)
				invalid, _, _ := elBlobsTestBlock(t, version, blobs(maxBlobs))
				valid, _, _ := elBlobsTestBlock(t, version, 2)
				require.Equal(t, blockGossipKey(invalid), blockGossipKey(valid))
				var started []*cltypes.SignedBeaconBlock
				b.startELColumnsImport = func(_ context.Context, block *cltypes.SignedBeaconBlock) {
					started = append(started, block)
				}

				b.startELColumnsImportOnce(t.Context(), invalid)
				b.startELColumnsImportOnce(t.Context(), valid)

				require.Equal(t, []*cltypes.SignedBeaconBlock{valid}, started)
			})
		}
	}
}

func TestQueuedGloasBlockWithWrongProposerDoesNotStartELColumnsImport(t *testing.T) {
	service, _, _, root, job := parkUnknownParentBlockWithBlobsForRetry(t, false, 2)
	started := 0
	service.startELColumnsImport = func(context.Context, *cltypes.SignedBeaconBlock) { started++ }

	service.processScheduledBlock(t.Context(), root, job, time.Now())

	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrBlockInvalid)
	require.Zero(t, started)
}

func parkUnknownParentFuluBlockForRetry(t *testing.T, matchingProposer bool) (*blockService, *cltypes.SignedBeaconBlock, *onBlockErrorStore, common.Hash, *blockJob) {
	t.Helper()
	service, child, processing, _ := newFuluColumnGossipFixture(t)
	root, job := parkUnknownParentBlock(t, service, child, processing.ForkChoiceStorage.(*mock_services.ForkChoiceStorageMock), matchingProposer)
	return service, child, processing, root, job
}

// A queued Fulu block starts the import once its proposer is authenticated, whatever fork choice then answers.
func TestQueuedUnknownParentFuluBlockStartsELColumnsImportOnce(t *testing.T) {
	service, _, processing, root, job := parkUnknownParentFuluBlockForRetry(t, true)
	transient := errors.New("transient")
	processing.err = transient
	starts := recordELColumnsImportStarts(t, service, processing)

	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, transient)
	require.Equal(t, []elImportStart{{root: root}}, *starts)

	processing.err = forkchoice.ErrEIP7594ColumnDataNotAvailable
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrEIP7594ColumnDataNotAvailable)
	require.Len(t, *starts, 1)
}

func TestQueuedEquivocatingFuluBlocksStartOneELColumnsImport(t *testing.T) {
	service, child, processing, root, job := parkUnknownParentFuluBlockForRetry(t, true)
	encoded, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	other := cltypes.NewSignedBeaconBlock(service.beaconCfg, child.Version())
	require.NoError(t, other.DecodeSSZ(encoded, int(child.Version())))
	other.Block.Body.Graffiti = common.Hash{0x01}
	otherJob, _ := service.scheduleBlockForLaterProcessing(other, nil, false, false)
	otherRoot, err := other.Block.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, root, common.Hash(otherRoot))
	starts := recordELColumnsImportStarts(t, service, processing)

	service.processScheduledBlock(t.Context(), root, job, time.Now())
	service.processScheduledBlock(t.Context(), otherRoot, otherJob, time.Now())

	require.ErrorIs(t, otherJob.lastAttempt.err, forkchoice.ErrEIP7594ColumnDataNotAvailable)
	require.Equal(t, []elImportStart{{root: root}}, *starts)
}

func TestQueuedFuluBlockWithWrongProposerDoesNotStartELColumnsImport(t *testing.T) {
	service, _, _, root, job := parkUnknownParentFuluBlockForRetry(t, false)
	started := 0
	service.startELColumnsImport = func(context.Context, *cltypes.SignedBeaconBlock) { started++ }

	service.processScheduledBlock(t.Context(), root, job, time.Now())

	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrBlockInvalid)
	require.Zero(t, started)
}

func TestProcessMessageStartsELColumnsImportForValidGossipBlock(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixtureWithBlobs(t, nil, 2)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	b := service.(*blockService)
	processing := &onBlockErrorStore{ForkChoiceStorage: fcu}
	b.forkchoiceStore = processing
	starts := recordELColumnsImportStarts(t, b, processing)
	root, err := child.Block.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, service.ProcessMessage(t.Context(), nil, child))

	require.Equal(t, []elImportStart{{root: root, admitted: true}}, *starts)
}

// A gossip block and an equivocating block of the same proposer and slot queued on an unknown parent start one import:
// the queued block never adds its key to the seen-blocks cache, so only the import claim stops the second one.
func TestGossipAndQueuedEquivocatingBlocksStartOneELColumnsImport(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixtureWithBlobs(t, nil, 2)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	b := service.(*blockService)
	processing := &onBlockErrorStore{ForkChoiceStorage: fcu}
	b.forkchoiceStore = processing
	encoded, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	other := cltypes.NewSignedBeaconBlock(b.beaconCfg, child.Version())
	require.NoError(t, other.DecodeSSZ(encoded, int(child.Version())))
	other.Block.Body.Graffiti = common.Hash{0x01}
	otherRoot, err := other.Block.HashSSZ()
	require.NoError(t, err)
	root, err := child.Block.HashSSZ()
	require.NoError(t, err)
	starts := recordELColumnsImportStarts(t, b, processing)

	require.NoError(t, service.ProcessMessage(t.Context(), nil, child))
	otherJob, _ := b.scheduleBlockForLaterProcessing(other, nil, false, false)
	b.processScheduledBlock(t.Context(), otherRoot, otherJob, time.Now())

	require.NotNil(t, otherJob.lastAttempt)
	require.Equal(t, []elImportStart{{root: root, admitted: true}}, *starts)
}

func TestProcessMessageDoesNotStartELColumnsImportForInvalidGossipBlock(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixtureWithBlobs(t, nil, 2)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	child.Signature[0] ^= 0xff
	b := service.(*blockService)
	started := 0
	b.startELColumnsImport = func(context.Context, *cltypes.SignedBeaconBlock) { started++ }

	require.Error(t, service.ProcessMessage(t.Context(), nil, child))

	require.Zero(t, started)
}

type gloasELImport struct {
	service    *blockService
	child      *cltypes.SignedBeaconBlock
	root       common.Hash
	hashes     []common.Hash
	fcu        *mock_services.ForkChoiceStorageMock
	processing *onBlockErrorStore
	engine     *execution_client.MockExecutionEngine
	peerDas    *das_mock.MockPeerDas
}

func newGloasELImport(t *testing.T, blobs int) *gloasELImport {
	t.Helper()
	api, child, fcu, parentRoot, _ := newGloasGossipValidationFixtureWithBlobs(t, nil, blobs)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	ctrl := gomock.NewController(t)
	f := &gloasELImport{
		service:    api.(*blockService),
		child:      child,
		hashes:     versionedHashesOf(t, child),
		fcu:        fcu,
		processing: &onBlockErrorStore{ForkChoiceStorage: fcu},
		engine:     execution_client.NewMockExecutionEngine(ctrl),
		peerDas:    das_mock.NewMockPeerDas(ctrl),
	}
	root, err := child.Block.HashSSZ()
	require.NoError(t, err)
	f.root = root
	fcu.MockPeerDas = f.peerDas
	f.service.forkchoiceStore = f.processing
	f.service.engine = f.engine
	return f
}

// runImportAtStart makes each started import run to completion before the start hook returns.
func (f *gloasELImport) runImportAtStart(t *testing.T) *[]elImportStart {
	starts := recordELColumnsImportStarts(t, f.service, f.processing)
	record := f.service.startELColumnsImport
	f.service.startELColumnsImport = func(ctx context.Context, block *cltypes.SignedBeaconBlock) {
		record(ctx, block)
		f.service.importColumnsFromELBlobs(ctx, block)
	}
	return starts
}

func (f *gloasELImport) expectImport(built []*cltypes.DataColumnSidecar) {
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{1}}, {{2}}}
	f.peerDas.EXPECT().IsDataAvailable(f.child.Block.Slot, f.root).Return(false, nil)
	f.engine.EXPECT().GetBlobs(gomock.Any(), f.hashes, clparams.GloasVersion).Return(blobs, proofs, nil)
	f.peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), f.child, blobs, proofs).Return(built, nil)
}

func requireBlockNotStored(t *testing.T, service *blockService, root common.Hash) {
	t.Helper()
	require.NoError(t, service.db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.Nil(t, slot)
		return nil
	}))
}

func TestProcessMessagePublishesGloasELColumnsThroughTheColumnService(t *testing.T) {
	f := newGloasELImport(t, 2)
	columnService, publisher, clock, _ := newELPublishTestService(t)
	defer func(f func(uint64) uint64) { computeSubnetForDataColumnSidecar = f }(computeSubnetForDataColumnSidecar)
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % columnService.cfg.DataColumnSidecarSubnetCount }
	f.service.dataColumns = columnService
	f.runImportAtStart(t)
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	sidecar.Index, sidecar.Slot, sidecar.BeaconBlockRoot = 3, f.child.Block.Slot, f.root
	f.expectImport([]*cltypes.DataColumnSidecar{sidecar})
	encoded, err := sidecar.EncodeSSZ(nil)
	require.NoError(t, err)
	digest := common.Bytes4{1, 2, 3, 4}
	clock.EXPECT().ComputeForkDigest(f.child.Block.Slot/columnService.cfg.SlotsPerEpoch).Return(digest, nil)
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), digest, gossip.TopicNameDataColumnSidecar(computeSubnetForDataColumnSidecar(3)), encoded).Return(nil).Times(1)

	require.NoError(t, f.service.ProcessMessage(t.Context(), nil, f.child))

	_, seen := columnService.seenGloasSidecar.Get(seenGloasSidecarKey{beaconBlockRoot: f.root, index: 3})
	require.True(t, seen)
}

func TestProcessMessageStartsGloasELColumnsImportOnceWhenAcceptedBlockWriteFails(t *testing.T) {
	f := newGloasELImport(t, 2)
	db := &blockDBError{RwDB: f.service.db, updateErr: errors.New("database unavailable")}
	f.service.db = db
	starts := recordELColumnsImportStarts(t, f.service, f.processing)

	require.NoError(t, f.service.ProcessMessage(t.Context(), nil, f.child), "an accepted block can propagate while its write is retried")
	require.Equal(t, []elImportStart{{root: f.root, admitted: true}}, *starts)

	db.updateErr = nil
	job := serviceJob(t, f.service, f.root)
	f.service.processScheduledBlock(t.Context(), f.root, job, time.Now())
	require.NoError(t, job.lastAttempt.err)
	require.Len(t, *starts, 1)
}

func TestProcessMessageStartsGloasELColumnsImportOnceWhenCallerCancelsDuringBlockWrite(t *testing.T) {
	f := newGloasELImport(t, 2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	db := &blockDBError{RwDB: f.service.db, beforeUpdate: cancel}
	f.service.db = db
	starts := recordELColumnsImportStarts(t, f.service, f.processing)

	require.NoError(t, f.service.ProcessMessage(ctx, nil, f.child), "an accepted block can propagate while its write is retried")
	require.Equal(t, []elImportStart{{root: f.root, admitted: true}}, *starts)

	db.beforeUpdate = nil
	job := serviceJob(t, f.service, f.root)
	f.service.processScheduledBlock(t.Context(), f.root, job, time.Now())
	require.NoError(t, job.lastAttempt.err)
	require.Len(t, *starts, 1)
}

func TestQueuedGloasBlockStartsELColumnsImportOnceWhenAcceptedBlockWriteFails(t *testing.T) {
	service, _, store, root, job := parkUnknownParentBlockWithBlobsForRetry(t, true, 2)
	processing := &onBlockErrorStore{ForkChoiceStorage: store}
	service.forkchoiceStore = processing
	db := &blockDBError{RwDB: service.db, updateErr: errors.New("database unavailable")}
	service.db = db
	starts := recordELColumnsImportStarts(t, service, processing)

	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, errBlockStorage)
	require.Equal(t, []elImportStart{{root: root, admitted: true}}, *starts)

	db.updateErr = nil
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.NoError(t, job.lastAttempt.err)
	require.Len(t, *starts, 1)
}

func TestGloasBlockRejectedByForkChoiceStartsNoELColumnsImport(t *testing.T) {
	for _, rejection := range []error{forkchoice.ErrBlockInvalid, errors.New("transient")} {
		t.Run("gossip/"+rejection.Error(), func(t *testing.T) {
			f := newGloasELImport(t, 2)
			columns := &recordingColumnsPublisher{}
			f.service.dataColumns = columns
			starts := f.runImportAtStart(t)
			f.processing.err = rejection

			require.ErrorIs(t, f.service.ProcessMessage(t.Context(), nil, f.child), rejection)

			require.Equal(t, int32(1), f.processing.calls.Load())
			require.Empty(t, *starts)
			require.Empty(t, columns.published)
			requireBlockNotStored(t, f.service, f.root)
		})
		t.Run("queued/"+rejection.Error(), func(t *testing.T) {
			service, _, store, root, job := parkUnknownParentBlockWithBlobsForRetry(t, true, 2)
			processing := &onBlockErrorStore{ForkChoiceStorage: store, err: rejection}
			service.forkchoiceStore = processing
			starts := recordELColumnsImportStarts(t, service, processing)

			service.processScheduledBlock(t.Context(), root, job, time.Now())

			require.ErrorIs(t, job.lastAttempt.err, rejection)
			require.Equal(t, int32(1), processing.calls.Load())
			require.Empty(t, *starts)
			requireBlockNotStored(t, service, root)
		})
	}
}

func TestGossipGloasBlockWaitingForItsParentEnvelopeStartsELColumnsImportWhenRetryAdmitsIt(t *testing.T) {
	f := newGloasELImport(t, 2)
	starts := recordELColumnsImportStarts(t, f.service, f.processing)
	f.processing.err = forkchoice.ErrParentEnvelopePending

	require.NoError(t, f.service.ProcessMessage(t.Context(), nil, f.child))
	require.Empty(t, *starts)

	f.processing.err = nil
	job := serviceJob(t, f.service, f.root)
	f.service.processScheduledBlock(t.Context(), f.root, job, time.Now())

	require.NoError(t, job.lastAttempt.err)
	require.Equal(t, []elImportStart{{root: f.root, admitted: true}}, *starts)
}

func TestProcessMessageDoesNotStartELColumnsImportForGloasBlockWithoutBlobs(t *testing.T) {
	f := newGloasELImport(t, 0)
	starts := recordELColumnsImportStarts(t, f.service, f.processing)

	require.NoError(t, f.service.ProcessMessage(t.Context(), nil, f.child))

	require.Empty(t, *starts)
}

// The proposer builds and publishes the columns of a block it published through the Beacon API itself.
func TestPublishedGloasBlockStartsNoELColumnsImport(t *testing.T) {
	f := newGloasELImport(t, 2)
	starts := recordELColumnsImportStarts(t, f.service, f.processing)
	stores := 0
	f.service.SchedulePublishedBlockForLaterProcessing(f.child, func(ctx context.Context) error {
		stores++
		if stores > 1 {
			return nil
		}
		if err := f.processing.OnBlock(ctx, f.child, true, true, false); err != nil {
			return err
		}
		return &AcceptedBlockWriteError{Block: f.child, Err: errors.New("database unavailable")}
	})
	job := serviceJob(t, f.service, f.root)

	f.service.processScheduledBlock(t.Context(), f.root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, errBlockStorage)
	f.service.processScheduledBlock(t.Context(), f.root, job, time.Now())
	require.NoError(t, job.lastAttempt.err)

	require.Empty(t, *starts)
}

func TestProcessMessageStartsFuluELColumnsImportBeforeAdmission(t *testing.T) {
	service, block, processing, _ := newFuluColumnGossipFixture(t)
	processing.err = nil
	starts := recordELColumnsImportStarts(t, service, processing)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))

	require.Equal(t, []elImportStart{{root: root, admitted: false}}, *starts)
	require.Equal(t, int32(1), processing.calls.Load())
}
