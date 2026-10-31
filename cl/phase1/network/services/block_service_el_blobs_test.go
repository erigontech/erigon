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
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

func elBlobsTestBlock(t *testing.T, version clparams.StateVersion, commitments int) (*cltypes.SignedBeaconBlock, common.Hash, []common.Hash) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, version)
	block.Block.Slot = 100
	hashes := make([]common.Hash, commitments)
	for i := range commitments {
		commitment := cltypes.KZGCommitment{byte(i + 1)}
		block.Block.Body.BlobKzgCommitments.Append(&commitment)
		hash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(commitment))
		require.NoError(t, err)
		hashes[i] = hash
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return block, root, hashes
}

type recordingColumnsPublisher struct {
	published  [][]*cltypes.DataColumnSidecar
	contextErr []error
}

func (r *recordingColumnsPublisher) PublishColumnsFromEL(ctx context.Context, sidecars []*cltypes.DataColumnSidecar) {
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
	b, engine, peerDas, columns := setupELBlobsBlockService(t)
	block, root, hashes := elBlobsTestBlock(t, clparams.FuluVersion, 2)
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{1}}, {{2}}}
	stored := []*cltypes.DataColumnSidecar{cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)}

	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
	engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.FuluVersion).Return(blobs, proofs, nil)
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs).Return(stored, nil)

	b.importColumnsFromELBlobs(t.Context(), block)

	require.Equal(t, [][]*cltypes.DataColumnSidecar{stored}, columns.published)
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
	b, _, _, _ := setupELBlobsBlockService(t)
	maxBlobs := b.beaconCfg.GetBlobParameters(100 / b.beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock
	block, _, _ := elBlobsTestBlock(t, clparams.FuluVersion, int(maxBlobs)+1)

	b.importColumnsFromELBlobs(t.Context(), block)
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
	t.Run("no blobs", func(t *testing.T) {
		b, _, _, _ := setupELBlobsBlockService(t)
		block, _, _ := elBlobsTestBlock(t, clparams.FuluVersion, 0)
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
	t.Run("engine error", func(t *testing.T) {
		b, engine, peerDas, _ := setupELBlobsBlockService(t)
		block, root, hashes := elBlobsTestBlock(t, clparams.FuluVersion, 2)
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
		engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.FuluVersion).Return(nil, nil, errors.New("unavailable"))
		b.importColumnsFromELBlobs(t.Context(), block)
	})
}

func TestQueuedUnknownParentBlockStartsELColumnsImportOnce(t *testing.T) {
	service, child, store, root, job := parkUnknownParentBlockForRetry(t, true)
	var started []*cltypes.SignedBeaconBlock
	service.startELColumnsImport = func(_ context.Context, block *cltypes.SignedBeaconBlock) {
		started = append(started, block)
	}
	transient := errors.New("transient")
	store.OnBlockErr = transient

	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, transient)
	require.Equal(t, []*cltypes.SignedBeaconBlock{child}, started)

	// The retry authenticates the block again; the import must not start twice.
	store.OnBlockErr = forkchoice.ErrEIP7594ColumnDataNotAvailable
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrEIP7594ColumnDataNotAvailable)
	require.Len(t, started, 1)
}

// Equivocating blocks of one proposer and slot, queued on the same unknown parent, start a single EL import.
func TestQueuedEquivocatingBlocksStartOneELColumnsImport(t *testing.T) {
	service, child, _, root, job := parkUnknownParentBlockForRetry(t, true)
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
	started := 0
	service.startELColumnsImport = func(context.Context, *cltypes.SignedBeaconBlock) { started++ }

	service.processScheduledBlock(t.Context(), root, job, time.Now())
	service.processScheduledBlock(t.Context(), otherRoot, otherJob, time.Now())

	require.ErrorIs(t, otherJob.lastAttempt.err, forkchoice.ErrEIP7594ColumnDataNotAvailable)
	require.Equal(t, 1, started)
}

func TestQueuedBlockWithWrongProposerDoesNotStartELColumnsImport(t *testing.T) {
	service, _, _, root, job := parkUnknownParentBlockForRetry(t, false)
	started := 0
	service.startELColumnsImport = func(context.Context, *cltypes.SignedBeaconBlock) { started++ }

	service.processScheduledBlock(t.Context(), root, job, time.Now())

	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrBlockInvalid)
	require.Zero(t, started)
}

func TestProcessMessageStartsELColumnsImportForValidGossipBlock(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	b := service.(*blockService)
	var started []*cltypes.SignedBeaconBlock
	b.startELColumnsImport = func(_ context.Context, block *cltypes.SignedBeaconBlock) {
		started = append(started, block)
	}

	_ = service.ProcessMessage(t.Context(), nil, child)

	require.Equal(t, []*cltypes.SignedBeaconBlock{child}, started)
}

func TestProcessMessageDoesNotStartELColumnsImportForInvalidGossipBlock(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	child.Signature[0] ^= 0xff
	b := service.(*blockService)
	started := 0
	b.startELColumnsImport = func(context.Context, *cltypes.SignedBeaconBlock) { started++ }

	require.Error(t, service.ProcessMessage(t.Context(), nil, child))

	require.Zero(t, started)
}
