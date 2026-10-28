package services

import (
	"context"
	"errors"
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

func setupELBlobsBlockService(t *testing.T) (*blockService, *execution_client.MockExecutionEngine, *das_mock.MockPeerDas) {
	t.Helper()
	ctrl := gomock.NewController(t)
	service, _, _, forkchoiceMock := setupBlockService(t, ctrl)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	peerDas := das_mock.NewMockPeerDas(ctrl)
	forkchoiceMock.MockPeerDas = peerDas
	b := service.(*blockService)
	b.engine = engine
	return b, engine, peerDas
}

func TestImportColumnsFromELBlobs(t *testing.T) {
	b, engine, peerDas := setupELBlobsBlockService(t)
	block, root, hashes := elBlobsTestBlock(t, clparams.FuluVersion, 2)
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{1}}, {{2}}}

	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
	engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.FuluVersion).Return(blobs, proofs, nil)
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs).Return(nil)

	b.importColumnsFromELBlobs(t.Context(), block)
}

func TestImportColumnsFromELBlobsSkipsWhenNotNeeded(t *testing.T) {
	t.Run("data available", func(t *testing.T) {
		b, _, peerDas := setupELBlobsBlockService(t)
		block, root, _ := elBlobsTestBlock(t, clparams.FuluVersion, 2)
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(true, nil)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("no blobs", func(t *testing.T) {
		b, _, _ := setupELBlobsBlockService(t)
		block, _, _ := elBlobsTestBlock(t, clparams.FuluVersion, 0)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("before fulu", func(t *testing.T) {
		b, _, _ := setupELBlobsBlockService(t)
		block, _, _ := elBlobsTestBlock(t, clparams.ElectraVersion, 2)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("no engine", func(t *testing.T) {
		b, _, _ := setupELBlobsBlockService(t)
		b.engine = nil
		block, _, _ := elBlobsTestBlock(t, clparams.FuluVersion, 2)
		b.importColumnsFromELBlobs(t.Context(), block)
	})
	t.Run("engine error", func(t *testing.T) {
		b, engine, peerDas := setupELBlobsBlockService(t)
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

func TestQueuedBlockWithWrongProposerDoesNotStartELColumnsImport(t *testing.T) {
	service, _, _, root, job := parkUnknownParentBlockForRetry(t, false)
	started := 0
	service.startELColumnsImport = func(context.Context, *cltypes.SignedBeaconBlock) { started++ }

	service.processScheduledBlock(t.Context(), root, job, time.Now())

	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrBlockInvalid)
	require.Zero(t, started)
}
