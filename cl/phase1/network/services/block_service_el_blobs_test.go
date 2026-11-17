package services

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	syncedmock "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
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
	published         [][]*cltypes.DataColumnSidecar
	contextErr        []error
	claims            [][]*cltypes.DataColumnSidecar
	claimedPublished  [][]*cltypes.DataColumnSidecar
	claimedContextErr []error
	order             []string
}

func (r *recordingColumnsPublisher) PublishColumns(ctx context.Context, sidecars []*cltypes.DataColumnSidecar) {
	r.order = append(r.order, "publish")
	r.published = append(r.published, sidecars)
	r.contextErr = append(r.contextErr, ctx.Err())
}

// ClaimColumns claims every sidecar it is given.
func (r *recordingColumnsPublisher) ClaimColumns(sidecars []*cltypes.DataColumnSidecar) func(context.Context) {
	r.order = append(r.order, "claim")
	r.claims = append(r.claims, sidecars)
	return func(ctx context.Context) {
		r.order = append(r.order, "publish claimed")
		r.claimedPublished = append(r.claimedPublished, sidecars)
		r.claimedContextErr = append(r.claimedContextErr, ctx.Err())
	}
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
	const gloasForkEpoch = 10
	gloasSlot := gloasForkEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
	for name, tc := range map[string]struct {
		version clparams.StateVersion
		slot    uint64
		claims  bool
	}{
		"last fulu slot":   {version: clparams.FuluVersion, slot: gloasSlot - 1},
		"first gloas slot": {version: clparams.GloasVersion, slot: gloasSlot, claims: true},
		"heze":             {version: clparams.HezeVersion, slot: gloasSlot + 1, claims: true},
	} {
		t.Run(name, func(t *testing.T) {
			b, engine, peerDas, columns := setupELBlobsBlockService(t)
			cfg := *b.beaconCfg
			cfg.GloasForkEpoch = gloasForkEpoch
			b.beaconCfg = &cfg
			block, _, hashes := elBlobsTestBlock(t, tc.version, 2)
			block.Block.Slot = tc.slot
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			blobs := [][]byte{{1}, {2}}
			proofs := [][][]byte{{{1}}, {{2}}}
			stored := []*cltypes.DataColumnSidecar{cltypes.NewDataColumnSidecarWithVersion(tc.version)}

			peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
			engine.EXPECT().GetBlobs(gomock.Any(), hashes, tc.version).Return(blobs, proofs, nil)
			peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs, gomock.Any()).DoAndReturn(
				func(_ context.Context, _ *cltypes.SignedBeaconBlock, _ [][]byte, _ [][][]byte, claim func([]*cltypes.DataColumnSidecar)) ([]*cltypes.DataColumnSidecar, error) {
					require.Equal(t, tc.claims, claim != nil)
					if claim != nil {
						claim(stored)
					}
					columns.order = append(columns.order, "batch")
					return stored, nil
				})

			b.importColumnsFromELBlobs(t.Context(), block)

			if tc.claims {
				require.Equal(t, []string{"claim", "batch", "publish claimed", "publish"}, columns.order)
				require.Equal(t, [][]*cltypes.DataColumnSidecar{stored}, columns.claims)
				require.Equal(t, [][]*cltypes.DataColumnSidecar{stored}, columns.published)
				return
			}
			require.Equal(t, []string{"batch", "publish"}, columns.order)
			require.Equal(t, [][]*cltypes.DataColumnSidecar{stored}, columns.published)
			require.Empty(t, columns.claims)
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
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs, gomock.Any()).Return(nil, errors.New("incomplete blobs"))

	b.importColumnsFromELBlobs(t.Context(), block)

	require.Empty(t, columns.published)
}

func requireSameSidecars(t *testing.T, expected, actual []*cltypes.DataColumnSidecar) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for i := range expected {
		require.Same(t, expected[i], actual[i])
	}
}

func TestGloasELImportPublishesClaimedColumnsAfterAFailedWrite(t *testing.T) {
	b, engine, peerDas, columns := setupELBlobsBlockService(t)
	block, root, hashes := elBlobsTestBlock(t, clparams.GloasVersion, 2)
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{1}}, {{2}}}
	stored := []*cltypes.DataColumnSidecar{cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)}

	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
	engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.GloasVersion).Return(blobs, proofs, nil)
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *cltypes.SignedBeaconBlock, _ [][]byte, _ [][][]byte, claim func([]*cltypes.DataColumnSidecar)) ([]*cltypes.DataColumnSidecar, error) {
			if claim != nil {
				claim(stored)
			}
			return stored, errors.New("write failed")
		})

	b.importColumnsFromELBlobs(t.Context(), block)

	require.Len(t, columns.claimedPublished, 1)
	requireSameSidecars(t, stored, columns.claimedPublished[0])
	require.Equal(t, [][]*cltypes.DataColumnSidecar{stored}, columns.published)
}

func TestGloasELImportRetriesAClaimedColumnWhosePublishFailed(t *testing.T) {
	b, engine, peerDas, _ := setupELBlobsBlockService(t)
	service, publisher, _ := newGloasClaimTestService(t)
	b.dataColumns = service
	published := recordPublishes(publisher, errors.New("topic not found"))
	block, root, hashes := elBlobsTestBlock(t, clparams.GloasVersion, 2)
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{1}}, {{2}}}
	failed, succeeded := elBuiltGloasSidecar(100, 3), elBuiltGloasSidecar(100, 5)
	built := []*cltypes.DataColumnSidecar{failed, succeeded}

	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
	engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.GloasVersion).Return(blobs, proofs, nil)
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *cltypes.SignedBeaconBlock, _ [][]byte, _ [][][]byte, claim func([]*cltypes.DataColumnSidecar)) ([]*cltypes.DataColumnSidecar, error) {
			claim(built)
			return built, nil
		})

	b.importColumnsFromELBlobs(t.Context(), block)

	requirePublished(t, *published, failed, succeeded, failed)
	require.True(t, service.seenGloas.contains(gloasSeenKey(failed)))
}

func logRecordsDuring(f func()) []*log.Record {
	records := make(chan *log.Record, 64)
	previous := log.Root().GetHandler()
	log.Root().SetHandler(log.FilterHandler(func(r *log.Record) bool {
		return strings.HasPrefix(r.Msg, "[blockService]")
	}, log.ChannelHandler(records)))
	defer log.Root().SetHandler(previous)
	f()
	var captured []*log.Record
	for {
		select {
		case record := <-records:
			captured = append(captured, record)
		default:
			return captured
		}
	}
}

func TestImportColumnsFromELBlobsLogsAFailedImport(t *testing.T) {
	importErr := errors.New("import failed")
	for name, tc := range map[string]struct {
		version      clparams.StateVersion
		built        bool
		expectedLvl  log.Lvl
		expectedText string
	}{
		"gloas columns built then write failed": {version: clparams.GloasVersion, built: true, expectedLvl: log.LvlWarn, expectedText: "[blockService] EL-built columns failed to be stored"},
		"gloas columns not built":               {version: clparams.GloasVersion, expectedLvl: log.LvlDebug, expectedText: "[blockService] columns not imported from EL blobs"},
		"fulu write failed":                     {version: clparams.FuluVersion, expectedLvl: log.LvlDebug, expectedText: "[blockService] columns not imported from EL blobs"},
	} {
		t.Run(name, func(t *testing.T) {
			b, engine, peerDas, _ := setupELBlobsBlockService(t)
			block, root, hashes := elBlobsTestBlock(t, tc.version, 2)
			blobs := [][]byte{{1}, {2}}
			proofs := [][][]byte{{{1}}, {{2}}}
			stored := []*cltypes.DataColumnSidecar{cltypes.NewDataColumnSidecarWithVersion(tc.version)}

			peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
			engine.EXPECT().GetBlobs(gomock.Any(), hashes, tc.version).Return(blobs, proofs, nil)
			peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs, gomock.Any()).DoAndReturn(
				func(_ context.Context, _ *cltypes.SignedBeaconBlock, _ [][]byte, _ [][][]byte, claim func([]*cltypes.DataColumnSidecar)) ([]*cltypes.DataColumnSidecar, error) {
					if !tc.built {
						return nil, importErr
					}
					claim(stored)
					return stored, importErr
				})

			records := logRecordsDuring(func() { b.importColumnsFromELBlobs(t.Context(), block) })

			require.Len(t, records, 1)
			require.Equal(t, tc.expectedLvl, records[0].Lvl)
			require.Equal(t, tc.expectedText, records[0].Msg)
			require.Equal(t, []any{"slot", block.Block.Slot, "blockRoot", root, "err", importErr}, records[0].Ctx)
		})
	}
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
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs, gomock.Any()).Return(built, nil)

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
	f.peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), f.child, blobs, proofs, gomock.Any()).Return(built, nil)
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

	seen := columnService.seenGloas.contains(seenGloasSidecarKey{beaconBlockRoot: f.root, index: 3})
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

const elImportTestTimeout = 10 * time.Second

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(elImportTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func joinWithin(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	joined := make(chan struct{})
	go func() {
		wg.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(elImportTestTimeout):
		t.Fatal("timed out joining goroutines")
	}
}

type peerDasForkChoice struct {
	*mock_services.ForkChoiceStorageMock
	peerDas das.PeerDas
}

func (f *peerDasForkChoice) GetPeerDas() das.PeerDas { return f.peerDas }

// syncBlockingFs holds the first Sync of any file, which the column batch makes while it owns the root lock.
type syncBlockingFs struct {
	afero.Fs
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *syncBlockingFs) Create(name string) (afero.File, error) {
	file, err := f.Fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &syncBlockingFile{File: file, fs: f}, nil
}

type syncBlockingFile struct {
	afero.File
	fs *syncBlockingFs
}

func (f *syncBlockingFile) Sync() error {
	f.fs.once.Do(func() {
		close(f.fs.entered)
		select {
		case <-f.fs.release:
		case <-time.After(elImportTestTimeout):
		}
	})
	return f.File.Sync()
}

var errGatedWrite = errors.New("gated write failed")

// gatedSingleWrite holds the first single-column write until released, then fails it. Batch writes pass through.
type gatedSingleWrite struct {
	blob_storage.DataColumnStorage
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func newGatedSingleWrite(storage blob_storage.DataColumnStorage) *gatedSingleWrite {
	return &gatedSingleWrite{DataColumnStorage: storage, entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *gatedSingleWrite) WriteColumnSidecars(context.Context, common.Hash, int64, *cltypes.DataColumnSidecar) error {
	s.once.Do(func() {
		close(s.entered)
		select {
		case <-s.release:
		case <-time.After(elImportTestTimeout):
		}
	})
	return errGatedWrite
}

func (s *gatedSingleWrite) open() { s.releaseOnce.Do(func() { close(s.release) }) }

type gossipResult struct {
	done chan struct{}
	err  error
}

// elImportHarness wires a real peer DAS, column storage, column service and block service, so that a Gloas EL import
// and the gossip copies of its columns run against each other. The import's batch write is held on its first Sync.
type elImportHarness struct {
	t            *testing.T
	block        *cltypes.SignedBeaconBlock
	root         common.Hash
	custody      []uint64
	wire         []*cltypes.DataColumnSidecar
	fs           *syncBlockingFs
	storage      blob_storage.DataColumnStorage
	service      *dataColumnSidecarService
	blockService *blockService
	events       chan *beaconevents.EventStream
	kzgCalls     atomic.Int32
	kzgCalled    chan struct{}
	importDone   chan struct{}
	releaseOnce  sync.Once
	wg           sync.WaitGroup
	mu           sync.Mutex
	published    [][]byte
}

// newELImportHarness builds the harness for a node with caplinConfig. wrapStorage, if not nil, wraps the column
// storage that the peer DAS and the column service share.
func newELImportHarness(t *testing.T, caplinConfig clparams.CaplinConfig, wrapStorage func(blob_storage.DataColumnStorage) blob_storage.DataColumnStorage) *elImportHarness {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch = 0, 0, 0, 0
	cfg.ElectraForkEpoch, cfg.FuluForkEpoch, cfg.GloasForkEpoch = 0, 0, 0
	block, root, hashes := elBlobsTestBlock(t, clparams.GloasVersion, 2)
	blobs := make([][]byte, 2)
	proofs := make([][][]byte, 2)
	for i := range blobs {
		blobs[i] = make([]byte, cltypes.BYTES_PER_BLOB)
		blobs[i][31] = byte(i + 1)
		proofs[i] = make([][]byte, cfg.NumberOfColumns)
		for j := range proofs[i] {
			proofs[i][j] = make([]byte, len(cltypes.KZGProof{}))
		}
	}
	cells, err := peerdasutils.ComputeBlobsCells(blobs)
	require.NoError(t, err)
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, len(blobs))
	for i := range blobs {
		cellsAndProofs[i] = peerdasutils.CellsAndKZGProofs{Blobs: cells[i], Proofs: make([]cltypes.KZGProof, cfg.NumberOfColumns)}
	}
	wire, err := peerdasutils.GetDataColumnSidecarsGloas(&cfg, block.Block.Slot, root, cellsAndProofs)
	require.NoError(t, err)

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(nodeDB.Close)
	localNode := enode.NewLocalNode(nodeDB, key)
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	state := peerdasstate.NewPeerDasState(&cfg, &networkConfig)
	state.SetLocalNodeID(localNode)
	// Custody derivation reads the global config.
	globalCfg := clparams.GetBeaconConfig()
	globalCustodyGroups := globalCfg.NumberOfCustodyGroups
	globalCfg.NumberOfCustodyGroups = cfg.NumberOfCustodyGroups
	t.Cleanup(func() { globalCfg.NumberOfCustodyGroups = globalCustodyGroups })
	custodyColumns, err := state.GetMyCustodyColumns()
	require.NoError(t, err)
	custody := make([]uint64, 0, len(custodyColumns))
	for index := range custodyColumns {
		custody = append(custody, index)
	}
	slices.Sort(custody)

	h := &elImportHarness{
		t:         t,
		block:     block,
		root:      root,
		custody:   custody,
		wire:      wire,
		fs:        &syncBlockingFs{Fs: afero.NewMemMapFs(), entered: make(chan struct{}), release: make(chan struct{})},
		events:    make(chan *beaconevents.EventStream, 256),
		kzgCalled: make(chan struct{}, 1),
	}
	h.storage = blob_storage.NewDataColumnStore(h.fs, &cfg, beaconevents.NewEventEmitter())
	if wrapStorage != nil {
		h.storage = wrapStorage(h.storage)
	}
	peerDas := das.NewPeerDas(nil, &cfg, &caplinConfig, h.storage, nil, nil, localNode.ID(), nil, state, nil, nil, nil)

	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(block.Block.Slot).AnyTimes()
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()
	clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(elTestDigest, nil).AnyTimes()
	syncedData := syncedmock.NewMockSyncedData(ctrl)
	syncedData.EXPECT().Syncing().Return(false).AnyTimes()
	gossipManager := gossipmock.NewMockGossip(ctrl)
	gossipManager.EXPECT().PublishToForkDigest(gomock.Any(), elTestDigest, gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ common.Bytes4, _ string, payload []byte) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.published = append(h.published, payload)
			return nil
		}).AnyTimes()
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.GloasVersion).Return(blobs, proofs, nil).AnyTimes()

	blockServiceIface, _, _, forkChoice := setupBlockService(t, ctrl)
	forkChoice.Blocks[root] = block
	forkChoice.Headers[root] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot}
	adapter := &peerDasForkChoice{ForkChoiceStorageMock: forkChoice, peerDas: peerDas}
	emitters := beaconevents.NewEventEmitter()
	subscription := emitters.Operation().Subscribe(h.events)
	t.Cleanup(subscription.Unsubscribe)
	h.service = NewDataColumnSidecarService(canceledPendingQueueContext(t), &cfg, clock, adapter, syncedData, h.storage, emitters, gossipManager).(*dataColumnSidecarService)
	h.blockService = blockServiceIface.(*blockService)
	h.blockService.forkchoiceStore = adapter
	h.blockService.engine = engine
	h.blockService.dataColumns = h.service

	originalSubnet := computeSubnetForDataColumnSidecar
	t.Cleanup(func() { computeSubnetForDataColumnSidecar = originalSubnet })
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % cfg.DataColumnSidecarSubnetCount }
	originalKZG := verifyDataColumnSidecarKZGProofsWithCommitments
	t.Cleanup(func() { verifyDataColumnSidecarKZGProofsWithCommitments = originalKZG })
	verifyDataColumnSidecarKZGProofsWithCommitments = func(*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) bool {
		h.kzgCalls.Add(1)
		select {
		case h.kzgCalled <- struct{}{}:
		default:
		}
		return true
	}
	t.Cleanup(h.finish)
	return h
}

// wireCopy returns a gossip copy of a column, decoded from the encoding of a sidecar the import never touches.
func (h *elImportHarness) wireCopy(index uint64) *cltypes.DataColumnSidecar {
	h.t.Helper()
	encoded, err := h.wire[index].EncodeSSZ(nil)
	require.NoError(h.t, err)
	copyOfColumn := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	require.NoError(h.t, copyOfColumn.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	return copyOfColumn
}

// startImport runs the import in the background and returns once its batch write is held.
func (h *elImportHarness) startImport() {
	h.t.Helper()
	h.importDone = make(chan struct{})
	h.wg.Go(func() {
		defer close(h.importDone)
		h.blockService.importColumnsFromELBlobs(h.t.Context(), h.block)
	})
	select {
	case <-h.fs.entered:
	case <-h.importDone:
		h.t.Fatal("the import returned before its batch write")
	case <-time.After(elImportTestTimeout):
		h.t.Fatal("timed out waiting for the import to reach its batch write")
	}
}

func (h *elImportHarness) process(msg *cltypes.DataColumnSidecar) *gossipResult {
	result := &gossipResult{done: make(chan struct{})}
	h.wg.Go(func() {
		defer close(result.done)
		result.err = h.service.ProcessMessage(h.t.Context(), nil, msg)
	})
	return result
}

// finish releases the held batch write and joins the background goroutines.
func (h *elImportHarness) finish() {
	h.t.Helper()
	h.releaseOnce.Do(func() { close(h.fs.release) })
	joinWithin(h.t, &h.wg)
}

func (h *elImportHarness) seen(index uint64) bool {
	return h.service.seenGloas.contains(seenGloasSidecarKey{beaconBlockRoot: h.root, index: index})
}

func (h *elImportHarness) stored(index uint64) bool {
	h.t.Helper()
	exists, err := h.storage.ColumnSidecarExists(h.t.Context(), h.block.Block.Slot, h.root, int64(index))
	require.NoError(h.t, err)
	return exists
}

func (h *elImportHarness) publishedIndices() []uint64 {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	indices := make([]uint64, 0, len(h.published))
	for _, payload := range h.published {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		require.NoError(h.t, sidecar.DecodeSSZ(payload, int(clparams.GloasVersion)))
		indices = append(indices, sidecar.Index)
	}
	slices.Sort(indices)
	return indices
}

func TestGloasGossipCopyOfAColumnTheELImportIsWritingIsIgnored(t *testing.T) {
	h := newELImportHarness(t, clparams.CaplinConfig{}, nil)
	column := h.custody[0]
	copyOfColumn := h.wireCopy(column)
	h.startImport()

	result := h.process(copyOfColumn)
	select {
	case <-h.kzgCalled:
	case <-result.done:
	case <-time.After(elImportTestTimeout):
		t.Error("timed out waiting for the gossip copy")
	}
	h.finish()

	require.Zero(t, h.kzgCalls.Load(), "KZG was called for a copy of a column the EL import is writing")
	require.ErrorIs(t, result.err, ErrIgnore)
	require.True(t, h.stored(column))
	require.True(t, h.seen(column))
	require.Equal(t, h.custody, h.publishedIndices())
}

func TestGloasELImportPublishesAColumnWhoseGossipOwnerFailed(t *testing.T) {
	var gate *gatedSingleWrite
	h := newELImportHarness(t, clparams.CaplinConfig{}, func(storage blob_storage.DataColumnStorage) blob_storage.DataColumnStorage {
		gate = newGatedSingleWrite(storage)
		return gate
	})
	t.Cleanup(func() { gate.open() })
	column := h.custody[0]
	failing := h.process(h.wireCopy(column))
	waitClosed(t, gate.entered, "the gossip copy to reach its write")
	h.startImport()

	gate.open()
	waitClosed(t, failing.done, "the gossip copy to fail")
	h.finish()

	require.ErrorIs(t, failing.err, ErrIgnore)
	require.ErrorIs(t, failing.err, errGatedWrite)
	require.Equal(t, h.custody, h.publishedIndices())
	require.True(t, h.seen(column))
}

func TestGloasGossipCopyOfAPublicationOnlyColumnIsAcceptedDuringTheImport(t *testing.T) {
	h := newELImportHarness(t, clparams.CaplinConfig{SubscribeAllTopics: true}, nil)
	var publicationOnly uint64
	for slices.Contains(h.custody, publicationOnly) {
		publicationOnly++
	}
	h.startImport()

	result := h.process(h.wireCopy(publicationOnly))
	waitClosed(t, result.done, "the gossip copy")

	require.NoError(t, result.err)
	require.Equal(t, int32(1), h.kzgCalls.Load())
	require.Len(t, h.events, 1)
	require.True(t, h.seen(publicationOnly))
	require.False(t, h.stored(publicationOnly))

	h.finish()
	var expected []uint64
	for index := range h.service.cfg.NumberOfColumns {
		if index != publicationOnly {
			expected = append(expected, index)
		}
	}
	require.Equal(t, expected, h.publishedIndices())
}

func TestGloasELImportPublishesClaimedColumnsAfterTheImportContextExpires(t *testing.T) {
	b, engine, peerDas, columns := setupELBlobsBlockService(t)
	cfg := *b.beaconCfg
	cfg.SecondsPerSlot = 0
	b.beaconCfg = &cfg
	block, root, hashes := elBlobsTestBlock(t, clparams.GloasVersion, 2)
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{1}}, {{2}}}
	built := []*cltypes.DataColumnSidecar{cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)}

	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil)
	engine.EXPECT().GetBlobs(gomock.Any(), hashes, clparams.GloasVersion).Return(blobs, proofs, nil)
	peerDas.EXPECT().ImportColumnsFromBlobs(gomock.Any(), block, blobs, proofs, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *cltypes.SignedBeaconBlock, _ [][]byte, _ [][][]byte, claim func([]*cltypes.DataColumnSidecar)) ([]*cltypes.DataColumnSidecar, error) {
			claim(built)
			return built, nil
		})

	b.importColumnsFromELBlobs(t.Context(), block)

	require.Equal(t, [][]*cltypes.DataColumnSidecar{built}, columns.claimedPublished)
	require.Equal(t, []error{nil}, columns.claimedContextErr)
}
