// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package das

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

func makeRecoverySidecars(t *testing.T, slot uint64, count int) (common.Hash, []*cltypes.BlobSidecar) {
	root, _, sidecars := makeRecoveryBlockAndSidecars(t, slot, count)
	return root, sidecars
}

func makeRecoveryBlockAndSidecars(t *testing.T, slot uint64, count int) (common.Hash, *cltypes.SignedBeaconBlock, []*cltypes.BlobSidecar) {
	t.Helper()
	blob := cltypes.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(&blob), 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&blob), commitment, 0)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	blockCommitment := cltypes.KZGCommitment(commitment)
	for range count {
		block.Block.Body.BlobKzgCommitments.Append(&blockCommitment)
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	sidecars := make([]*cltypes.BlobSidecar, count)
	for index := range count {
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(index)
		require.NoError(t, err)
		inclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for branchIndex, hash := range branch {
			inclusionProof.Set(branchIndex, hash)
		}
		sidecars[index] = cltypes.NewBlobSidecar(uint64(index), &blob, common.Bytes48(commitment), common.Bytes48(proof), block.SignedBeaconBlockHeader(), inclusionProof)
	}
	return root, block, sidecars
}

func receiveRecoveryRequest(t *testing.T, queue <-chan recoverBlobsRequest) recoverBlobsRequest {
	t.Helper()
	select {
	case request := <-queue:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blob recovery request")
		return recoverBlobsRequest{}
	}
}

func receiveRecoveryResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blob recovery result")
		return context.DeadlineExceeded
	}
}

func waitRecoverySignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func newRecoveryTestPeerDas(t *testing.T) (*peerdas, *mock_services.MockDataColumnStorage, *mock_services.MockBlobStorage) {
	t.Helper()
	ctrl := gomock.NewController(t)
	columns := mock_services.NewMockDataColumnStorage(ctrl)
	blobs := mock_services.NewMockBlobStorage(ctrl)
	cfg := clparams.MainnetBeaconConfig
	cfg.NumberOfColumns = 4
	return &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columns,
		blobStorage:       blobs,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]*blobRecovery),
	}, columns, blobs
}

func gloasRecoveryConfig() clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	return cfg
}

func denebRecoveryConfig() clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = math.MaxUint64
	cfg.FuluForkEpoch = math.MaxUint64
	cfg.GloasForkEpoch = math.MaxUint64
	cfg.NumberOfColumns = 4
	cfg.InitializeForkSchedule()
	return cfg
}

type blockingGloasBlockReader struct {
	freezeblocks.BeaconSnapshotReader
}

type staticRecoveryBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	block *cltypes.SignedBeaconBlock
}

type countingRecoveryBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	block *cltypes.SignedBeaconBlock
	calls atomic.Int64
}

func (r *countingRecoveryBlockReader) ReadBlockByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
	r.calls.Add(1)
	return r.block, nil
}

func (r staticRecoveryBlockReader) ReadBlockByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
	return r.block, nil
}

type recoveryBlockGetter map[common.Hash]*cltypes.SignedBeaconBlock

func (g recoveryBlockGetter) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	block, ok := g[root]
	return block, ok
}

type removalTrackingBlobStorage struct {
	blob_storage.BlobStorage
	removed bool
}

func (s *removalTrackingBlobStorage) RemoveBlobSidecars(ctx context.Context, slot uint64, root common.Hash) error {
	s.removed = true
	return s.BlobStorage.RemoveBlobSidecars(ctx, slot, root)
}

func (blockingGloasBlockReader) ReadBlockByRoot(ctx context.Context, _ kv.Tx, _ common.Hash) (*cltypes.SignedBeaconBlock, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestHistoricalGloasLookupObservesRecoveryContext(t *testing.T) {
	cache, err := lru.New[common.Hash, *gloasBlockData]("gloasRecoveryContextTest", 1)
	require.NoError(t, err)
	d := &peerdas{
		indiciesDB:     memdb.NewTestDB(t, dbcfg.ChainDB),
		blockReader:    blockingGloasBlockReader{},
		gloasDataCache: cache,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = d.getGloasData(ctx, 0, common.HexToHash("0x30"))
	require.ErrorIs(t, err, context.Canceled)
}

func TestGloasCanonicalLookupRejectsIncompleteBlocks(t *testing.T) {
	cfg := gloasRecoveryConfig()
	root := common.HexToHash("0x31")
	missingBid := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	missingBid.Block.Body.SignedExecutionPayloadBid = nil
	missingBidMessage := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	missingBidMessage.Block.Body.SignedExecutionPayloadBid.Message = nil
	for _, tc := range []struct {
		name  string
		block *cltypes.SignedBeaconBlock
	}{
		{name: "nil block", block: &cltypes.SignedBeaconBlock{}},
		{name: "nil body", block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{}}},
		{name: "missing bid", block: missingBid},
		{name: "missing bid message", block: missingBidMessage},
	} {
		t.Run("forkchoice "+tc.name, func(t *testing.T) {
			cache, err := lru.New[common.Hash, *gloasBlockData]("gloasIncompleteForkChoice", 1)
			require.NoError(t, err)
			d := &peerdas{beaconConfig: &cfg, forkChoice: recoveryBlockGetter{root: tc.block}, gloasDataCache: cache}
			require.NotPanics(t, func() {
				_, err = d.getGloasData(t.Context(), 0, root)
			})
			require.Error(t, err)
		})
		t.Run("historical "+tc.name, func(t *testing.T) {
			cache, err := lru.New[common.Hash, *gloasBlockData]("gloasIncompleteHistorical", 1)
			require.NoError(t, err)
			d := &peerdas{
				beaconConfig:   &cfg,
				blockReader:    staticRecoveryBlockReader{block: tc.block},
				indiciesDB:     memdb.NewTestDB(t, dbcfg.ChainDB),
				gloasDataCache: cache,
			}
			require.NotPanics(t, func() {
				_, err = d.getGloasData(t.Context(), 0, root)
			})
			require.Error(t, err)
		})
	}
}

func TestPreGloasCanonicalCommitmentsRejectIncompleteBlocks(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.GloasForkEpoch = math.MaxUint64
	root := common.HexToHash("0x32")
	for _, tc := range []struct {
		name  string
		block *cltypes.SignedBeaconBlock
	}{
		{name: "nil block", block: &cltypes.SignedBeaconBlock{}},
		{name: "nil body", block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{}}},
	} {
		t.Run("forkchoice "+tc.name, func(t *testing.T) {
			d := &peerdas{beaconConfig: &cfg, forkChoice: recoveryBlockGetter{root: tc.block}}
			var err error
			require.NotPanics(t, func() {
				_, err = d.canonicalBlobCommitments(t.Context(), 0, root)
			})
			require.Error(t, err)
		})
		t.Run("historical "+tc.name, func(t *testing.T) {
			d := &peerdas{beaconConfig: &cfg, blockReader: staticRecoveryBlockReader{block: tc.block}, indiciesDB: memdb.NewTestDB(t, dbcfg.ChainDB)}
			var err error
			require.NotPanics(t, func() {
				_, err = d.canonicalBlobCommitments(t.Context(), 0, root)
			})
			require.Error(t, err)
		})
	}
}

func TestGloasCanonicalSourcesRejectIdentityMismatch(t *testing.T) {
	cfg := gloasRecoveryConfig()
	const slot = uint64(10)
	valid := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	valid.Block.Slot = slot
	validRoot, err := valid.Block.HashSSZ()
	require.NoError(t, err)
	wrongVersion := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	wrongVersion.Block.Slot = slot
	wrongVersionRoot, err := wrongVersion.Block.HashSSZ()
	require.NoError(t, err)
	for _, tc := range []struct {
		name          string
		block         *cltypes.SignedBeaconBlock
		requestedSlot uint64
		requestedRoot common.Hash
	}{
		{name: "root", block: valid, requestedSlot: slot, requestedRoot: common.HexToHash("0x41")},
		{name: "slot", block: valid, requestedSlot: slot + 1, requestedRoot: validRoot},
		{name: "version", block: wrongVersion, requestedSlot: slot, requestedRoot: wrongVersionRoot},
	} {
		for _, source := range []string{"forkchoice", "historical"} {
			t.Run(source+" "+tc.name, func(t *testing.T) {
				cache, err := lru.New[common.Hash, *gloasBlockData]("gloasIdentity", 1)
				require.NoError(t, err)
				d := &peerdas{beaconConfig: &cfg, gloasDataCache: cache}
				if source == "forkchoice" {
					d.forkChoice = recoveryBlockGetter{tc.requestedRoot: tc.block}
				} else {
					d.blockReader = staticRecoveryBlockReader{block: tc.block}
					d.indiciesDB = memdb.NewTestDB(t, dbcfg.ChainDB)
				}

				_, err = d.getGloasData(t.Context(), tc.requestedSlot, tc.requestedRoot)
				require.Error(t, err)
				_, err = d.canonicalSignedBlockHeader(t.Context(), tc.requestedSlot, tc.requestedRoot)
				require.Error(t, err)
				_, err = d.canonicalBlobCommitments(t.Context(), tc.requestedSlot, tc.requestedRoot)
				require.Error(t, err)
				require.Zero(t, cache.Len())
			})
		}
	}
}

func TestGloasMalformedForkChoiceDoesNotFallBackToHistory(t *testing.T) {
	cfg := gloasRecoveryConfig()
	const slot = uint64(10)
	valid := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	valid.Block.Slot = slot
	root, err := valid.Block.HashSSZ()
	require.NoError(t, err)
	malformed := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	malformed.Block.Slot = slot + 1
	reader := &countingRecoveryBlockReader{block: valid}
	cache, err := lru.New[common.Hash, *gloasBlockData]("gloasNoMalformedFallback", 1)
	require.NoError(t, err)
	d := &peerdas{
		beaconConfig:   &cfg,
		forkChoice:     recoveryBlockGetter{root: malformed},
		blockReader:    reader,
		indiciesDB:     memdb.NewTestDB(t, dbcfg.ChainDB),
		gloasDataCache: cache,
	}

	_, err = d.canonicalBlobCommitments(t.Context(), slot, root)

	require.Error(t, err)
	require.Zero(t, reader.calls.Load())
	require.Zero(t, cache.Len())
}

func TestGloasMissingForkChoiceFallsBackToValidHistory(t *testing.T) {
	cfg := gloasRecoveryConfig()
	const slot = uint64(10)
	valid := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	valid.Block.Slot = slot
	root, err := valid.Block.HashSSZ()
	require.NoError(t, err)
	reader := &countingRecoveryBlockReader{block: valid}
	cache, err := lru.New[common.Hash, *gloasBlockData]("gloasValidFallback", 1)
	require.NoError(t, err)
	d := &peerdas{
		beaconConfig:   &cfg,
		forkChoice:     recoveryBlockGetter{},
		blockReader:    reader,
		indiciesDB:     memdb.NewTestDB(t, dbcfg.ChainDB),
		gloasDataCache: cache,
	}

	_, err = d.canonicalBlobCommitments(t.Context(), slot, root)

	require.NoError(t, err)
	require.Equal(t, int64(1), reader.calls.Load())
	require.Equal(t, 1, cache.Len())
}

func TestBlobRecoveryCompleteRejectsWrongCanonicalSourceIdentity(t *testing.T) {
	const slot = uint64(70)
	root, sidecars := makeRecoverySidecars(t, slot, 1)
	wrong := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	wrong.Block.Slot = slot
	wrong.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	commitment := cltypes.KZGCommitment(sidecars[0].KzgCommitment)
	wrong.Block.Body.BlobKzgCommitments.Append(&commitment)
	wrong.Block.ParentRoot[0] ^= 0xff
	d, _, blobs := newRecoveryTestPeerDas(t)
	d.beaconConfig.DenebForkEpoch = 0
	d.beaconConfig.FuluForkEpoch = math.MaxUint64
	d.beaconConfig.GloasForkEpoch = math.MaxUint64
	d.beaconConfig.InitializeForkSchedule()
	d.forkChoice = recoveryBlockGetter{root: wrong}
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), slot, root).Return(sidecars, true, nil)

	complete, err := d.blobRecoveryCompleteAny(t.Context(), slot, root)

	require.Error(t, err)
	require.False(t, complete)
}

func TestCanceledForcedRecoveryIsDroppedBeforeWork(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x01")
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(10), root).Return(nil, false, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(10), root).Return([]uint64{0, 1}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- d.ForceScheduleRecover(ctx, 10, root, 2) }()
	request := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	cancel()
	require.ErrorIs(t, receiveRecoveryResult(t, result), context.Canceled)

	called := false
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		called = true
		return nil
	})
	require.False(t, called)
}

func TestAdmittedRecoverySurvivesCallerCancellation(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x05")
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(50), root).Return(nil, false, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(50), root).Return([]*cltypes.BlobSidecar{{}}, true, nil)
	callerCtx, cancelCaller := context.WithCancel(t.Context())
	result := make(chan error, 1)
	request := recoverBlobsRequest{slot: 50, blockRoot: root, expectedBlobs: 1, force: true, ctx: callerCtx, result: result}
	d.handleRecoverBlobsRequest(t.Context(), request, func(ownerCtx context.Context, _ recoverBlobsRequest) error {
		cancelCaller()
		require.NoError(t, ownerCtx.Err())
		return nil
	})
	require.ErrorIs(t, callerCtx.Err(), context.Canceled)
	d.recoveringMutex.Lock()
	_, active := d.isRecovering[root]
	d.recoveringMutex.Unlock()
	require.False(t, active)
}

func TestLiveRecoveryCoalescesRetryAfterOwnerFailure(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, maxBlobRecoveryWaiters+2)
	root := common.HexToHash("0x06")
	otherRoot := common.HexToHash("0x16")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), gomock.Any(), gomock.Any()).Return([]uint64{0, 1}, nil).AnyTimes()
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.handleRecoverBlobsRequest(t.Context(), recoverBlobsRequest{slot: 60, blockRoot: root}, func(context.Context, recoverBlobsRequest) error {
			close(started)
			<-release
			return errors.New("owner failed")
		})
	}()
	waitRecoverySignal(t, started, "recovery start")
	for range maxBlobRecoveryWaiters {
		require.NoError(t, d.TryScheduleRecover(60, root))
	}
	d.recoveringMutex.Lock()
	waiterCount := len(d.isRecovering[root].waiters)
	d.recoveringMutex.Unlock()
	close(release)
	waitRecoverySignal(t, done, "recovery completion")
	require.Zero(t, waiterCount)

	require.NoError(t, d.TryScheduleRecover(61, otherRoot))
	requests := []recoverBlobsRequest{receiveRecoveryRequest(t, d.recoverBlobsQueue), receiveRecoveryRequest(t, d.recoverBlobsQueue)}
	counts := map[common.Hash]int{}
	for _, request := range requests {
		counts[request.blockRoot]++
	}
	require.Equal(t, 1, counts[root])
	require.Equal(t, 1, counts[otherRoot])
	require.Empty(t, d.recoverBlobsQueue)
}

func TestDequeuedDuplicateRecoveryRetriesOnceAfterOwnerFailure(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 3)
	root := common.HexToHash("0x17")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(71), root).Return([]uint64{0, 1}, nil).AnyTimes()
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil).AnyTimes()

	require.NoError(t, d.TryScheduleRecover(71, root))
	require.NoError(t, d.TryScheduleRecover(71, root))
	first := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	duplicate := receiveRecoveryRequest(t, d.recoverBlobsQueue)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	callbacks := 0
	go func() {
		defer close(done)
		d.handleRecoverBlobsRequest(t.Context(), first, func(context.Context, recoverBlobsRequest) error {
			callbacks++
			close(started)
			<-release
			return errors.New("owner failed")
		})
	}()
	waitRecoverySignal(t, started, "recovery start")
	d.handleRecoverBlobsRequest(t.Context(), duplicate, func(context.Context, recoverBlobsRequest) error {
		t.Fatal("dequeued duplicate started concurrent recovery")
		return nil
	})
	close(release)
	waitRecoverySignal(t, done, "recovery completion")

	retry := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	d.handleRecoverBlobsRequest(t.Context(), retry, func(context.Context, recoverBlobsRequest) error {
		callbacks++
		return errors.New("retry remained incomplete")
	})
	require.Equal(t, 2, callbacks)
	require.Empty(t, d.recoverBlobsQueue)
}

func TestRecoveryOwnerStopsWhenWorkerIsCanceled(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x26")
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil).AnyTimes()
	workerCtx, cancelWorker := context.WithCancel(t.Context())
	entered := make(chan struct{})
	release := make(chan struct{})
	callbackResult := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.handleRecoverBlobsRequest(workerCtx, recoverBlobsRequest{slot: 62, blockRoot: root}, func(ownerCtx context.Context, _ recoverBlobsRequest) error {
			close(entered)
			select {
			case <-ownerCtx.Done():
				callbackResult <- ownerCtx.Err()
				return ownerCtx.Err()
			case <-release:
				return errors.New("test released callback")
			}
		})
	}()
	waitRecoverySignal(t, entered, "recovery callback entry")
	cancelWorker()

	select {
	case err := <-callbackResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		close(release)
		waitRecoverySignal(t, done, "recovery completion")
		t.Fatal("worker cancellation did not reach recovery callback")
	}
	waitRecoverySignal(t, done, "recovery completion")
}

func TestLiveRecoveryRequeuesWhenCoalescedOwnerRemainsIncomplete(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	root := common.HexToHash("0x07")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(70), root).Return([]uint64{0, 1}, nil).AnyTimes()
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil).AnyTimes()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.handleRecoverBlobsRequest(t.Context(), recoverBlobsRequest{slot: 70, blockRoot: root}, func(context.Context, recoverBlobsRequest) error {
			close(started)
			<-release
			return nil
		})
	}()
	waitRecoverySignal(t, started, "recovery start")
	require.NoError(t, d.TryScheduleRecover(70, root))
	close(release)
	waitRecoverySignal(t, done, "recovery completion")

	request := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	require.Equal(t, uint64(70), request.slot)
	require.Equal(t, root, request.blockRoot)
}

func TestNonForcedRecoveryBroadcastsCallbackFailure(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x08")
	wantErr := errors.New("column recovery failed")
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil).Times(2)
	result := make(chan error, 1)

	d.handleRecoverBlobsRequest(t.Context(), recoverBlobsRequest{slot: 80, blockRoot: root, result: result}, func(context.Context, recoverBlobsRequest) error {
		return wantErr
	})

	require.ErrorIs(t, receiveRecoveryResult(t, result), wantErr)
}

func TestForcedRecoveryCoalescesOntoActiveResult(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x02")
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(20), root).Return(nil, false, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(20), root).Return([]uint64{0, 1}, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(20), root).Return([]*cltypes.BlobSidecar{{}}, true, nil)
	d.isRecovering[root] = &blobRecovery{}

	result := make(chan error, 1)
	forceCtx, cancelForce := context.WithTimeout(t.Context(), time.Second)
	defer cancelForce()
	go func() { result <- d.ForceScheduleRecover(forceCtx, 20, root, 2) }()
	request := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		t.Fatal("coalesced request started duplicate recovery")
		return nil
	})

	select {
	case err := <-result:
		t.Fatalf("forced recovery returned before active recovery completed: %v", err)
	default:
	}
	d.finishBlobRecovery(root, nil)
	require.ErrorContains(t, receiveRecoveryResult(t, result), "blob recovery did not complete")
}

func TestForcedRecoveryRechecksCompletedDataAfterQueueDelay(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	cfg := denebRecoveryConfig()
	d.beaconConfig = &cfg
	root, block, completeSidecars := makeRecoveryBlockAndSidecars(t, 30, 2)
	canonicalRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root, common.Hash(canonicalRoot))
	d.forkChoice = recoveryBlockGetter{root: block}
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(30), root).Return(nil, false, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(30), root).Return([]uint64{0, 1}, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(30), root).Return(completeSidecars, true, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(30), root).Return(completeSidecars, true, nil)

	result := make(chan error, 1)
	forceCtx, cancelForce := context.WithTimeout(t.Context(), time.Second)
	defer cancelForce()
	go func() { result <- d.ForceScheduleRecover(forceCtx, 30, root, 2) }()
	request := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		t.Fatal("completed queued request started stale forced recovery")
		return nil
	})
	require.NoError(t, receiveRecoveryResult(t, result))
}

func TestForcedRecoveryRejectsUnderreportedBlobMetadata(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	cfg := denebRecoveryConfig()
	d.beaconConfig = &cfg
	root, block, twoBlobs := makeRecoveryBlockAndSidecars(t, 40, 2)
	canonicalRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root, common.Hash(canonicalRoot))
	d.forkChoice = recoveryBlockGetter{root: block}
	oneBlob := twoBlobs[:1]
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(40), root).Return(oneBlob, true, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(40), root).Return([]uint64{0, 1}, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(40), root).Return(oneBlob, true, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(40), root).Return(twoBlobs, true, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(40), root).Return(twoBlobs, true, nil)

	result := make(chan error, 1)
	forceCtx, cancelForce := context.WithTimeout(t.Context(), time.Second)
	defer cancelForce()
	go func() { result <- d.ForceScheduleRecover(forceCtx, 40, root, 2) }()
	request := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	called := false
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		called = true
		return nil
	})
	require.True(t, called)
	require.NoError(t, receiveRecoveryResult(t, result))
}

func TestRecoveryPanicCleansOwnershipAndWakesWaiters(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x09")
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil)
	result := make(chan error, 1)

	d.handleRecoverBlobsRequest(t.Context(), recoverBlobsRequest{slot: 90, blockRoot: root, result: result}, func(context.Context, recoverBlobsRequest) error {
		panic("recovery crashed")
	})

	select {
	case err := <-result:
		require.ErrorContains(t, err, "recovery panicked")
	default:
		t.Fatal("recovery panic did not wake waiter")
	}
	d.recoveringMutex.Lock()
	_, active := d.isRecovering[root]
	d.recoveringMutex.Unlock()
	require.False(t, active)
}

func TestLiveRecoverySchedulesWhenBlobFilesAreIncomplete(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	root := common.HexToHash("0x0a")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(91), root).Return([]uint64{0, 1}, nil)
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(2), nil).Times(3)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(91), root).Return([]*cltypes.BlobSidecar{{}}, false, nil).Times(2)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(91), root).Return([]*cltypes.BlobSidecar{{}, {}}, true, nil)

	require.NoError(t, d.TryScheduleRecover(91, root))
	request := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	called := false
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		called = true
		return nil
	})
	require.True(t, called)
	d.recoveringMutex.Lock()
	_, active := d.isRecovering[root]
	d.recoveringMutex.Unlock()
	require.True(t, active)
	retry := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	require.Equal(t, root, retry.blockRoot)
}

func TestArchiveDataAvailabilityRejectsIncompleteBlobFiles(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	root := common.HexToHash("0x1a")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(94), root).Return([]uint64{0}, nil)
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(2), nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(94), root).Return([]*cltypes.BlobSidecar{{}}, false, nil)

	available, err := d.IsDataAvailable(94, root)
	require.NoError(t, err)
	require.False(t, available)
}

func TestArchiveDataAvailabilityUsesColumnQuorumDespiteCorruptBlobs(t *testing.T) {
	d, columns, _ := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	root := common.HexToHash("0x19")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(93), root).Return([]uint64{0, 1}, nil)

	available, err := d.IsDataAvailable(93, root)
	require.NoError(t, err)
	require.True(t, available)
}

func TestLiveRecoveryClearsCorruptBlobsBeforeScheduling(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	root := common.HexToHash("0x18")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(92), root).Return([]uint64{0, 1}, nil)
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(92), root).Return(nil, false, blob_storage.ErrBlobSidecarCorrupt)
	blobs.EXPECT().RemoveBlobSidecars(gomock.Any(), uint64(92), root).Return(nil)

	require.NoError(t, d.TryScheduleRecover(92, root))
	request := receiveRecoveryRequest(t, d.recoverBlobsQueue)
	require.Equal(t, root, request.blockRoot)
}

func TestForcedFuluRecoveryRepairsStoredSignatureMismatch(t *testing.T) {
	const slot = uint64(100)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = math.MaxUint64
	cfg.NumberOfColumns = 4
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = slot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	blob := cltypes.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(&blob), 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&blob), commitment, 0)
	require.NoError(t, err)
	blockCommitment := cltypes.KZGCommitment(commitment)
	block.Block.Body.BlobKzgCommitments.Append(&blockCommitment)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	branch, err := block.Block.Body.KzgCommitmentMerkleProof(0)
	require.NoError(t, err)
	inclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
	for index, hash := range branch {
		inclusionProof.Set(index, hash)
	}
	valid := cltypes.NewBlobSidecar(0, &blob, common.Bytes48(commitment), common.Bytes48(proof), block.SignedBeaconBlockHeader(), inclusionProof)
	invalid := *valid
	invalidHeader := *invalid.SignedBlockHeader
	invalidHeader.Signature[0] ^= 0xff
	invalid.SignedBlockHeader = &invalidHeader
	storage := &removalTrackingBlobStorage{BlobStorage: blob_storage.NewBlobStore(memdb.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs(), math.MaxUint64, &cfg, nil)}
	require.NoError(t, storage.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{&invalid}))

	d, columns, _ := newRecoveryTestPeerDas(t)
	d.beaconConfig = &cfg
	d.blobStorage = storage
	d.forkChoice = recoveryBlockGetter{root: block}
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), slot, root).Return([]uint64{0, 1}, nil)
	workerCtx, cancelWorker := context.WithCancel(t.Context())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		select {
		case request := <-d.recoverBlobsQueue:
			d.handleRecoverBlobsRequest(workerCtx, request, func(ctx context.Context, _ recoverBlobsRequest) error {
				return storage.WriteBlobSidecars(ctx, root, []*cltypes.BlobSidecar{valid})
			})
		case <-workerCtx.Done():
		}
	}()

	forceCtx, cancelForce := context.WithTimeout(t.Context(), time.Second)
	defer cancelForce()
	require.NoError(t, d.ForceScheduleRecover(forceCtx, slot, root, 1))
	stored, complete, err := storage.ReadBlobSidecars(t.Context(), slot, root)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, stored, 1)
	require.Equal(t, block.Signature, stored[0].SignedBlockHeader.Signature)
	require.True(t, storage.removed)
	cancelWorker()
	waitRecoverySignal(t, workerDone, "recovery worker completion")
}

func TestArchiveDataAvailabilityRejectsWrongRootBlobFiles(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	root := common.HexToHash("0x1b")
	wrongBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	wrongBlock.Block.Slot = 97
	wrongBlock.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	wrongSidecar := &cltypes.BlobSidecar{Index: 0, SignedBlockHeader: wrongBlock.SignedBeaconBlockHeader()}
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(97), root).Return([]uint64{0}, nil)
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(97), root).Return([]*cltypes.BlobSidecar{wrongSidecar}, true, nil)

	available, err := d.IsDataAvailable(97, root)
	require.NoError(t, err)
	require.False(t, available)
}

func TestArchiveDataAvailabilityRejectsInvalidBlobProof(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 98
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	sidecar := &cltypes.BlobSidecar{Index: 0, SignedBlockHeader: block.SignedBeaconBlockHeader()}
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(98), root).Return([]uint64{0}, nil)
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(98), root).Return([]*cltypes.BlobSidecar{sidecar}, true, nil)

	available, err := d.IsDataAvailable(98, root)
	require.NoError(t, err)
	require.False(t, available)
}

func TestArchiveDataAvailabilityRejectsValidBlobSubset(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	cfg := denebRecoveryConfig()
	d.beaconConfig = &cfg
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	_, sidecars := makeRecoverySidecars(t, 99, 2)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block = &cltypes.BeaconBlock{Slot: 99, Body: cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig, clparams.DenebVersion)}
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	for _, sidecar := range sidecars {
		commitment := cltypes.KZGCommitment(sidecar.KzgCommitment)
		block.Block.Body.BlobKzgCommitments.Append(&commitment)
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	for _, sidecar := range sidecars {
		sidecar.SignedBlockHeader = block.SignedBeaconBlockHeader()
	}
	d.forkChoice = recoveryBlockGetter{root: block}
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(99), root).Return([]uint64{0}, nil)
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(99), root).Return(sidecars[:1], true, nil)

	available, err := d.IsDataAvailable(99, root)
	require.NoError(t, err)
	require.False(t, available)
}

func TestRecoveryWorkerSurvivesMalformedRecovery(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	firstRoot := common.HexToHash("0x2a")
	secondRoot := common.HexToHash("0x2b")
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), firstRoot).Return(uint32(0), nil)
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), secondRoot).Return(uint32(0), nil).Times(2)

	workerCtx, cancelWorker := context.WithCancel(t.Context())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for {
			select {
			case <-workerCtx.Done():
				return
			case request := <-d.recoverBlobsQueue:
				d.handleRecoverBlobsRequest(workerCtx, request, func(_ context.Context, request recoverBlobsRequest) error {
					if request.blockRoot == firstRoot {
						panic("malformed sidecar")
					}
					return errors.New("second recovery attempted")
				})
			}
		}
	}()
	firstResult := make(chan error, 1)
	secondResult := make(chan error, 1)
	d.recoverBlobsQueue <- recoverBlobsRequest{slot: 95, blockRoot: firstRoot, result: firstResult}
	require.ErrorContains(t, receiveRecoveryResult(t, firstResult), "recovery panicked")
	d.recoverBlobsQueue <- recoverBlobsRequest{slot: 96, blockRoot: secondRoot, result: secondResult}
	require.ErrorContains(t, receiveRecoveryResult(t, secondResult), "second recovery attempted")
	cancelWorker()
	waitRecoverySignal(t, workerDone, "recovery worker completion")
}

func TestForcedRecoveryColumnAdmissionObservesCallerCancellation(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x0b")
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(92), root).Return(nil, false, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(92), root).DoAndReturn(func(ctx context.Context, _ uint64, _ common.Hash) ([]uint64, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := d.ForceScheduleRecover(ctx, 92, root, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBlobRecoveryCountObservesOwnerCancellation(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x0c")
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).DoAndReturn(func(ctx context.Context, _ common.Hash) (uint32, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := d.blobRecoveryCompleteAny(ctx, 92, root)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCanceledRecoveryWaitersDoNotConsumeAdmissionLimit(t *testing.T) {
	d, _, _ := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x0d")
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	active := &blobRecovery{waiters: make([]recoveryWaiter, maxBlobRecoveryWaiters)}
	for index := range active.waiters {
		active.waiters[index] = recoveryWaiter{ctx: canceledCtx, result: make(chan error, 1)}
	}
	d.isRecovering[root] = active
	result := make(chan error, 1)

	d.handleRecoverBlobsRequest(t.Context(), recoverBlobsRequest{slot: 93, blockRoot: root, force: true, result: result}, func(context.Context, recoverBlobsRequest) error {
		t.Fatal("coalesced waiter started duplicate recovery")
		return nil
	})

	require.Len(t, active.waiters, 1)
	require.Equal(t, result, active.waiters[0].result)
}
