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
	"container/heap"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/p2p/enode"
)

// initTestBeaconConfig installs cfg as the global config if no test has done so
// yet. InitGlobalStaticConfig panics on a second call, so tests in this package
// must agree on every global-only field; they may differ only in fork epochs,
// which each test reads from its own local config.
func initTestBeaconConfig(cfg *clparams.BeaconChainConfig) {
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(cfg, &clparams.CaplinConfig{})
	}
}

type writeNotifyingBlobStorage struct {
	blob_storage.BlobStorage
	written chan struct{}
	once    sync.Once
}

type transientBlobWriteStorage struct {
	blob_storage.BlobStorage
	calls     atomic.Int32
	succeeded chan struct{}
	once      sync.Once
}

func (s *transientBlobWriteStorage) WriteBlobSidecars(ctx context.Context, root common.Hash, sidecars []*cltypes.BlobSidecar) error {
	if s.calls.Add(1) == 1 {
		return errors.New("transient blob write failure")
	}
	if err := s.BlobStorage.WriteBlobSidecars(ctx, root, sidecars); err != nil {
		return err
	}
	s.once.Do(func() { close(s.succeeded) })
	return nil
}

type unavailableRootsBlobStorage struct {
	blob_storage.BlobStorage
	roots    map[common.Hash]struct{}
	attempts atomic.Int32
	started  chan struct{}
	once     sync.Once
}

func (s *unavailableRootsBlobStorage) KzgCommitmentsCount(ctx context.Context, blockRoot common.Hash) (uint32, error) {
	if _, unavailable := s.roots[blockRoot]; unavailable {
		s.attempts.Add(1)
		s.once.Do(func() { close(s.started) })
		return 0, errors.New("blob storage unavailable")
	}
	return s.BlobStorage.KzgCommitmentsCount(ctx, blockRoot)
}

type savedColumnReadCountingStorage struct {
	blob_storage.DataColumnStorage
	reads atomic.Int32
}

type transientSavedColumnIndexStorage struct {
	blob_storage.DataColumnStorage
	calls  atomic.Int32
	failAt int32
	failed chan struct{}
	onFail func()
	once   sync.Once
}

type transientColumnSidecarReadStorage struct {
	blob_storage.DataColumnStorage
	columnIndex   int64
	readErr       error
	targetReads   atomic.Int32
	targetRemoves atomic.Int32
	onFail        func()
	failed        chan struct{}
	once          sync.Once
}

type blockingBlobWriteStorage struct {
	blob_storage.BlobStorage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingBlobWriteStorage) WriteBlobSidecars(ctx context.Context, blockRoot common.Hash, sidecars []*cltypes.BlobSidecar) error {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return s.BlobStorage.WriteBlobSidecars(ctx, blockRoot, sidecars)
	}
}

type postPruneColumnWriteStorage struct {
	blob_storage.DataColumnStorage
	pruned atomic.Bool
	writes atomic.Int32
	want   int32
	done   chan struct{}
	once   sync.Once
}

func (s *postPruneColumnWriteStorage) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, sidecar *cltypes.DataColumnSidecar) error {
	err := s.DataColumnStorage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
	if s.pruned.Load() && s.writes.Add(1) == s.want {
		s.once.Do(func() { close(s.done) })
	}
	return err
}

func (s *savedColumnReadCountingStorage) GetSavedColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash) ([]uint64, error) {
	s.reads.Add(1)
	return s.DataColumnStorage.GetSavedColumnIndex(ctx, slot, blockRoot)
}

func (s *transientSavedColumnIndexStorage) GetSavedColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash) ([]uint64, error) {
	if s.calls.Add(1) == s.failAt {
		if s.onFail != nil {
			s.onFail()
		}
		s.once.Do(func() { close(s.failed) })
		return nil, errors.New("transient saved-column read failure")
	}
	return s.DataColumnStorage.GetSavedColumnIndex(ctx, slot, blockRoot)
}

func (s *transientColumnSidecarReadStorage) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	if columnIndex == s.columnIndex && s.targetReads.Add(1) == 1 {
		if s.onFail != nil {
			s.onFail()
		}
		s.once.Do(func() { close(s.failed) })
		return nil, s.readErr
	}
	return s.DataColumnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, columnIndex)
}

func (s *transientColumnSidecarReadStorage) RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndices ...int64) error {
	for _, columnIndex := range columnIndices {
		if columnIndex == s.columnIndex {
			s.targetRemoves.Add(1)
		}
	}
	return s.DataColumnStorage.RemoveColumnSidecars(ctx, slot, blockRoot, columnIndices...)
}

type blockHashCountingBlock struct {
	cltypes.ColumnSyncableSignedBlock
	calls atomic.Int32
}

func (b *blockHashCountingBlock) BlockHashSSZ() ([32]byte, error) {
	b.calls.Add(1)
	return b.ColumnSyncableSignedBlock.BlockHashSSZ()
}

func (s *writeNotifyingBlobStorage) WriteBlobSidecars(ctx context.Context, blockRoot common.Hash, sidecars []*cltypes.BlobSidecar) error {
	if err := s.BlobStorage.WriteBlobSidecars(ctx, blockRoot, sidecars); err != nil {
		return err
	}
	s.once.Do(func() { close(s.written) })
	return nil
}

func recoverableFuluData(t *testing.T, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, common.Hash, []*cltypes.BlobSidecar, []*cltypes.DataColumnSidecar) {
	return recoverableFuluDataAtSlot(t, cfg, 100)
}

func recoverableFuluDataAtSlot(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, common.Hash, []*cltypes.BlobSidecar, []*cltypes.DataColumnSidecar) {
	return recoverableFuluDataAtSlotWithExecutionBlockHash(t, cfg, slot, common.Hash{})
}

func recoverableFuluDataAtSlotWithExecutionBlockHash(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64, executionBlockHash common.Hash) (*cltypes.SignedBeaconBlock, common.Hash, []*cltypes.BlobSidecar, []*cltypes.DataColumnSidecar) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	block.Block.Slot = slot
	block.Block.Body.ExecutionPayload.BlockHash = executionBlockHash
	blobs := []goethkzg.Blob{{1}, {2}}
	commitments := make([]goethkzg.KZGCommitment, len(blobs))
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, len(blobs))
	for i := range blobs {
		commitment, err := kzg.Ctx().BlobToKZGCommitment(&blobs[i], 0)
		require.NoError(t, err)
		commitments[i] = commitment
		block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
		cells, proofs, err := peerdasutils.ComputeCellsAndKZGProofs(blobs[i][:])
		require.NoError(t, err)
		cellsAndProofs[i] = peerdasutils.CellsAndKZGProofs{Blobs: cells, Proofs: proofs}
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	proofRaw, err := block.Block.Body.KzgCommitmentsInclusionProof()
	require.NoError(t, err)
	columnProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
	for i := range proofRaw {
		columnProof.Set(i, proofRaw[i])
	}
	columns, err := peerdasutils.GetDataColumnSidecars(block.SignedBeaconBlockHeader(), block.GetBlobKzgCommitments(), columnProof, cellsAndProofs)
	require.NoError(t, err)

	sidecars := make([]*cltypes.BlobSidecar, len(blobs))
	for i := range blobs {
		proof, err := kzg.Ctx().ComputeBlobKZGProof(&blobs[i], commitments[i], 0)
		require.NoError(t, err)
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
		require.NoError(t, err)
		inclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for j := range branch {
			inclusionProof.Set(j, common.Hash(branch[j]))
		}
		sidecars[i] = cltypes.NewBlobSidecar(uint64(i), (*cltypes.Blob)(&blobs[i]), common.Bytes48(commitments[i]), common.Bytes48(proof), block.SignedBeaconBlockHeader(), inclusionProof)
	}
	return block, root, sidecars, columns
}

func recoverableGloasColumns(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, common.Hash, []*cltypes.DataColumnSidecar) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	blobs := []goethkzg.Blob{{1}, {2}}
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, len(blobs))
	for i := range blobs {
		commitment, err := kzg.Ctx().BlobToKZGCommitment(&blobs[i], 0)
		require.NoError(t, err)
		block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
		cells, proofs, err := peerdasutils.ComputeCellsAndKZGProofs(blobs[i][:])
		require.NoError(t, err)
		cellsAndProofs[i] = peerdasutils.CellsAndKZGProofs{Blobs: cells, Proofs: proofs}
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	columns, err := peerdasutils.GetDataColumnSidecarsGloas(slot, root, cellsAndProofs)
	require.NoError(t, err)
	return block, root, columns
}

func TestPeerDasPruneBelowUpdatesEarliestAvailableSlot(t *testing.T) {
	tests := []struct {
		name           string
		initial, floor uint64
		pruneErr       error
		want           uint64
	}{
		{name: "advances", initial: 0, floor: 100, want: 100},
		{name: "does not move backwards", initial: 100, floor: 50, want: 100},
		{name: "zero floor resets", initial: 100, floor: 0, want: 0},
		{name: "advances after prune error", initial: 100, floor: 200, pruneErr: errors.New("prune failed"), want: 200},
		{name: "does not advance when prune did not start", initial: 100, floor: 200, pruneErr: fmt.Errorf("readdir: %w", blob_storage.ErrPruneNotStarted), want: 100},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
			columnStorage.EXPECT().PruneBelow(test.floor).Return(test.pruneErr)
			cfg := clparams.MainnetBeaconConfig
			state := peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{})
			state.SetEarliestAvailableSlot(test.initial)
			d := &peerdas{state: state, columnStorage: columnStorage}

			err := d.PruneBelow(test.floor)
			require.ErrorIs(t, err, test.pruneErr)
			require.Equal(t, test.want, state.GetEarliestAvailableSlot())
		})
	}
}

func TestPeerDasPruneBelowUpdatesRecoveryOwnership(t *testing.T) {
	pruneErr := errors.New("partial prune")
	for _, test := range []struct {
		name      string
		err       error
		wantPrune bool
	}{
		{name: "success", wantPrune: true},
		{name: "partial", err: pruneErr, wantPrune: true},
		{name: "not started", err: blob_storage.ErrPruneNotStarted},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
			columnStorage.EXPECT().PruneBelow(uint64(10)).Return(test.err)
			cfg := clparams.MainnetBeaconConfig
			state := peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{})
			d := &peerdas{state: state, columnStorage: columnStorage, recoverBlobsQueue: make(chan recoverBlobsRequest, 8)}
			below := recoverBlobsRequest{slot: 9, blockRoot: common.Hash{9}}
			at := recoverBlobsRequest{slot: 10, blockRoot: common.Hash{10}}
			above := recoverBlobsRequest{slot: 11, blockRoot: common.Hash{11}}
			require.True(t, d.delayBlobRecovery(below))
			require.True(t, d.delayBlobRecovery(at))
			require.True(t, d.delayBlobRecovery(above))
			require.True(t, d.cacheBlobRecoveryResult(below.blockRoot, &blobRecoveryResult{encodedBytes: 1}))

			err := d.PruneBelow(10)
			require.ErrorIs(t, err, test.err)
			if !test.wantPrune {
				require.Contains(t, d.recoveryRetries, below.blockRoot)
				require.Contains(t, d.recoveryResults, below.blockRoot)
				require.Contains(t, d.recoverySlots, below.slot)
				require.Equal(t, 1, d.recoveryResultBytes)
				require.Zero(t, d.recoveryPruneFloor)
				return
			}
			require.NotContains(t, d.recoveryRetries, below.blockRoot)
			require.NotContains(t, d.isRecovering, below.blockRoot)
			require.NotContains(t, d.recoveryResults, below.blockRoot)
			require.NotContains(t, d.recoverySlots, below.slot)
			require.Zero(t, d.recoveryResultBytes)
			require.Contains(t, d.recoveryRetries, at.blockRoot)
			require.Contains(t, d.recoveryRetries, above.blockRoot)
			require.Len(t, d.recoveryRetryQueue, 2)
			require.Equal(t, uint64(10), d.recoveryPruneFloor)
			require.False(t, d.delayBlobRecovery(below))
			require.NotContains(t, d.isRecovering, below.blockRoot)
		})
	}
}

func TestPeerDasPruneBelowHandlesQueuedAndActiveRecovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().PruneBelow(uint64(10)).Return(nil)
	cfg := clparams.MainnetBeaconConfig
	state := peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{})
	d := &peerdas{state: state, columnStorage: columnStorage, recoverBlobsQueue: make(chan recoverBlobsRequest, 2)}
	active := recoverBlobsRequest{slot: 8, blockRoot: common.Hash{8}}
	queued := recoverBlobsRequest{slot: 9, blockRoot: common.Hash{9}}
	require.NoError(t, d.enqueueBlobRecovery(active))
	require.NoError(t, d.enqueueBlobRecovery(queued))
	activeToken := <-d.recoverBlobsQueue
	activeToken, generation, ok := d.claimBlobRecovery(activeToken)
	require.True(t, ok)

	require.NoError(t, d.PruneBelow(10))
	require.True(t, d.isRecovering[active.blockRoot])
	require.False(t, d.delayBlobRecovery(activeToken))
	d.releaseBlobRecovery(active.blockRoot, generation)
	require.NotContains(t, d.isRecovering, active.blockRoot)

	queuedToken := <-d.recoverBlobsQueue
	_, _, ok = d.claimBlobRecovery(queuedToken)
	require.False(t, ok)
	require.NotContains(t, d.isRecovering, queued.blockRoot)
}

func TestPeerDasPruneBelowPreventsActiveRecoveryFromRecreatingColumns(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	const slot = uint64(9_999)
	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, slot)

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	state := peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{})
	state.SetLocalNodeID(enode.NewLocalNode(nodeDB, key))
	custodyColumns, err := state.GetMyCustodyColumns()
	require.NoError(t, err)
	require.NotEmpty(t, custodyColumns)

	columnFS := afero.NewMemMapFs()
	baseColumnStorage := blob_storage.NewDataColumnStore(columnFS, &cfg, beaconevents.NewEventEmitter())
	seeded := 0
	for index, sidecar := range columns {
		if custodyColumns[uint64(index)] {
			continue
		}
		require.NoError(t, baseColumnStorage.WriteColumnSidecars(t.Context(), root, int64(index), sidecar))
		seeded++
		if seeded == int((cfg.NumberOfColumns+1)/2) {
			break
		}
	}
	require.Equal(t, int((cfg.NumberOfColumns+1)/2), seeded)
	columnStorage := &postPruneColumnWriteStorage{
		DataColumnStorage: baseColumnStorage,
		want:              int32(len(custodyColumns)),
		done:              make(chan struct{}),
	}

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobFS := afero.NewMemMapFs()
	baseBlobStorage := blob_storage.NewBlobStore(db, blobFS)
	blobStorage := &blockingBlobWriteStorage{
		BlobStorage: baseBlobStorage,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d := &peerdas{
		beaconConfig:      &cfg,
		state:             state,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	require.NoError(t, d.enqueueBlobRecovery(recoverBlobsRequest{slot: slot, blockRoot: root, metadata: metadata}))

	ctx, cancel := context.WithCancel(t.Context())
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()
	t.Cleanup(func() {
		select {
		case <-blobStorage.release:
		default:
			close(blobStorage.release)
		}
		cancel()
		<-workerDone
	})
	select {
	case <-blobStorage.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("blob recovery did not reach persistence")
	}

	require.NoError(t, d.PruneBelow(slot+1))
	columnStorage.pruned.Store(true)
	close(blobStorage.release)
	select {
	case <-columnStorage.done:
	case <-time.After(30 * time.Second):
		t.Fatal("blob recovery did not finish its column persistence suffix")
	}

	freshColumns := blob_storage.NewDataColumnStore(columnFS, &cfg, beaconevents.NewEventEmitter())
	storedColumns, err := freshColumns.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Empty(t, storedColumns)
	blobs, found, err := baseBlobStorage.ReadBlobSidecars(t.Context(), slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, blobs, 2)
}

func TestGloasRecoveryPersistsGeneratedCustodyColumnAtOriginalIdentity(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	slot := 2 * cfg.SlotsPerEpoch
	block, root, columns := recoverableGloasColumns(t, &cfg, slot)

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	state := peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{})
	state.SetLocalNodeID(enode.NewLocalNode(nodeDB, key))
	custodyColumns, err := state.GetMyCustodyColumns()
	require.NoError(t, err)
	require.NotEmpty(t, custodyColumns)

	columnFS := afero.NewMemMapFs()
	columnStorage := blob_storage.NewDataColumnStore(columnFS, &cfg, beaconevents.NewEventEmitter())
	seeded := 0
	for index, sidecar := range columns {
		if custodyColumns[uint64(index)] {
			continue
		}
		require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(index), sidecar))
		seeded++
		if seeded == int((cfg.NumberOfColumns+1)/2) {
			break
		}
	}
	require.Equal(t, int((cfg.NumberOfColumns+1)/2), seeded)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobStorage := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	gloasDataCache, err := lru.New[common.Hash, *gloasBlockData]("testGloasData", 1)
	require.NoError(t, err)
	gloasDataCache.Add(root, &gloasBlockData{
		BlobKzgCommitments:      block.GetBlobKzgCommitments(),
		SignedBeaconBlockHeader: block.SignedBeaconBlockHeader(),
	})
	d := &peerdas{
		beaconConfig:      &cfg,
		state:             state,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
		gloasDataCache:    gloasDataCache,
	}
	require.NoError(t, d.enqueueBlobRecovery(recoverBlobsRequest{slot: slot, blockRoot: root, metadata: metadata}))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })

	var stored *cltypes.DataColumnSidecar
	for index := range custodyColumns {
		require.Eventually(t, func() bool {
			fresh := blob_storage.NewDataColumnStore(columnFS, &cfg, beaconevents.NewEventEmitter())
			var readErr error
			stored, readErr = fresh.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, int64(index))
			return readErr == nil
		}, 10*time.Second, 10*time.Millisecond)
		break
	}
	require.Equal(t, clparams.GloasVersion, stored.Version())
	require.Equal(t, slot, stored.Slot)
	require.Equal(t, root, stored.BeaconBlockRoot)
}

func TestDownloadColumnsAndRecoverBlobsAdmitsPartialBlobCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, root).Return(nil, nil).Times(2)
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil)
	d := &peerdas{beaconConfig: &cfg, columnStorage: columnStorage, blobStorage: blobStorage}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block}))
}

func assertDownloadColumnsOverwritesBlobStorage(t *testing.T, initialSidecars func([]*cltypes.BlobSidecar) []*cltypes.BlobSidecar) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, columns := recoverableFuluData(t, &cfg)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fs := afero.NewMemMapFs()
	storage := blob_storage.NewBlobStore(db, fs)
	require.NoError(t, storage.WriteBlobSidecars(t.Context(), root, initialSidecars(sidecars)))
	notifying := &writeNotifyingBlobStorage{BlobStorage: storage, written: make(chan struct{})}
	columnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	d := &peerdas{
		beaconConfig:      &cfg,
		caplinConfig:      &clparams.CaplinConfig{ArchiveBlobs: true},
		state:             peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:     columnStorage,
		blobStorage:       notifying,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	go d.blobsRecoverWorker(ctx)

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block}))
	select {
	case <-notifying.written:
	case <-time.After(30 * time.Second):
		t.Fatal("incomplete or invalid blob storage was not overwritten by PeerDAS recovery")
	}

	fresh := blob_storage.NewBlobStore(db, fs)
	stored, found, err := fresh.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, stored, len(sidecars))
	require.NoError(t, blob_storage.VerifyBlobSidecars(stored, clparams.FuluVersion, nil))
	for i := range stored {
		require.Equal(t, uint64(i), stored[i].Index)
		require.Equal(t, sidecars[i].KzgCommitment, stored[i].KzgCommitment)
	}
}

func TestDownloadColumnsAndRecoverBlobsOverwritesPartialBlobStorage(t *testing.T) {
	assertDownloadColumnsOverwritesBlobStorage(t, func(sidecars []*cltypes.BlobSidecar) []*cltypes.BlobSidecar {
		return sidecars[:1]
	})
}

func TestDownloadColumnsAndRecoverBlobsOverwritesInvalidBlobStorageWithExpectedCount(t *testing.T) {
	assertDownloadColumnsOverwritesBlobStorage(t, func(sidecars []*cltypes.BlobSidecar) []*cltypes.BlobSidecar {
		return []*cltypes.BlobSidecar{sidecars[0], sidecars[0]}
	})
}

func TestDownloadColumnsAndRecoverBlobsRetriesTransientInitialRecoveryRead(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, columns := recoverableFuluData(t, &cfg)

	baseColumnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, baseColumnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}
	columnStorage := &transientSavedColumnIndexStorage{
		DataColumnStorage: baseColumnStorage,
		failAt:            3,
		failed:            make(chan struct{}),
	}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobFS := afero.NewMemMapFs()
	baseBlobStorage := blob_storage.NewBlobStore(db, blobFS)
	blobStorage := &writeNotifyingBlobStorage{BlobStorage: baseBlobStorage, written: make(chan struct{})}
	d := &peerdas{
		beaconConfig:      &cfg,
		caplinConfig:      &clparams.CaplinConfig{ArchiveBlobs: true},
		state:             peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	ctx, cancel := context.WithCancel(t.Context())
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()
	t.Cleanup(func() {
		cancel()
		<-workerDone
	})

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block}))
	select {
	case <-columnStorage.failed:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not reach the transient saved-column read failure")
	}
	d.recoveringMutex.Lock()
	_, ownerRetained := d.recoveryRequests[root]
	d.recoveringMutex.Unlock()
	require.True(t, ownerRetained, "transient recovery read must retain exact ownership")

	select {
	case <-blobStorage.written:
	case <-time.After(30 * time.Second):
		t.Fatal("transient saved-column read lost the sole recovery owner")
	}
	require.Eventually(t, func() bool {
		d.recoveringMutex.Lock()
		defer d.recoveringMutex.Unlock()
		_, owned := d.recoveryRequests[root]
		return !owned
	}, 30*time.Second, 10*time.Millisecond)
	fresh := blob_storage.NewBlobStore(db, blobFS)
	stored, found, err := fresh.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, stored, len(sidecars))
}

func TestDownloadColumnsAndRecoverBlobsDoesNotRetryCanceledInitialRecoveryRead(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluData(t, &cfg)

	baseColumnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, baseColumnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}
	ctx, cancel := context.WithCancel(t.Context())
	columnStorage := &transientSavedColumnIndexStorage{
		DataColumnStorage: baseColumnStorage,
		failAt:            3,
		failed:            make(chan struct{}),
		onFail:            cancel,
	}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobStorage := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	d := &peerdas{
		beaconConfig:      &cfg,
		caplinConfig:      &clparams.CaplinConfig{ArchiveBlobs: true},
		state:             peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block}))
	select {
	case <-columnStorage.failed:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not reach the canceled saved-column read")
	}
	select {
	case <-workerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("canceled recovery worker did not stop")
	}
	require.Equal(t, int32(3), columnStorage.calls.Load())
	d.recoveringMutex.Lock()
	_, owned := d.recoveryRequests[root]
	d.recoveringMutex.Unlock()
	require.False(t, owned)
	stored, found, err := blobStorage.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, stored)
}

func TestDownloadColumnsAndRecoverBlobsRetriesTransientColumnReadWithoutRemoving(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, columns := recoverableFuluData(t, &cfg)

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	state := peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{})
	state.SetLocalNodeID(enode.NewLocalNode(nodeDB, key))
	custodyColumns, err := state.GetMyCustodyColumns()
	require.NoError(t, err)
	require.NotEmpty(t, custodyColumns)
	var targetColumn uint64
	for column := range custodyColumns {
		targetColumn = column
		break
	}

	columnFS := afero.NewMemMapFs()
	baseColumnStorage := blob_storage.NewDataColumnStore(columnFS, &cfg, beaconevents.NewEventEmitter())
	require.NoError(t, baseColumnStorage.WriteColumnSidecars(t.Context(), root, int64(targetColumn), columns[targetColumn]))
	seeded := 1
	for column, sidecar := range columns {
		if uint64(column) == targetColumn {
			continue
		}
		require.NoError(t, baseColumnStorage.WriteColumnSidecars(t.Context(), root, int64(column), sidecar))
		seeded++
		if seeded == int((cfg.NumberOfColumns+1)/2) {
			break
		}
	}
	require.Equal(t, int((cfg.NumberOfColumns+1)/2), seeded)
	columnStorage := &transientColumnSidecarReadStorage{
		DataColumnStorage: baseColumnStorage,
		columnIndex:       int64(targetColumn),
		readErr:           errors.New("transient column-sidecar read failure"),
		failed:            make(chan struct{}),
	}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobFS := afero.NewMemMapFs()
	baseBlobStorage := blob_storage.NewBlobStore(db, blobFS)
	blobStorage := &writeNotifyingBlobStorage{BlobStorage: baseBlobStorage, written: make(chan struct{})}
	d := &peerdas{
		beaconConfig:      &cfg,
		caplinConfig:      &clparams.CaplinConfig{ArchiveBlobs: true},
		state:             state,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	ctx, cancel := context.WithCancel(t.Context())
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()
	t.Cleanup(func() {
		cancel()
		<-workerDone
	})

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block}))
	select {
	case <-columnStorage.failed:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not reach the transient column-sidecar read failure")
	}
	select {
	case <-blobStorage.written:
	case <-time.After(30 * time.Second):
		t.Fatal("transient column-sidecar read lost the sole recovery owner")
	}
	require.Eventually(t, func() bool {
		d.recoveringMutex.Lock()
		defer d.recoveringMutex.Unlock()
		_, owned := d.recoveryRequests[root]
		return !owned
	}, 30*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(2), columnStorage.targetReads.Load())
	require.Zero(t, columnStorage.targetRemoves.Load())
	_, err = baseColumnStorage.ReadColumnSidecarByColumnIndex(t.Context(), block.Block.Slot, root, int64(targetColumn))
	require.NoError(t, err)
	fresh := blob_storage.NewBlobStore(db, blobFS)
	stored, found, err := fresh.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, stored, len(sidecars))
}

func TestDownloadColumnsAndRecoverBlobsRemovesMissingColumnAndReleasesOwner(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluData(t, &cfg)

	baseColumnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for column := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, baseColumnStorage.WriteColumnSidecars(t.Context(), root, int64(column), columns[column]))
	}
	columnStorage := &transientColumnSidecarReadStorage{
		DataColumnStorage: baseColumnStorage,
		columnIndex:       0,
		readErr:           os.ErrNotExist,
		failed:            make(chan struct{}),
	}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobStorage := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	d := &peerdas{
		beaconConfig:      &cfg,
		caplinConfig:      &clparams.CaplinConfig{ArchiveBlobs: true},
		state:             peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	ctx, cancel := context.WithCancel(t.Context())
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()
	t.Cleanup(func() {
		cancel()
		<-workerDone
	})

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block}))
	select {
	case <-columnStorage.failed:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not reach the missing column-sidecar read")
	}
	require.Eventually(t, func() bool {
		d.recoveringMutex.Lock()
		_, owned := d.recoveryRequests[root]
		d.recoveringMutex.Unlock()
		return !owned && columnStorage.targetRemoves.Load() == 1
	}, 30*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), columnStorage.targetReads.Load())
	_, err := baseColumnStorage.ReadColumnSidecarByColumnIndex(t.Context(), block.Block.Slot, root, 0)
	require.ErrorIs(t, err, os.ErrNotExist)
	saved, err := baseColumnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, int((cfg.NumberOfColumns+1)/2)-1)
	stored, found, err := blobStorage.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, stored)
}

func TestDownloadColumnsAndRecoverBlobsDoesNotRetryCanceledColumnRead(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluData(t, &cfg)

	baseColumnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for column := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, baseColumnStorage.WriteColumnSidecars(t.Context(), root, int64(column), columns[column]))
	}
	ctx, cancel := context.WithCancel(t.Context())
	columnStorage := &transientColumnSidecarReadStorage{
		DataColumnStorage: baseColumnStorage,
		columnIndex:       0,
		readErr:           errors.New("transient column-sidecar read failure"),
		onFail:            cancel,
		failed:            make(chan struct{}),
	}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobStorage := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	d := &peerdas{
		beaconConfig:      &cfg,
		caplinConfig:      &clparams.CaplinConfig{ArchiveBlobs: true},
		state:             peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block}))
	select {
	case <-columnStorage.failed:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not reach the canceled column-sidecar read")
	}
	select {
	case <-workerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
	require.Equal(t, int32(1), columnStorage.targetReads.Load())
	require.Zero(t, columnStorage.targetRemoves.Load())
	d.recoveringMutex.Lock()
	_, owned := d.recoveryRequests[root]
	d.recoveringMutex.Unlock()
	require.False(t, owned)
	d.recoveryRetryMutex.Lock()
	_, delayed := d.recoveryRetries[root]
	d.recoveryRetryMutex.Unlock()
	require.False(t, delayed)
	_, err := baseColumnStorage.ReadColumnSidecarByColumnIndex(t.Context(), block.Block.Slot, root, 0)
	require.NoError(t, err)
	stored, found, err := blobStorage.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, stored)
}

func TestBlobRecoveryRetriesTransientFinalWrite(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, columns := recoverableFuluData(t, &cfg)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	baseBlobStorage := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	blobStorage := &transientBlobWriteStorage{BlobStorage: baseBlobStorage, succeeded: make(chan struct{})}
	baseColumnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	columnStorage := &savedColumnReadCountingStorage{DataColumnStorage: baseColumnStorage}
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}

	ctx, cancel := context.WithCancel(t.Context())
	d := &peerdas{
		beaconConfig:      &cfg,
		caplinConfig:      &clparams.CaplinConfig{ArchiveBlobs: true},
		state:             peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	done := make(chan struct{})
	go func() { d.blobsRecoverWorker(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block}))
	select {
	case <-blobStorage.succeeded:
	case <-time.After(10 * time.Second):
		t.Fatal("transient final write dropped the recovery owner")
	}
	require.Equal(t, int32(2), blobStorage.calls.Load())
	require.Equal(t, int32(3), columnStorage.reads.Load(), "final-write retry must reuse reconstruction")
	stored, found, err := baseBlobStorage.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, stored, len(sidecars))
}

func TestDownloadColumnsAndRecoverBlobsRetainsAllDelayedRootsAndAdmitsHealthyRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluData(t, &cfg)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	storage := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	notifying := &writeNotifyingBlobStorage{BlobStorage: storage, written: make(chan struct{})}
	unavailable := &unavailableRootsBlobStorage{
		BlobStorage: notifying,
		roots:       make(map[common.Hash]struct{}),
		started:     make(chan struct{}),
	}
	columnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}

	d := &peerdas{
		beaconConfig:      &cfg,
		caplinConfig:      &clparams.CaplinConfig{ArchiveBlobs: true},
		state:             peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:     columnStorage,
		blobStorage:       unavailable,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 128),
		isRecovering:      make(map[common.Hash]bool),
	}
	poisonedCount := cap(d.recoverBlobsQueue) + 1
	for i := range poisonedCount {
		poisonedRoot := common.Hash{byte(i + 1), 0xff}
		unavailable.roots[poisonedRoot] = struct{}{}
		d.delayBlobRecovery(recoverBlobsRequest{slot: block.Block.Slot, blockRoot: poisonedRoot})
	}
	require.Len(t, d.recoveryRetries, poisonedCount)
	require.Len(t, d.isRecovering, poisonedCount)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var workers sync.WaitGroup
	for range numOfBlobRecoveryWorkers {
		workers.Go(func() { d.blobsRecoverWorker(ctx) })
	}
	select {
	case <-unavailable.started:
	case <-time.After(3 * time.Second):
		cancel()
		workers.Wait()
		t.Fatal("delayed poison roots were not retried")
	}
	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(t.Context(), []cltypes.ColumnSyncableSignedBlock{block}))
	select {
	case <-notifying.written:
	case <-time.After(10 * time.Second):
		cancel()
		workers.Wait()
		t.Fatal("delayed retries blocked a fresh healthy recovery root")
	}
	require.Positive(t, unavailable.attempts.Load())
	d.recoveringMutex.Lock()
	for poisonedRoot := range unavailable.roots {
		require.Contains(t, d.isRecovering, poisonedRoot)
	}
	d.recoveringMutex.Unlock()
	cancel()
	workers.Wait()
}

func TestBlobsRecoverWorkerStopsWhenDurableCheckIsCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	readStarted := make(chan struct{})
	postCancelRecovery := make(chan struct{})
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil)
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(ctx context.Context, _ uint64, _ common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			close(readStarted)
			<-ctx.Done()
			return nil, false, ctx.Err()
		},
	)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			close(postCancelRecovery)
			return nil, errors.New("unexpected recovery after cancellation")
		},
	).AnyTimes()

	ctx, cancel := context.WithCancel(t.Context())
	d := &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d.recoverBlobsQueue <- recoverBlobsRequest{slot: block.Block.Slot, blockRoot: root, metadata: metadata}
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()

	<-readStarted
	cancel()
	<-workerDone
	select {
	case <-postCancelRecovery:
		t.Fatal("worker started recovery after its durable check was canceled")
	default:
	}
}

func TestBlobsRecoverWorkerStopsWhenRecoveryContextIsCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfg := clparams.MainnetBeaconConfig
	root := common.HexToHash("0xcafe")
	ctx, cancel := context.WithCancel(t.Context())
	postCancelColumnRead := make(chan struct{})

	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(100), root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			cancel()
			return make([]uint64, (cfg.NumberOfColumns+1)/2), nil
		},
	)
	columnStorage.EXPECT().ReadColumnSidecarByColumnIndex(gomock.Any(), uint64(100), root, gomock.Any()).DoAndReturn(
		func(context.Context, uint64, common.Hash, int64) (*cltypes.DataColumnSidecar, error) {
			close(postCancelColumnRead)
			return nil, errors.New("unexpected column read after cancellation")
		},
	).AnyTimes()
	columnStorage.EXPECT().RemoveColumnSidecars(gomock.Any(), uint64(100), root, gomock.Any()).Return(nil).AnyTimes()

	d := &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	d.recoverBlobsQueue <- recoverBlobsRequest{slot: 100, blockRoot: root}
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()

	<-workerDone
	select {
	case <-postCancelColumnRead:
		t.Fatal("worker read recovery columns after its context was canceled")
	default:
	}
}

func TestBlobsRecoverWorkerRechecksDurableCompletionBeforeWrite(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, columns := recoverableFuluData(t, &cfg)
	columnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}

	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	var countReads atomic.Int32
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).DoAndReturn(
		func(context.Context, common.Hash) (uint32, error) {
			if countReads.Add(1) == 1 {
				return 0, nil
			}
			return uint32(len(sidecars)), nil
		},
	).AnyTimes()
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			cancel()
			return sidecars, true, nil
		},
	).AnyTimes()
	overwriteAttempted := make(chan struct{})
	var overwriteOnce sync.Once
	blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), root, gomock.Any()).DoAndReturn(
		func(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
			overwriteOnce.Do(func() { close(overwriteAttempted) })
			cancel()
			return errors.New("unexpected overwrite of concurrent completion")
		},
	).AnyTimes()

	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d := &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	d.recoverBlobsQueue <- recoverBlobsRequest{slot: block.Block.Slot, blockRoot: root, metadata: metadata}
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()
	select {
	case <-workerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("blob recovery worker did not finish")
	}
	select {
	case <-overwriteAttempted:
		t.Fatal("worker overwrote blob storage that completed during recovery")
	default:
	}
	require.GreaterOrEqual(t, countReads.Load(), int32(2), "worker must revalidate immediately before writing")
}

func TestMetadataFreeBlobRecoveryRechecksConcurrentCompletionBeforeWrite(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluData(t, &cfg)
	columnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}

	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	var countReads atomic.Int32
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).DoAndReturn(
		func(context.Context, common.Hash) (uint32, error) {
			if countReads.Add(1) == 1 {
				return 0, nil
			}
			cancel()
			return 1, nil
		},
	).AnyTimes()
	overwriteAttempted := make(chan struct{})
	var overwriteOnce sync.Once
	blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), root, gomock.Any()).DoAndReturn(
		func(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
			overwriteOnce.Do(func() { close(overwriteAttempted) })
			return errors.New("unexpected metadata-free overwrite")
		},
	).AnyTimes()
	d := &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	require.NoError(t, d.enqueueBlobRecovery(recoverBlobsRequest{slot: block.Block.Slot, blockRoot: root}))
	d.blobsRecoverWorker(ctx)

	select {
	case <-overwriteAttempted:
		t.Fatal("metadata-free recovery overwrote concurrent durable data")
	default:
	}
	require.GreaterOrEqual(t, countReads.Load(), int32(2))
}

func TestBlobsRecoverWorkerContinuesAfterTransientAdmissionValidation(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, columns := recoverableFuluData(t, &cfg)
	columnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}

	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).DoAndReturn(
		func(context.Context, common.Hash) (uint32, error) {
			return uint32(len(sidecars)), nil
		},
	).AnyTimes()
	var durableReads atomic.Int32
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			if durableReads.Add(1) == 1 {
				return nil, false, errors.New("transient admission read failure")
			}
			return nil, false, nil
		},
	).AnyTimes()
	writeAttempted := make(chan struct{})
	var writeOnce sync.Once
	blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), root, gomock.Any()).DoAndReturn(
		func(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
			writeOnce.Do(func() { close(writeAttempted) })
			cancel()
			return errors.New("stop after observing recovery write")
		},
	).AnyTimes()

	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d := &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 2),
		isRecovering:      make(map[common.Hash]bool),
	}
	d.recoverBlobsQueue <- recoverBlobsRequest{slot: block.Block.Slot, blockRoot: root, metadata: metadata}
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()
	select {
	case <-workerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("blob recovery worker did not release capacity")
	}
	select {
	case <-writeAttempted:
	default:
		t.Fatal("transient admission validation dropped the only recovery owner")
	}
	require.Equal(t, int32(3), durableReads.Load(), "worker must revalidate after the delay and at the write boundary")
}

func TestBlobsRecoverWorkerRetriesTransientPrewriteValidation(t *testing.T) {
	assertBlobsRecoverWorkerPrewriteUnavailable(t, false)
}

func TestBlobsRecoverWorkerPacesPersistentPrewriteUnavailableUntilCancellation(t *testing.T) {
	assertBlobsRecoverWorkerPrewriteUnavailable(t, true)
}

func TestBlobsRecoverWorkersDoNotStarveHealthyRootBehindPersistentUnavailable(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, _, sidecars, columns := recoverableFuluData(t, &cfg)
	existingColumns := make([]uint64, (cfg.NumberOfColumns+1)/2)
	encodedColumns := make([][]byte, len(existingColumns))
	for i := range existingColumns {
		existingColumns[i] = uint64(i)
		encoded, err := columns[i].EncodeSSZ(nil)
		require.NoError(t, err)
		encodedColumns[i] = encoded
	}

	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, gomock.Any()).Return(existingColumns, nil).AnyTimes()
	columnStorage.EXPECT().ReadColumnSidecarByColumnIndex(gomock.Any(), block.Block.Slot, gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint64, _ common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
			column := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
			if err := column.DecodeSSZ(encodedColumns[columnIndex], int(clparams.FuluVersion)); err != nil {
				return nil, err
			}
			return column, nil
		},
	).AnyTimes()

	poisonedRoots := make(map[common.Hash]bool, numOfBlobRecoveryWorkers)
	requests := make([]recoverBlobsRequest, 0, numOfBlobRecoveryWorkers+1)
	baseMetadata, err := newBlobRecoveryMetadata(block, common.Hash{})
	require.NoError(t, err)
	for i := range numOfBlobRecoveryWorkers {
		root := common.Hash{byte(i + 1)}
		poisonedRoots[root] = true
		metadata := *baseMetadata
		metadata.blockRoot = root
		requests = append(requests, recoverBlobsRequest{slot: block.Block.Slot, blockRoot: root, metadata: &metadata})
	}
	healthyRoot := common.Hash{0xff}
	healthyMetadata := *baseMetadata
	healthyMetadata.blockRoot = healthyRoot
	healthyRequest := recoverBlobsRequest{slot: block.Block.Slot, blockRoot: healthyRoot, metadata: &healthyMetadata}

	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	var storageMutex sync.Mutex
	countReads := make(map[common.Hash]int)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, root common.Hash) (uint32, error) {
			storageMutex.Lock()
			defer storageMutex.Unlock()
			countReads[root]++
			if countReads[root] == 1 {
				return 0, nil
			}
			return uint32(len(sidecars)), nil
		},
	).AnyTimes()
	allPoisonedObserved := make(chan struct{})
	poisonedSeen := make(map[common.Hash]bool, numOfBlobRecoveryWorkers)
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint64, root common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			if poisonedRoots[root] {
				storageMutex.Lock()
				if !poisonedSeen[root] {
					poisonedSeen[root] = true
					if len(poisonedSeen) == numOfBlobRecoveryWorkers {
						close(allPoisonedObserved)
					}
				}
				storageMutex.Unlock()
				return nil, false, errors.New("persistent storage outage")
			}
			return nil, false, nil
		},
	).AnyTimes()
	healthyWritten := make(chan struct{})
	poisonedWrite := make(chan struct{})
	var healthyWriteOnce sync.Once
	var poisonedWriteOnce sync.Once
	blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, root common.Hash, _ []*cltypes.BlobSidecar) error {
			if poisonedRoots[root] {
				poisonedWriteOnce.Do(func() { close(poisonedWrite) })
			} else if root == healthyRoot {
				healthyWriteOnce.Do(func() { close(healthyWritten) })
			}
			return errors.New("stop after observing recovery write")
		},
	).AnyTimes()

	ctx, cancel := context.WithCancel(t.Context())
	d := &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 16),
		isRecovering:      make(map[common.Hash]bool),
	}
	for _, request := range requests {
		d.recoverBlobsQueue <- request
	}
	workersDone := make(chan struct{})
	var workers sync.WaitGroup
	for range numOfBlobRecoveryWorkers {
		workers.Go(func() { d.blobsRecoverWorker(ctx) })
	}
	go func() {
		workers.Wait()
		close(workersDone)
	}()
	select {
	case <-allPoisonedObserved:
	case <-time.After(30 * time.Second):
		cancel()
		<-workersDone
		t.Fatal("not every recovery worker reached persistent unavailable storage")
	}
	d.recoverBlobsQueue <- healthyRequest
	select {
	case <-healthyWritten:
	case <-time.After(10 * time.Second):
		cancel()
		<-workersDone
		t.Fatal("persistent unavailable roots starved a healthy recovery root")
	}
	select {
	case <-poisonedWrite:
		t.Fatal("persistent unavailable storage was overwritten")
	default:
	}
	cancel()
	select {
	case <-workersDone:
	case <-time.After(30 * time.Second):
		t.Fatal("recovery workers did not drain after cancellation")
	}
	d.recoveringMutex.Lock()
	require.Empty(t, d.isRecovering)
	d.recoveringMutex.Unlock()
	d.recoveryRetryMutex.Lock()
	require.Empty(t, d.recoveryRetries)
	d.recoveryRetryMutex.Unlock()
	d.recoveringMutex.Lock()
	require.Empty(t, d.recoveryResults)
	require.Zero(t, d.recoveryResultBytes)
	d.recoveringMutex.Unlock()
}

func TestEnqueueBlobRecoveryDeduplicatesOwnedRoot(t *testing.T) {
	d := &peerdas{
		recoverBlobsQueue: make(chan recoverBlobsRequest, 2),
		isRecovering:      make(map[common.Hash]bool),
	}
	request := recoverBlobsRequest{blockRoot: common.Hash{1}}
	require.NoError(t, d.enqueueBlobRecovery(request))
	require.NoError(t, d.enqueueBlobRecovery(request))
	require.Len(t, d.recoverBlobsQueue, 1)
	require.Len(t, d.isRecovering, 1)

	queued := <-d.recoverBlobsQueue
	_, generation, ok := d.claimBlobRecovery(queued)
	require.True(t, ok)
	_, _, ok = d.claimBlobRecovery(queued)
	require.False(t, ok)
	d.releaseBlobRecovery(queued.blockRoot, generation)
	require.Empty(t, d.isRecovering)
}

func TestEnqueueBlobRecoveryPreservesOwnershipAtCapacity(t *testing.T) {
	d := &peerdas{
		recoverBlobsQueue: make(chan recoverBlobsRequest, 2),
		isRecovering:      make(map[common.Hash]bool),
	}
	first := recoverBlobsRequest{blockRoot: common.Hash{1}}
	second := recoverBlobsRequest{blockRoot: common.Hash{2}}
	third := recoverBlobsRequest{blockRoot: common.Hash{3}}
	require.NoError(t, d.enqueueBlobRecovery(first))
	require.NoError(t, d.enqueueBlobRecovery(second))
	require.NoError(t, d.enqueueBlobRecovery(third))
	require.Len(t, d.recoverBlobsQueue, 2)
	require.Len(t, d.isRecovering, 3)
	require.Contains(t, d.recoveryRetries, third.blockRoot)

	queued := <-d.recoverBlobsQueue
	_, generation, ok := d.claimBlobRecovery(queued)
	require.True(t, ok)
	d.releaseBlobRecovery(queued.blockRoot, generation)
	d.recoveryRetries[third.blockRoot].notBefore = time.Now()
	d.recoveryPreferRetry = true
	retry, ok := d.nextBlobRecoveryRequest(t.Context())
	require.True(t, ok)
	require.Equal(t, third.blockRoot, retry.blockRoot)
	require.Len(t, d.recoverBlobsQueue, 1)
	require.Len(t, d.isRecovering, 2)
}

func TestNextBlobRecoveryRequestAlternatesReadyQueueAndRetry(t *testing.T) {
	d := &peerdas{
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]bool),
	}
	queued := recoverBlobsRequest{blockRoot: common.Hash{1}}
	delayed := recoverBlobsRequest{blockRoot: common.Hash{2}}
	d.recoverBlobsQueue <- queued
	require.True(t, d.delayBlobRecovery(delayed))
	d.recoveryRetryMutex.Lock()
	d.recoveryRetries[delayed.blockRoot].notBefore = time.Now().Add(-time.Second)
	heap.Fix(&d.recoveryRetryQueue, d.recoveryRetries[delayed.blockRoot].heapIndex)
	d.recoveryRetryMutex.Unlock()

	first, ok := d.nextBlobRecoveryRequest(t.Context())
	require.True(t, ok)
	require.Equal(t, queued.blockRoot, first.blockRoot)
	second, ok := d.nextBlobRecoveryRequest(t.Context())
	require.True(t, ok)
	require.Equal(t, delayed.blockRoot, second.blockRoot)
}

func TestNextBlobRecoveryRequestGloballyPacesDelayedRetries(t *testing.T) {
	d := &peerdas{recoverBlobsQueue: make(chan recoverBlobsRequest, 1)}
	const retries = 9
	for i := range retries {
		request := recoverBlobsRequest{slot: uint64(i + 1), blockRoot: common.Hash{byte(i + 1)}}
		require.True(t, d.delayBlobRecovery(request))
	}
	d.recoveryRetryMutex.Lock()
	for _, delayed := range d.recoveryRetries {
		delayed.notBefore = time.Now().Add(-time.Second)
	}
	heap.Init(&d.recoveryRetryQueue)
	d.recoveryRetryMutex.Unlock()

	started := time.Now()
	for range retries {
		_, ok := d.nextBlobRecoveryRequest(t.Context())
		require.True(t, ok)
	}
	require.GreaterOrEqual(t, time.Since(started), blobRecoveryValidationRetryInterval-10*time.Millisecond)
	require.Len(t, d.isRecovering, retries)
	require.Empty(t, d.recoveryRetries)

	typeOfPeerDas := reflect.TypeFor[peerdas]()
	heapField, hasHeap := typeOfPeerDas.FieldByName("recoveryRetryQueue")
	_, hasLinearOrder := typeOfPeerDas.FieldByName("recoveryRetryOrder")
	require.True(t, hasHeap)
	require.True(t, reflect.PointerTo(heapField.Type).Implements(reflect.TypeFor[heap.Interface]()))
	require.False(t, hasLinearOrder)
}

func TestBlobRecoveryMetadataUpgradeRepairsInvalidEqualCountStorage(t *testing.T) {
	for _, lifecycle := range []string{"queued", "active", "delayed"} {
		t.Run(lifecycle, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.FuluForkEpoch = 0
			cfg.InitializeForkSchedule()
			initTestBeaconConfig(&cfg)
			block, root, sidecars, columns := recoverableFuluData(t, &cfg)
			columnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
			for i := range (cfg.NumberOfColumns + 1) / 2 {
				require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
			}

			ctrl := gomock.NewController(t)
			firstCountStarted := make(chan struct{})
			releaseFirstCount := make(chan struct{})
			var countCalls atomic.Int32
			blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
			blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).DoAndReturn(
				func(context.Context, common.Hash) (uint32, error) {
					if lifecycle == "active" && countCalls.Add(1) == 1 {
						close(firstCountStarted)
						<-releaseFirstCount
					}
					return uint32(len(sidecars)), nil
				},
			).AnyTimes()
			blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).Return(nil, false, nil).AnyTimes()
			written := make(chan struct{})
			var writeOnce sync.Once
			ctx, cancel := context.WithCancel(t.Context())
			blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), root, gomock.Any()).DoAndReturn(
				func(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
					writeOnce.Do(func() { close(written) })
					cancel()
					return errors.New("stop after metadata repair")
				},
			).AnyTimes()
			metadata, err := newBlobRecoveryMetadata(block, root)
			require.NoError(t, err)
			d := &peerdas{
				beaconConfig:      &cfg,
				state:             peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
				columnStorage:     columnStorage,
				blobStorage:       blobStorage,
				recoverBlobsQueue: make(chan recoverBlobsRequest, 2),
				isRecovering:      make(map[common.Hash]bool),
			}
			nilRequest := recoverBlobsRequest{slot: block.Block.Slot, blockRoot: root}
			strongRequest := recoverBlobsRequest{slot: block.Block.Slot, blockRoot: root, metadata: metadata}
			switch lifecycle {
			case "queued":
				require.NoError(t, d.enqueueBlobRecovery(nilRequest))
				require.NoError(t, d.enqueueBlobRecovery(strongRequest))
			case "delayed":
				d.delayBlobRecovery(nilRequest)
				require.NoError(t, d.enqueueBlobRecovery(strongRequest))
			case "active":
				require.NoError(t, d.enqueueBlobRecovery(nilRequest))
			}

			workerDone := make(chan struct{})
			go func() {
				d.blobsRecoverWorker(ctx)
				close(workerDone)
			}()
			if lifecycle == "active" {
				select {
				case <-firstCountStarted:
				case <-time.After(10 * time.Second):
					cancel()
					<-workerDone
					t.Fatal("metadata-free recovery did not become active")
				}
				require.NoError(t, d.enqueueBlobRecovery(strongRequest))
				close(releaseFirstCount)
			}
			select {
			case <-written:
			case <-time.After(30 * time.Second):
				cancel()
				<-workerDone
				t.Fatal("stronger metadata did not retain recovery ownership")
			}
			cancel()
			<-workerDone
		})
	}
}

func TestBlobRecoveryResultCacheEnforcesByteCapacity(t *testing.T) {
	d := &peerdas{}
	firstRoot := common.Hash{1}
	secondRoot := common.Hash{2}
	require.True(t, d.cacheBlobRecoveryResult(firstRoot, &blobRecoveryResult{encodedBytes: maxBlobRecoveryResultBytes - 1}))
	require.True(t, d.cacheBlobRecoveryResult(secondRoot, &blobRecoveryResult{encodedBytes: 2}))
	require.NotContains(t, d.recoveryResults, firstRoot)
	require.Contains(t, d.recoveryResults, secondRoot)
	require.Equal(t, 2, d.recoveryResultBytes)
	require.False(t, d.cacheBlobRecoveryResult(common.Hash{3}, &blobRecoveryResult{encodedBytes: maxBlobRecoveryResultBytes + 1}))
}

func TestBlobRecoveryResultCacheUsesEncodedProtocolBoundary(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.BlobSchedule = []clparams.BlobParameters{{Epoch: 1, MaxBlobsPerBlock: 48}}
	require.EqualValues(t, 48, cfg.MaxBlobsPerBlockUpperBound())

	resultForBlobs := func(count int) *blobRecoveryResult {
		matrix := make([][]cltypes.MatrixEntry, count)
		sidecars := make([]*cltypes.BlobSidecar, count)
		for i := range count {
			matrix[i] = make([]cltypes.MatrixEntry, 128)
			sidecars[i] = &cltypes.BlobSidecar{}
		}
		return newBlobRecoveryResult(nil, false, nil, nil, matrix, sidecars, uint64(count), 0, 0)
	}

	d := &peerdas{recoverBlobsQueue: make(chan recoverBlobsRequest, 1)}
	fortyOne := resultForBlobs(41)
	fortyTwo := resultForBlobs(42)
	require.LessOrEqual(t, fortyOne.encodedBytes, maxBlobRecoveryResultBytes)
	require.Greater(t, fortyTwo.encodedBytes, maxBlobRecoveryResultBytes)
	require.True(t, d.cacheBlobRecoveryResult(common.Hash{41}, fortyOne))
	require.False(t, d.cacheBlobRecoveryResult(common.Hash{42}, fortyTwo))
}

func TestBlobRecoveryResultCacheEvictionPreservesExactRetryOwnership(t *testing.T) {
	resultForBlobs := func(count int) *blobRecoveryResult {
		matrix := make([][]cltypes.MatrixEntry, count)
		sidecars := make([]*cltypes.BlobSidecar, count)
		for i := range count {
			matrix[i] = make([]cltypes.MatrixEntry, 128)
			sidecars[i] = &cltypes.BlobSidecar{}
		}
		return newBlobRecoveryResult(nil, false, nil, nil, matrix, sidecars, uint64(count), 0, 0)
	}

	d := &peerdas{recoverBlobsQueue: make(chan recoverBlobsRequest, 1)}
	first := recoverBlobsRequest{slot: 1, blockRoot: common.Hash{1}}
	second := recoverBlobsRequest{slot: 2, blockRoot: common.Hash{2}}
	firstResult := resultForBlobs(21)
	secondResult := resultForBlobs(21)
	require.Less(t, firstResult.encodedBytes, maxBlobRecoveryResultBytes)
	require.Less(t, secondResult.encodedBytes, maxBlobRecoveryResultBytes)
	require.Greater(t, firstResult.encodedBytes+secondResult.encodedBytes, maxBlobRecoveryResultBytes)
	require.True(t, d.delayBlobRecovery(first))
	require.True(t, d.delayBlobRecovery(second))
	require.True(t, d.cacheBlobRecoveryResult(first.blockRoot, firstResult))
	require.True(t, d.cacheBlobRecoveryResult(second.blockRoot, secondResult))
	require.NotContains(t, d.recoveryResults, first.blockRoot)
	require.Contains(t, d.recoveryResults, second.blockRoot)
	require.Contains(t, d.recoveryRetries, first.blockRoot)
	require.Contains(t, d.recoveryRetries, second.blockRoot)
	require.Contains(t, d.isRecovering, first.blockRoot)
	require.Contains(t, d.isRecovering, second.blockRoot)
	require.LessOrEqual(t, d.recoveryResultBytes, maxBlobRecoveryResultBytes)
}

func TestBlobsRecoverWorkerClearsQueuedOwnershipOnShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).AnyTimes()
	d := &peerdas{
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 2),
		isRecovering:      make(map[common.Hash]bool),
	}
	require.NoError(t, d.enqueueBlobRecovery(recoverBlobsRequest{blockRoot: common.Hash{1}}))
	require.NoError(t, d.enqueueBlobRecovery(recoverBlobsRequest{blockRoot: common.Hash{2}}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	d.blobsRecoverWorker(ctx)

	require.Empty(t, d.isRecovering)
	require.Empty(t, d.recoveryRequests)
	require.Empty(t, d.recoveryGenerations)
	require.Empty(t, d.recoveryRetries)
	require.Empty(t, d.recoveryRetryQueue)
	require.Empty(t, d.recoverySlots)
	require.Empty(t, d.recoverySlotQueue)
}

func assertBlobsRecoverWorkerPrewriteUnavailable(t *testing.T, persistent bool) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, columns := recoverableFuluData(t, &cfg)
	baseColumnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	columnStorage := &savedColumnReadCountingStorage{DataColumnStorage: baseColumnStorage}
	for i := range (cfg.NumberOfColumns + 1) / 2 {
		require.NoError(t, columnStorage.WriteColumnSidecars(t.Context(), root, int64(i), columns[i]))
	}

	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	var countReads atomic.Int32
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).DoAndReturn(
		func(context.Context, common.Hash) (uint32, error) {
			if countReads.Add(1) == 1 {
				return 0, nil
			}
			return uint32(len(sidecars)), nil
		},
	).AnyTimes()
	var durableReads atomic.Int32
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			read := durableReads.Add(1)
			if persistent || read == 1 {
				if persistent && read == 2 {
					cancel()
				}
				return nil, false, errors.New("prewrite storage unavailable")
			}
			return nil, false, nil
		},
	).AnyTimes()
	writeAttempted := make(chan struct{})
	var writeOnce sync.Once
	blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), root, gomock.Any()).DoAndReturn(
		func(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
			writeOnce.Do(func() { close(writeAttempted) })
			cancel()
			return errors.New("stop after observing recovery write")
		},
	).AnyTimes()

	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d := &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columnStorage,
		blobStorage:       blobStorage,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 2),
		isRecovering:      make(map[common.Hash]bool),
	}
	d.recoverBlobsQueue <- recoverBlobsRequest{slot: block.Block.Slot, blockRoot: root, metadata: metadata}
	workerDone := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(workerDone)
	}()
	select {
	case <-workerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("blob recovery worker did not stop")
	}
	expectedReads := int32(3)
	if persistent {
		expectedReads = 2
	}
	require.Equal(t, expectedReads, durableReads.Load(), "prewrite validation must be paced and retain ownership")
	require.Equal(t, int32(1), columnStorage.reads.Load(), "prewrite retry must reuse the completed reconstruction")
	select {
	case <-writeAttempted:
		require.False(t, persistent, "persistent unavailable storage must not be overwritten")
	default:
		require.True(t, persistent, "finite transient outage dropped the recovery owner")
	}
}

func TestRecoverBlobsRequestDoesNotRetainFullBlock(t *testing.T) {
	blockType := reflect.TypeFor[cltypes.ColumnSyncableSignedBlock]()
	requestType := reflect.TypeFor[recoverBlobsRequest]()
	for i := range requestType.NumField() {
		require.NotEqual(t, blockType, requestType.Field(i).Type, "recovery queue must not retain a full block")
	}
}

func TestDownloadRequestDoesNotRetainFullBlock(t *testing.T) {
	blockType := reflect.TypeFor[cltypes.ColumnSyncableSignedBlock]()
	requestType := reflect.TypeFor[downloadRequest]()
	for i := range requestType.NumField() {
		fieldType := requestType.Field(i).Type
		if fieldType.Kind() == reflect.Map {
			require.NotEqual(t, blockType, fieldType.Elem(), "column download lifecycle must not retain full blocks")
		}
	}
}

func TestDownloadColumnsHashesBlockOncePerLifecycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	counted := &blockHashCountingBlock{ColumnSyncableSignedBlock: block}

	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, root).Return(nil, nil).Times(2)
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil)
	d := &peerdas{beaconConfig: &cfg, columnStorage: columnStorage, blobStorage: blobStorage}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, d.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{counted}))
	require.Equal(t, int32(1), counted.calls.Load(), "block root must be reused across the download lifecycle")
}

func TestBlobRecoveryMetadataIsCompactImmutableAcrossBlockSchemas(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	tests := []struct {
		name    string
		version clparams.StateVersion
		block   func() cltypes.ColumnSyncableSignedBlock
	}{
		{name: "fulu block", version: clparams.FuluVersion, block: func() cltypes.ColumnSyncableSignedBlock {
			return cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
		}},
		{name: "fulu blinded block", version: clparams.FuluVersion, block: func() cltypes.ColumnSyncableSignedBlock {
			return cltypes.NewSignedBlindedBeaconBlock(&cfg, clparams.FuluVersion)
		}},
		{name: "gloas block", version: clparams.GloasVersion, block: func() cltypes.ColumnSyncableSignedBlock {
			return cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			block := test.block()
			commitment := cltypes.KZGCommitment{1}
			block.GetBlobKzgCommitments().Append(&commitment)
			root, err := block.BlockHashSSZ()
			require.NoError(t, err)

			metadata, err := newBlobRecoveryMetadata(block, root)
			require.NoError(t, err)
			commitment[0] = 2

			require.Equal(t, common.Hash(root), metadata.blockRoot)
			require.Equal(t, block.GetSlot(), metadata.slot)
			require.Equal(t, test.version, metadata.version)
			require.Equal(t, byte(1), metadata.commitments[0][0], "metadata must own its copied commitment")
		})
	}
}

func TestBlobRecoveryMetadataRejectsWrongBlockSignature(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, _ := recoverableFuluData(t, &cfg)
	sidecars[0].SignedBlockHeader.Signature[0] = 1
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).Return(sidecars, true, nil)
	d := &peerdas{blobStorage: blobStorage}

	require.Equal(t, blobRecoveryInvalid, d.validateStoredBlobRecoveryMetadata(t.Context(), metadata, uint32(len(sidecars))))
}

func TestIsExpectedColumnDownloadMiss(t *testing.T) {
	require.False(t, isExpectedColumnDownloadMiss(nil))
	require.True(t, isExpectedColumnDownloadMiss(&httpreqresp.PeerResponseError{
		Code: httpreqresp.ResponseCodeResourceUnavailable,
	}))
	require.True(t, isExpectedColumnDownloadMiss(fmt.Errorf("column miss: %w", &httpreqresp.PeerResponseError{
		Code: httpreqresp.ResponseCodeResourceUnavailable,
	})))
	require.False(t, isExpectedColumnDownloadMiss(&httpreqresp.PeerResponseError{
		Code:    httpreqresp.ResponseCodeServerError,
		Message: "broken",
	}))
	require.False(t, isExpectedColumnDownloadMiss(&httpreqresp.HTTPError{
		StatusCode: 400,
		Body:       "Read Code: EOF",
	}))
	require.False(t, isExpectedColumnDownloadMiss(errors.New("peer error code: 2 (server error). Error message: broken")))
}

func TestResolveColumnSidecarSlotAndRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	d := &peerdas{beaconConfig: &cfg}
	spe := cfg.SlotsPerEpoch

	t.Run("rejects Gloas schema carrying a pre-Gloas slot", func(t *testing.T) {
		// A peer selects the Gloas decode schema (no SignedBlockHeader) via the
		// response fork-digest, then claims slot 0, which maps to a pre-Gloas
		// fork. The pre-Gloas branch must not dereference the absent header.
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		sidecar.Slot = 0
		require.Nil(t, sidecar.SignedBlockHeader)
		_, _, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.False(t, ok)
	})

	t.Run("rejects pre-Gloas schema with nil signed block header", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		sidecar.SignedBlockHeader = nil
		_, _, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.False(t, ok)
	})

	t.Run("accepts a consistent Fulu sidecar", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		require.NotNil(t, sidecar.SignedBlockHeader)
		sidecar.SignedBlockHeader.Header.Slot = spe // epoch 1 => Fulu
		slot, blockRoot, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.True(t, ok)
		require.Equal(t, spe, slot)
		want, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, common.Hash(want), blockRoot)
	})

	t.Run("accepts a consistent Gloas sidecar", func(t *testing.T) {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		sidecar.Slot = 2 * spe // epoch 2 => Gloas
		sidecar.BeaconBlockRoot = common.HexToHash("0xabc")
		slot, blockRoot, ok := d.resolveColumnSidecarSlotAndRoot(sidecar)
		require.True(t, ok)
		require.Equal(t, 2*spe, slot)
		require.Equal(t, common.HexToHash("0xabc"), blockRoot)
	})
}
