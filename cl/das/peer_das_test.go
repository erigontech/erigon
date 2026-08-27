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
	"fmt"
	"sync"
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
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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

func (s *writeNotifyingBlobStorage) WriteBlobSidecars(ctx context.Context, blockRoot common.Hash, sidecars []*cltypes.BlobSidecar) error {
	if err := s.BlobStorage.WriteBlobSidecars(ctx, blockRoot, sidecars); err != nil {
		return err
	}
	s.once.Do(func() { close(s.written) })
	return nil
}

func recoverableFuluData(t *testing.T, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, common.Hash, []*cltypes.BlobSidecar, []*cltypes.DataColumnSidecar) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	block.Block.Slot = 100
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
