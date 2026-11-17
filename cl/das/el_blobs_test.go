package das

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// elBlobsFixture returns a Fulu block with n distinct dense blobs and the engine_getBlobsV2
// style response (blobs and per-cell proofs) for it.
func elBlobsFixture(t testing.TB, cfg *clparams.BeaconChainConfig, n int) (*cltypes.SignedBeaconBlock, common.Hash, [][]byte, [][][]byte) {
	t.Helper()
	return elBlobsFixtureOfVersion(t, cfg, clparams.FuluVersion, n)
}

func elBlobsFixtureOfVersion(t testing.TB, cfg *clparams.BeaconChainConfig, version clparams.StateVersion, n int) (*cltypes.SignedBeaconBlock, common.Hash, [][]byte, [][][]byte) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(cfg, version)
	block.Block.Slot = 100
	blobs := make([][]byte, n)
	proofs := make([][][]byte, n)
	for i := range blobs {
		data := testBlobData(t, i, true)
		block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&data.commitment))
		cellProofs := data.cellProofs
		blobs[i] = data.blob[:]
		for j := range cellProofs {
			proofs[i] = append(proofs[i], cellProofs[j][:])
		}
	}
	// A proof moved to another blob at the same cell index must not verify.
	for i := 1; i < n; i++ {
		for j := range proofs[i] {
			require.NotEqual(t, proofs[0][j], proofs[i][j], "blob %d cell %d", i, j)
		}
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return block, root, blobs, proofs
}

// elBlobsTestConfig returns a mainnet config with every fork up to Fulu active from genesis and Gloas from gloasForkEpoch.
func elBlobsTestConfig(gloasForkEpoch uint64) *clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch = 0, 0, 0, 0
	cfg.ElectraForkEpoch, cfg.FuluForkEpoch, cfg.GloasForkEpoch = 0, 0, gloasForkEpoch
	return &cfg
}

func newELBlobsTestPeerDas(t testing.TB, cfg *clparams.BeaconChainConfig, caplinConfig *clparams.CaplinConfig) *peerdas {
	t.Helper()
	// The block and sidecar types read the global config. It is a mainnet copy so the fork epochs of cfg do not leak
	// into other tests.
	global := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&global)
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	state := peerdasstate.NewPeerDasState(cfg, &networkConfig)
	state.SetLocalNodeID(newCgcTestLocalNode(t))
	return &peerdas{
		state:         state,
		beaconConfig:  cfg,
		caplinConfig:  caplinConfig,
		columnStorage: blob_storage.NewDataColumnStore(afero.NewMemMapFs(), cfg, beaconevents.NewEventEmitter()),
	}
}

func TestImportColumnsFromBlobsStoresCustodyColumns(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{})
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)

	available, err := d.IsDataAvailable(block.Block.Slot, root)
	require.NoError(t, err)
	require.False(t, available)

	stored, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	available, err = d.IsDataAvailable(block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, available)
	custody, err := d.state.GetMyCustodyColumns()
	require.NoError(t, err)
	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, len(custody))
	require.Len(t, stored, len(custody))
	for _, sidecar := range stored {
		require.True(t, custody[sidecar.Index])
	}
	for _, index := range saved {
		require.True(t, custody[index])
		sidecar, err := d.columnStorage.ReadColumnSidecarByColumnIndex(t.Context(), block.Block.Slot, root, int64(index))
		require.NoError(t, err)
		require.True(t, VerifyDataColumnSidecar(sidecar))
		require.True(t, VerifyDataColumnSidecarInclusionProof(sidecar))
		require.True(t, VerifyDataColumnSidecarKZGProofs(sidecar))
	}
}

func TestImportColumnsFromBlobsWritesCustodyColumnsInOneBatch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{})
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(gomock.NewController(t))
	d.columnStorage = columnStorage
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)
	custody, err := d.state.GetMyCustodyColumns()
	require.NoError(t, err)
	require.NotEmpty(t, custody)
	require.Less(t, len(custody), int(cfg.NumberOfColumns))
	var written []*cltypes.DataColumnSidecar
	columnStorage.EXPECT().WriteColumnSidecarsBatch(gomock.Any(), root, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ common.Hash, sidecars []*cltypes.DataColumnSidecar) error {
			written = sidecars
			return nil
		}).Times(1)

	stored, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	require.Len(t, written, len(custody))
	for _, sidecar := range written {
		require.True(t, custody[sidecar.Index])
	}
	require.Equal(t, written, stored)
}

type statCountingFs struct {
	afero.Fs
	stats atomic.Int32
}

func (f *statCountingFs) Stat(name string) (os.FileInfo, error) {
	f.stats.Add(1)
	return f.Fs.Stat(name)
}

func TestIsDataAvailableReadsOnlyCustodyColumns(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{})
	fs := &statCountingFs{Fs: afero.NewMemMapFs()}
	d.columnStorage = blob_storage.NewDataColumnStore(fs, &cfg, beaconevents.NewEventEmitter())
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 1)
	custody, err := d.state.GetMyCustodyColumns()
	require.NoError(t, err)
	require.Less(t, len(custody), int(cfg.NumberOfColumns))

	available, err := d.IsDataAvailable(block.Block.Slot, root)
	require.NoError(t, err)
	require.False(t, available)
	require.Equal(t, int32(1), fs.stats.Load(), "the check must stop at the first missing custody column")

	_, err = d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)
	fs.stats.Store(0)
	available, err = d.IsDataAvailable(block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, available)
	require.Equal(t, int32(len(custody)), fs.stats.Load())
}

func TestImportColumnsFromBlobsStoresAllColumnsInArchiveMode(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{ArchiveBlobs: true})
	d.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 1)
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)

	toPublish, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, int(cfg.NumberOfColumns))
	require.Len(t, toPublish, int(cfg.NumberOfColumns))
	select {
	case request := <-d.recoverBlobsQueue:
		require.Equal(t, root, request.blockRoot)
	default:
		t.Fatal("blob recovery was not scheduled")
	}
}

// Without a custody set the node subscribes to no column subnet: it keeps every column and publishes none.
func TestImportColumnsFromBlobsStoresAllColumnsOnCustodyLookupError(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{})
	d.state.SetCustodyGroupCount(cfg.NumberOfCustodyGroups+1, func() uint64 { return 0 })
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)

	toPublish, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, int(cfg.NumberOfColumns))
	require.Empty(t, toPublish)
}

// An archive node subscribes to every column subnet whatever its custody set.
func TestImportColumnsFromBlobsPublishesAllColumnsInArchiveModeOnCustodyLookupError(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{ArchiveBlobs: true})
	d.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 1)
	d.state.SetCustodyGroupCount(cfg.NumberOfCustodyGroups+1, func() uint64 { return 0 })
	_, err := d.state.GetMyCustodyColumns()
	require.Error(t, err)
	block, _, blobs, proofs := elBlobsFixture(t, &cfg, 2)

	toPublish, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	require.Len(t, toPublish, int(cfg.NumberOfColumns))
}

func TestImportColumnsFromBlobsStoresAllColumnsWithoutLocalNode(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{})
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	d.state = peerdasstate.NewPeerDasState(&cfg, &networkConfig)
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)

	toPublish, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, int(cfg.NumberOfColumns))
	require.Empty(t, toPublish)
}

func TestImportColumnsFromBlobsRejectsUnusableResponses(t *testing.T) {
	tests := map[string]func(blobs [][]byte, proofs [][][]byte) ([][]byte, [][][]byte){
		"missing blob":      func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { return b[:1], p[:1] },
		"empty entry":       func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { b[1] = nil; return b, p },
		"short blob":        func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { b[1] = b[1][:100]; return b, p },
		"missing proofs":    func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { p[1] = p[1][:1]; return b, p },
		"short proof":       func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { p[1][3] = p[1][3][:47]; return b, p },
		"extra blob":        func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { return append(b, b[0]), append(p, p[0]) },
		"extra blob only":   func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { return append(b, b[0]), p },
		"extra proofs only": func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { return b, append(p, p[0]) },
	}
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		cfg := elBlobsTestConfig(0)
		if version == clparams.FuluVersion {
			cfg.GloasForkEpoch = cfg.FarFutureEpoch
		}
		for name, corrupt := range tests {
			t.Run(fmt.Sprintf("%s/%s", version, name), func(t *testing.T) {
				d := newELBlobsTestPeerDas(t, cfg, &clparams.CaplinConfig{})
				block, root, blobs, proofs := elBlobsFixtureOfVersion(t, cfg, version, 2)
				blobs, proofs = corrupt(blobs, proofs)

				_, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
				require.Error(t, err)

				saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
				require.NoError(t, err)
				require.Empty(t, saved)
			})
		}
	}
}

// The EL verifies cell proofs when it admits a blob transaction, so the columns are built from its
// response as is (Fulu p2p: clients SHOULD skip verification of blobs from the local EL).
func TestImportColumnsFromBlobsTrustsELCellProofs(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{ArchiveBlobs: true})
	d.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 1)
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)
	proofs[0][5] = proofs[0][6]

	_, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	sidecar, err := d.columnStorage.ReadColumnSidecarByColumnIndex(t.Context(), block.Block.Slot, root, 5)
	require.NoError(t, err)
	require.Equal(t, proofs[0][5], sidecar.KzgProofs.Get(0)[:])
}

func TestImportColumnsFromBlobsSupernodeSchedulesRecovery(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{})
	d.state.SetCustodyGroupCount(cfg.NumberOfCustodyGroups, func() uint64 { return 0 })
	require.True(t, d.state.IsSupernode())
	d.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 1)
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)

	_, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	select {
	case request := <-d.recoverBlobsQueue:
		require.Equal(t, root, request.blockRoot)
	default:
		t.Fatal("blob recovery was not scheduled")
	}
}

func TestImportColumnsFromBlobsPublishesAllColumnsWhenSubscribedToAllTopics(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{SubscribeAllTopics: true})
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)

	toPublish, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	require.Len(t, toPublish, int(cfg.NumberOfColumns))
	custody, err := d.state.GetMyCustodyColumns()
	require.NoError(t, err)
	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, len(custody))
	for _, index := range saved {
		require.True(t, custody[index])
	}
}

func TestImportColumnsFromBlobsBuildsGloasSidecars(t *testing.T) {
	cfg := elBlobsTestConfig(0)
	d := newELBlobsTestPeerDas(t, cfg, &clparams.CaplinConfig{})
	block, root, blobs, proofs := elBlobsFixtureOfVersion(t, cfg, clparams.GloasVersion, 2)
	commitments := block.GetBlobKzgCommitments()

	toPublish, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	custody, err := d.state.GetMyCustodyColumns()
	require.NoError(t, err)
	require.Len(t, toPublish, len(custody))
	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, len(custody))
	for _, sidecar := range toPublish {
		require.True(t, custody[sidecar.Index])
		require.Equal(t, clparams.GloasVersion, sidecar.Version())
		require.Equal(t, block.Block.Slot, sidecar.Slot)
		require.Equal(t, root, sidecar.BeaconBlockRoot)
		require.Nil(t, sidecar.SignedBlockHeader)
		require.Nil(t, sidecar.KzgCommitments)
		require.Nil(t, sidecar.KzgCommitmentsInclusionProof)
		require.Equal(t, len(blobs), sidecar.Column.Len())
		require.Equal(t, len(blobs), sidecar.KzgProofs.Len())
		for i := range blobs {
			require.Equal(t, proofs[i][sidecar.Index], sidecar.KzgProofs.Get(i)[:])
		}
		encoded, err := sidecar.EncodeSSZ(nil)
		require.NoError(t, err)

		decoded := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
		reencoded, err := decoded.EncodeSSZ(nil)
		require.NoError(t, err)
		require.Equal(t, encoded, reencoded)

		fromStorage, err := d.columnStorage.ReadColumnSidecarByColumnIndex(t.Context(), block.Block.Slot, root, int64(sidecar.Index))
		require.NoError(t, err)
		storedEncoded, err := fromStorage.EncodeSSZ(nil)
		require.NoError(t, err)
		require.Equal(t, encoded, storedEncoded)
		require.True(t, VerifyDataColumnSidecarWithCommitments(fromStorage, commitments))
		require.True(t, VerifyDataColumnSidecarKZGProofsWithCommitments(fromStorage, commitments))
	}
}

func TestImportColumnsFromBlobsBuildsSidecarsOfTheBlockForkAtTheGloasBoundary(t *testing.T) {
	const gloasForkEpoch = 10
	cfg := elBlobsTestConfig(gloasForkEpoch)
	d := newELBlobsTestPeerDas(t, cfg, &clparams.CaplinConfig{})
	gloasSlot := gloasForkEpoch * cfg.SlotsPerEpoch
	for _, test := range []struct {
		version clparams.StateVersion
		slot    uint64
	}{
		{clparams.FuluVersion, gloasSlot - 1},
		{clparams.GloasVersion, gloasSlot},
	} {
		t.Run(test.version.String(), func(t *testing.T) {
			block, _, blobs, proofs := elBlobsFixtureOfVersion(t, cfg, test.version, 2)
			block.Block.Slot = test.slot
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)

			toPublish, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
			require.NoError(t, err)

			require.NotEmpty(t, toPublish)
			isFulu := test.version == clparams.FuluVersion
			for _, sidecar := range toPublish {
				require.Equal(t, isFulu, sidecar.SignedBlockHeader != nil)
				require.Equal(t, !isFulu, sidecar.Version() == clparams.GloasVersion)
				fromStorage, err := d.columnStorage.ReadColumnSidecarByColumnIndex(t.Context(), test.slot, root, int64(sidecar.Index))
				require.NoError(t, err)
				require.Equal(t, test.version, fromStorage.Version())
				encoded, err := sidecar.EncodeSSZ(nil)
				require.NoError(t, err)
				storedEncoded, err := fromStorage.EncodeSSZ(nil)
				require.NoError(t, err)
				require.Equal(t, encoded, storedEncoded)
			}
		})
	}
}

func TestImportColumnsFromBlobsStoresAndPublishesAllGloasColumnsInArchiveMode(t *testing.T) {
	cfg := elBlobsTestConfig(0)
	d := newELBlobsTestPeerDas(t, cfg, &clparams.CaplinConfig{ArchiveBlobs: true})
	d.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 1)
	block, root, blobs, proofs := elBlobsFixtureOfVersion(t, cfg, clparams.GloasVersion, 2)

	toPublish, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)

	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, int(cfg.NumberOfColumns))
	require.Len(t, toPublish, int(cfg.NumberOfColumns))
	select {
	case request := <-d.recoverBlobsQueue:
		require.Equal(t, root, request.blockRoot)
	default:
		t.Fatal("blob recovery was not scheduled")
	}
}

func TestImportColumnsFromBlobsRejectsBlockWithoutCommitmentList(t *testing.T) {
	cfg := elBlobsTestConfig(0)
	tests := map[string]struct {
		version clparams.StateVersion
		strip   func(*cltypes.SignedBeaconBlock)
	}{
		"gloas without bid":         {clparams.GloasVersion, func(b *cltypes.SignedBeaconBlock) { b.Block.Body.SignedExecutionPayloadBid = nil }},
		"gloas without bid message": {clparams.GloasVersion, func(b *cltypes.SignedBeaconBlock) { b.Block.Body.SignedExecutionPayloadBid.Message = nil }},
		"fulu nil commitments":      {clparams.FuluVersion, func(b *cltypes.SignedBeaconBlock) { b.Block.Body.BlobKzgCommitments = nil }},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			d := newELBlobsTestPeerDas(t, cfg, &clparams.CaplinConfig{})
			block, _, blobs, proofs := elBlobsFixtureOfVersion(t, cfg, test.version, 2)
			test.strip(block)

			require.NotPanics(t, func() {
				_, err := d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
				require.Error(t, err)
			})
		})
	}
}

// The recovery worker reads a Gloas block's commitments and header from fork choice, so it runs for a block that fork
// choice holds.
func TestImportColumnsFromBlobsGloasRecoveryRunsForBlockInForkChoice(t *testing.T) {
	cfg := elBlobsTestConfig(0)
	d := newELBlobsTestPeerDas(t, cfg, &clparams.CaplinConfig{ArchiveBlobs: true})
	d.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 1)
	gloasDataCache, err := lru.New[common.Hash, *gloasBlockData]("testGloasData", 1)
	require.NoError(t, err)
	d.gloasDataCache = gloasDataCache
	block, root, blobs, proofs := elBlobsFixtureOfVersion(t, cfg, clparams.GloasVersion, 2)
	d.forkChoice = forkChoiceBlocks{root: block}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Unix(0, 0)).AnyTimes()
	d.ethClock = clock

	_, err = d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		d.blobsRecoverWorker(ctx)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })

	require.Eventually(t, func() bool { return d.IsBlobAlreadyRecovered(root) }, 30*time.Second, 20*time.Millisecond)
}
