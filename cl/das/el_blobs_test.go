package das

import (
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// elBlobsFixture returns a Fulu block with n distinct dense blobs and the engine_getBlobsV2
// style response (blobs and per-cell proofs) for it.
func elBlobsFixture(t testing.TB, cfg *clparams.BeaconChainConfig, n int) (*cltypes.SignedBeaconBlock, common.Hash, [][]byte, [][][]byte) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	block.Block.Slot = 100
	blobs := make([][]byte, n)
	proofs := make([][][]byte, n)
	for i := range blobs {
		blob := &goethkzg.Blob{}
		for j := 0; j < len(blob); j += 32 {
			blob[j+31], blob[j+30] = byte(i+1), byte(j/32+i)
		}
		commitment, err := kzg.Ctx().BlobToKZGCommitment(blob, 0)
		require.NoError(t, err)
		block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
		_, cellProofs, err := peerdasutils.ComputeCellsAndKZGProofs(blob[:])
		require.NoError(t, err)
		blobs[i] = blob[:]
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

func newELBlobsTestPeerDas(t testing.TB, cfg *clparams.BeaconChainConfig, caplinConfig *clparams.CaplinConfig) *peerdas {
	t.Helper()
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

	require.NoError(t, d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs))

	available, err = d.IsDataAvailable(block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, available)
	custody, err := d.state.GetMyCustodyColumns()
	require.NoError(t, err)
	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, len(custody))
	for _, index := range saved {
		require.True(t, custody[index])
		sidecar, err := d.columnStorage.ReadColumnSidecarByColumnIndex(t.Context(), block.Block.Slot, root, int64(index))
		require.NoError(t, err)
		require.True(t, VerifyDataColumnSidecar(sidecar))
		require.True(t, VerifyDataColumnSidecarInclusionProof(sidecar))
		require.True(t, VerifyDataColumnSidecarKZGProofs(sidecar))
	}
}

func TestImportColumnsFromBlobsStoresAllColumnsInArchiveMode(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{ArchiveBlobs: true})
	d.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 1)
	block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)

	require.NoError(t, d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs))

	saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.Len(t, saved, int(cfg.NumberOfColumns))
	select {
	case request := <-d.recoverBlobsQueue:
		require.Equal(t, root, request.blockRoot)
	default:
		t.Fatal("blob recovery was not scheduled")
	}
}

func TestImportColumnsFromBlobsRejectsUnusableResponses(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	tests := map[string]func(blobs [][]byte, proofs [][][]byte) ([][]byte, [][][]byte){
		"missing blob":   func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { return b[:1], p[:1] },
		"empty entry":    func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { b[1] = nil; return b, p },
		"short blob":     func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { b[1] = b[1][:100]; return b, p },
		"missing proofs": func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { p[1] = p[1][:1]; return b, p },
		"short proof":    func(b [][]byte, p [][][]byte) ([][]byte, [][][]byte) { p[1][3] = p[1][3][:47]; return b, p },
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			d := newELBlobsTestPeerDas(t, &cfg, &clparams.CaplinConfig{})
			block, root, blobs, proofs := elBlobsFixture(t, &cfg, 2)
			blobs, proofs = corrupt(blobs, proofs)

			require.Error(t, d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs))

			saved, err := d.columnStorage.GetSavedColumnIndex(t.Context(), block.Block.Slot, root)
			require.NoError(t, err)
			require.Empty(t, saved)
		})
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

	require.NoError(t, d.ImportColumnsFromBlobs(t.Context(), block, blobs, proofs))

	sidecar, err := d.columnStorage.ReadColumnSidecarByColumnIndex(t.Context(), block.Block.Slot, root, 5)
	require.NoError(t, err)
	require.Equal(t, proofs[0][5], sidecar.KzgProofs.Get(0)[:])
}
