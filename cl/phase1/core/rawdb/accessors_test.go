package rawdb_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func TestBeaconBlock(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	signedBeaconBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	require.NoError(t, signedBeaconBlock.DecodeSSZ(rawdb.SSZTestBeaconBlock, int(clparams.BellatrixVersion)))

	root, err := signedBeaconBlock.Block.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, rawdb.WriteBeaconBlock(tx, signedBeaconBlock))
	newBlock, _, _, err := rawdb.ReadBeaconBlock(tx, root, signedBeaconBlock.Block.Slot, clparams.BellatrixVersion)
	require.NoError(t, err)
	newRoot, err := newBlock.HashSSZ()
	require.NoError(t, err)
	root, err = signedBeaconBlock.HashSSZ()
	require.NoError(t, err)

	require.Equal(t, root, newRoot)
}

func TestFinalizedBlockRoot(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	signedBeaconBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	require.NoError(t, signedBeaconBlock.DecodeSSZ(rawdb.SSZTestBeaconBlock, int(clparams.BellatrixVersion)))

	root, err := signedBeaconBlock.Block.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, rawdb.WriteFinalizedBlockRoot(tx, signedBeaconBlock.Block.Slot, root))
	newRoot, err := rawdb.ReadFinalizedBlockRoot(tx, signedBeaconBlock.Block.Slot)
	require.NoError(t, err)
	require.Equal(t, libcommon.BytesToHash(root[:]), newRoot)
}
