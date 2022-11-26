package rawdb_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/stretchr/testify/require"
)

func TestBytes2(t *testing.T) {
	len := 1000
	buf := rawdb.Bytes2FromLength(len)
	require.Equal(t, len, rawdb.LengthFromBytes2(buf))
}

func TestBeaconBlock(t *testing.T) {
	signedBeaconBlock := &cltypes.SignedBeaconBlockBellatrix{}
	require.NoError(t, signedBeaconBlock.UnmarshalSSZ(rawdb.SSZTestBeaconBlock))

	blockRoot, err := signedBeaconBlock.Block.HashTreeRoot()
	require.NoError(t, err)

	_, tx := memdb.NewTestTx(t)

	require.NoError(t, rawdb.WriteBeaconBlock(tx, signedBeaconBlock))
	newBlock, err := rawdb.ReadBeaconBlock(tx, signedBeaconBlock.Block.Slot, blockRoot)
	require.NoError(t, err)
	newRoot, err := newBlock.HashTreeRoot()
	require.NoError(t, err)
	root, err := signedBeaconBlock.HashTreeRoot()
	require.NoError(t, err)

	require.Equal(t, root, newRoot)
}
