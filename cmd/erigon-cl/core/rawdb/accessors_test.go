package rawdb_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/require"
)

func TestBytes2(t *testing.T) {
	len := 1000
	buf := rawdb.Bytes2FromLength(len)
	require.Equal(t, len, rawdb.LengthFromBytes2(buf))
}

var emptyBlock = &cltypes.Eth1Block{
	Header: &types.Header{
		BaseFee: big.NewInt(0),
		Number:  big.NewInt(0),
	},
}

func TestBeaconBlock(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	signedBeaconBlock := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Body: &cltypes.BeaconBody{
				Eth1Data:         &cltypes.Eth1Data{},
				Graffiti:         make([]byte, 32),
				SyncAggregate:    &cltypes.SyncAggregate{},
				ExecutionPayload: emptyBlock,
			},
		},
	}

	require.NoError(t, rawdb.WriteBeaconBlock(tx, signedBeaconBlock))
	newBlock, err := rawdb.ReadBeaconBlock(tx, signedBeaconBlock.Block.Slot)
	require.NoError(t, err)
	newBlock.Block.Body.ExecutionPayload = emptyBlock
	newRoot, err := newBlock.HashSSZ()
	require.NoError(t, err)
	signedBeaconBlock.Block.Body.ExecutionPayload = emptyBlock
	root, err := signedBeaconBlock.HashSSZ()
	require.NoError(t, err)

	require.Equal(t, root, newRoot)
}

// Benchmarks
func BenchmarkSnappyBeaconBlock(b *testing.B) {
	uncompressed := rawdb.SSZTestBeaconBlock
	fmt.Printf("Uncompressed size: %d\n", len(uncompressed))
	var compressed []byte
	for i := 0; i < b.N; i++ {
		compressed = utils.CompressSnappy(uncompressed)
	}
	fmt.Printf("Compressed size: %d\n", len(compressed))
}

func BenchmarkSnappyBeaconBlockUncompress(b *testing.B) {
	uncompressed := rawdb.SSZTestBeaconBlock
	compressed := utils.CompressSnappy(uncompressed)
	for i := 0; i < b.N; i++ {
		utils.DecompressSnappy(compressed)
	}
}
