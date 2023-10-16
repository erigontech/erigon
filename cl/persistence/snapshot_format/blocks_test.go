package snapshot_format_test

import (
	"bytes"
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/snapshot_format"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/phase0.ssz_snappy
var phase0BlockSSZSnappy []byte

//go:embed test_data/altair.ssz_snappy
var altairBlockSSZSnappy []byte

//go:embed test_data/bellatrix.ssz_snappy
var bellatrixBlockSSZSnappy []byte

//go:embed test_data/capella.ssz_snappy
var capellaBlockSSZSnappy []byte

//go:embed test_data/deneb.ssz_snappy
var denebBlockSSZSnappy []byte

// obtain the test blocks
func getTestBlocks(t *testing.T) []*cltypes.SignedBeaconBlock {
	denebBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	capellaBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	bellatrixBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	altairBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	phase0Block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)

	require.NoError(t, utils.DecodeSSZSnappy(denebBlock, denebBlockSSZSnappy, int(clparams.DenebVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(capellaBlock, capellaBlockSSZSnappy, int(clparams.CapellaVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(bellatrixBlock, bellatrixBlockSSZSnappy, int(clparams.BellatrixVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(altairBlock, altairBlockSSZSnappy, int(clparams.AltairVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(phase0Block, phase0BlockSSZSnappy, int(clparams.Phase0Version)))
	return []*cltypes.SignedBeaconBlock{phase0Block, altairBlock, bellatrixBlock, capellaBlock, denebBlock}
}

type TestBlockReader struct {
	Block *cltypes.Eth1Block
}

func (t *TestBlockReader) BlockByNumber(number uint64) (*cltypes.Eth1Block, error) {
	return t.Block, nil
}

func TestBlockSnapshotEncoding(t *testing.T) {
	for _, blk := range getTestBlocks(t) {
		var br TestBlockReader
		if blk.Version() >= clparams.BellatrixVersion {
			br = TestBlockReader{Block: blk.Block.Body.ExecutionPayload}
		}
		var b bytes.Buffer
		require.NoError(t, snapshot_format.WriteBlockForSnapshot(blk, &b))
		blk2, err := snapshot_format.ReadBlockFromSnapshot(&b, &br, &clparams.MainnetBeaconConfig)
		require.NoError(t, err)
		_ = blk2
		hash1, err := blk.HashSSZ()
		require.NoError(t, err)
		hash2, err := blk2.HashSSZ()
		require.NoError(t, err)

		require.Equal(t, hash1, hash2)
	}
}
