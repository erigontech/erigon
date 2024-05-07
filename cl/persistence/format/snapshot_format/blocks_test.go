package snapshot_format_test

import (
	"bytes"
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
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

var emptyBlock = cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)

// obtain the test blocks
func getTestBlocks(t *testing.T) []*cltypes.SignedBeaconBlock {
	var emptyBlockCapella = cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	emptyBlockCapella.Block.Slot = clparams.MainnetBeaconConfig.CapellaForkEpoch * 32

	emptyBlock.EncodingSizeSSZ()
	emptyBlockCapella.EncodingSizeSSZ()
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
	return []*cltypes.SignedBeaconBlock{phase0Block, altairBlock, bellatrixBlock, capellaBlock, denebBlock, emptyBlock, emptyBlockCapella}
}

func TestBlockSnapshotEncoding(t *testing.T) {
	for _, blk := range getTestBlocks(t) {
		var br snapshot_format.MockBlockReader
		if blk.Version() >= clparams.BellatrixVersion {
			br = snapshot_format.MockBlockReader{Block: blk.Block.Body.ExecutionPayload}
		}
		var b bytes.Buffer
		_, err := snapshot_format.WriteBlockForSnapshot(&b, blk, nil)
		require.NoError(t, err)
		blk2, err := snapshot_format.ReadBlockFromSnapshot(&b, &br, &clparams.MainnetBeaconConfig)
		require.NoError(t, err)

		hash1, err := blk.HashSSZ()
		require.NoError(t, err)
		hash2, err := blk2.HashSSZ()
		require.NoError(t, err)
		// Rewrite for header test
		b.Reset()
		_, err = snapshot_format.WriteBlockForSnapshot(&b, blk, nil)
		require.NoError(t, err)
		header, bn, bHash, err := snapshot_format.ReadBlockHeaderFromSnapshotWithExecutionData(&b, &clparams.MainnetBeaconConfig)
		require.NoError(t, err)
		hash3, err := header.HashSSZ()
		require.NoError(t, err)
		// now do it with blinded
		require.Equal(t, hash1, hash2)

		require.Equal(t, header.Signature, blk.Signature)
		require.Equal(t, header.Header.Slot, blk.Block.Slot)

		b.Reset()
		_, err = snapshot_format.WriteBlockForSnapshot(&b, blk, nil)
		require.NoError(t, err)
		blk4, err := snapshot_format.ReadBlindedBlockFromSnapshot(&b, &clparams.MainnetBeaconConfig)
		require.NoError(t, err)

		hash4, err := blk4.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, hash1, hash4)

		if blk.Version() >= clparams.BellatrixVersion {
			require.Equal(t, bn, blk.Block.Body.ExecutionPayload.BlockNumber)
			require.Equal(t, bHash, blk.Block.Body.ExecutionPayload.BlockHash)
		}
		require.Equal(t, hash3, hash2)
	}
}
