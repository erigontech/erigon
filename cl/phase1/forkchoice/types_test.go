package forkchoice

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestForkChoiceNodePayloadStatusSSZUint8(t *testing.T) {
	root := common.Hash{0x01, 0x02, 0x03}
	node := &ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusFull,
	}

	encoded, err := node.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Len(t, encoded, node.EncodingSizeSSZ())
	require.Len(t, encoded, 33)
	require.Equal(t, root[:], encoded[:32])
	require.Equal(t, byte(cltypes.PayloadStatusFull), encoded[32])

	var decoded ForkChoiceNode
	require.NoError(t, decoded.DecodeSSZ(encoded, 0))
	require.Equal(t, *node, decoded)
}
