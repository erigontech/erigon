package wit_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

var goldenToProto = map[uint]map[uint64]sentryproto.MessageId{
	1: {
		wit.NewWitnessMsg:       sentryproto.MessageId_NEW_WITNESS_W0,
		wit.NewWitnessHashesMsg: sentryproto.MessageId_NEW_WITNESS_HASHES_W0,
		wit.GetWitnessMsg:       sentryproto.MessageId_GET_BLOCK_WITNESS_W0,
		wit.WitnessMsg:          sentryproto.MessageId_BLOCK_WITNESS_W0,
	},
}

var goldenFromProto = map[uint]map[sentryproto.MessageId]uint64{
	1: {
		sentryproto.MessageId_NEW_WITNESS_W0:        wit.NewWitnessMsg,
		sentryproto.MessageId_NEW_WITNESS_HASHES_W0: wit.NewWitnessHashesMsg,
		sentryproto.MessageId_GET_BLOCK_WITNESS_W0:  wit.GetWitnessMsg,
		sentryproto.MessageId_BLOCK_WITNESS_W0:      wit.WitnessMsg,
	},
}

func TestToProtoGolden(t *testing.T) {
	require.Equal(t, goldenToProto, wit.ToProto)
}

func TestFromProtoGolden(t *testing.T) {
	require.Equal(t, goldenFromProto, wit.FromProto)
}
