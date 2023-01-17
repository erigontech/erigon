package cltypes_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
)

var testMetadata = &cltypes.Metadata{
	SeqNumber: 99,
	Attnets:   69,
}

var testPing = &cltypes.Ping{
	Id: 420,
}

var testSingleRoot = &cltypes.SingleRoot{
	Root: libcommon.HexToHash("96"),
}

var testLcRangeRequest = &cltypes.LightClientUpdatesByRangeRequest{
	Period: 69,
	Count:  666,
}

var testBlockRangeRequest = &cltypes.BeaconBlocksByRangeRequest{
	StartSlot: 999,
	Count:     666,
}

var testStatus = &cltypes.Status{
	FinalizedEpoch: 666,
	HeadSlot:       94,
	HeadRoot:       libcommon.HexToHash("a"),
	FinalizedRoot:  libcommon.HexToHash("bbba"),
}

func TestMarshalNetworkTypes(t *testing.T) {
	cases := []ssz_utils.EncodableSSZ{
		testMetadata,
		testPing,
		testSingleRoot,
		testLcRangeRequest,
		testBlockRangeRequest,
		testStatus,
	}

	unmarshalDestinations := []ssz_utils.EncodableSSZ{
		&cltypes.Metadata{},
		&cltypes.Ping{},
		&cltypes.SingleRoot{},
		&cltypes.LightClientUpdatesByRangeRequest{},
		&cltypes.BeaconBlocksByRangeRequest{},
		&cltypes.Status{},
	}
	for i, tc := range cases {
		marshalledBytes, err := tc.MarshalSSZ()
		require.NoError(t, err)
		require.Equal(t, len(marshalledBytes), tc.SizeSSZ())
		require.NoError(t, unmarshalDestinations[i].UnmarshalSSZ(marshalledBytes))
		require.Equal(t, tc, unmarshalDestinations[i])
	}
}
