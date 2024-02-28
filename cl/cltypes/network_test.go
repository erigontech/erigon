package cltypes_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

var testMetadata = &cltypes.Metadata{
	SeqNumber: 99,
	Attnets:   [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
}

var testPing = &cltypes.Ping{
	Id: 420,
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

var testHeader = &cltypes.BeaconBlockHeader{
	Slot:          2,
	ProposerIndex: 24,
	ParentRoot:    libcommon.HexToHash("a"),
	Root:          libcommon.HexToHash("d"),
	BodyRoot:      libcommon.HexToHash("ad"),
}

var testBlockRoot = &cltypes.Root{
	Root: libcommon.HexToHash("a"),
}

var testLightClientUpdatesByRange = &cltypes.LightClientUpdatesByRangeRequest{
	StartPeriod: 100,
	Count:       10,
}

var testBlobRequestByRange = &cltypes.BlobsByRangeRequest{
	StartSlot: 100,
	Count:     10,
}

func TestMarshalNetworkTypes(t *testing.T) {
	cases := []ssz.EncodableSSZ{
		testMetadata,
		testPing,
		testBlockRangeRequest,
		testStatus,
		testBlockRoot,
		testLightClientUpdatesByRange,
		testBlobRequestByRange,
	}

	unmarshalDestinations := []ssz.EncodableSSZ{
		&cltypes.Metadata{},
		&cltypes.Ping{},
		&cltypes.BeaconBlocksByRangeRequest{},
		&cltypes.Status{},
		&cltypes.Root{},
		&cltypes.LightClientUpdatesByRangeRequest{},
		&cltypes.BlobsByRangeRequest{},
	}
	for i, tc := range cases {
		marshalledBytes, err := tc.EncodeSSZ(nil)
		require.NoError(t, err)
		require.Equal(t, len(marshalledBytes), tc.EncodingSizeSSZ())
		require.NoError(t, unmarshalDestinations[i].DecodeSSZ(marshalledBytes, int(clparams.CapellaVersion)))
	}
}
