package cltypes_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func createDepositTest() *cltypes.Deposit {
	// Make proof
	proof := make([]libcommon.Hash, 33)
	for i := range proof {
		proof[i][0] = byte(i)
	}
	return &cltypes.Deposit{
		Proof: proof,
		Data: &cltypes.DepositData{
			Amount: 3994,
			Root:   common.HexToHash("aaa"),
		},
	}
}

var testBeaconBlockVariation = &cltypes.SignedBeaconBlock{
	Block: &cltypes.BeaconBlock{
		Slot:          69,
		ProposerIndex: 96,
		ParentRoot:    common.HexToHash("a"),
		StateRoot:     common.HexToHash("b"),
		Body: &cltypes.BeaconBody{
			Eth1Data: testEth1Data,
			Graffiti: make([]byte, 32),
			ProposerSlashings: []*cltypes.ProposerSlashing{
				{
					Header1: &cltypes.SignedBeaconBlockHeader{
						Header: &cltypes.BeaconBlockHeader{
							Slot:          69,
							ProposerIndex: 96,
							ParentRoot:    common.HexToHash("a"),
							Root:          common.HexToHash("c"),
							BodyRoot:      common.HexToHash("c"),
						},
					},
					Header2: &cltypes.SignedBeaconBlockHeader{
						Header: &cltypes.BeaconBlockHeader{
							Slot:          690,
							ProposerIndex: 402,
							ParentRoot:    common.HexToHash("a"),
							Root:          common.HexToHash("f"),
							BodyRoot:      common.HexToHash("ff"),
						},
					},
				},
			},
			AttesterSlashings: []*cltypes.AttesterSlashing{
				{
					Attestation_1: &cltypes.IndexedAttestation{
						Data: testAttData,
					},
					Attestation_2: &cltypes.IndexedAttestation{
						Data: testAttData,
					},
				},
			},
			Attestations: attestations,
			VoluntaryExits: []*cltypes.SignedVoluntaryExit{
				{
					VolunaryExit: &cltypes.VoluntaryExit{
						Epoch:          99,
						ValidatorIndex: 234,
					},
				},
			},
			Deposits:         []*cltypes.Deposit{createDepositTest(), createDepositTest()},
			SyncAggregate:    &cltypes.SyncAggregate{},
			ExecutionPayload: cltypes.NewEth1Block(clparams.BellatrixVersion),
			ExecutionChanges: []*cltypes.SignedBLSToExecutionChange{
				{
					Message: &cltypes.BLSToExecutionChange{
						ValidatorIndex: 34,
						To:             common.HexToAddress("aaa"),
					},
				},
			},
		},
	},
}

var (
	// Hashes
	capellaHash   = common.HexToHash("0xc4892f81461ed3a24db4b44f26a728219faf1f278d8a1c21d774e2efa73cf1a3")
	bellatrixHash = common.HexToHash("9a5bc717ecaf6a8d6e879478003729b9ce4e71f5c4e9b4bd4dd166780894ee93")
	altairHash    = common.HexToHash("36aa8fe956265d171b7ad740077ea9579e25ed3b2f7b2010016513e4ac4754cb")
	phase0Hash    = common.HexToHash("83dd9e30bf61720822be889abf73760a26fb42dc9fb27fa872f845d68af92bc4")
)

func TestCapellaBlock(t *testing.T) {
	testBeaconBlockVariation.Block.Body.Version = clparams.CapellaVersion
	testBeaconBlockVariation.Block.Body.ExecutionPayload = cltypes.NewEth1Block(clparams.CapellaVersion)
	require.Equal(t, testBeaconBlockVariation.Version(), clparams.CapellaVersion)
	// Simple unit test: unmarshal + marshal + hashtreeroot
	hash, err := testBeaconBlockVariation.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(hash), capellaHash)
	encoded, err := testBeaconBlockVariation.EncodeSSZ(nil)
	require.NoError(t, err)
	block2 := &cltypes.SignedBeaconBlock{}
	require.NoError(t, block2.DecodeSSZ(encoded, int(clparams.CapellaVersion)))
}

func TestBellatrixBlock(t *testing.T) {
	testBeaconBlockVariation.Block.Body.Version = clparams.BellatrixVersion
	testBeaconBlockVariation.Block.Body.ExecutionPayload = cltypes.NewEth1Block(clparams.BellatrixVersion)
	require.Equal(t, testBeaconBlockVariation.Version(), clparams.BellatrixVersion)
	// Simple unit test: unmarshal + marshal + hashtreeroot
	hash, err := testBeaconBlockVariation.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(hash), bellatrixHash)
	encoded, err := testBeaconBlockVariation.EncodeSSZ(nil)
	require.NoError(t, err)
	block2 := &cltypes.SignedBeaconBlock{}
	require.NoError(t, block2.DecodeSSZ(encoded, int(clparams.BellatrixVersion)))
}

func TestAltairBlock(t *testing.T) {
	testBeaconBlockVariation.Block.Body.Version = clparams.AltairVersion
	require.Equal(t, testBeaconBlockVariation.Version(), clparams.AltairVersion)
	// Simple unit test: unmarshal + marshal + hashtreeroot
	hash, err := testBeaconBlockVariation.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(hash), altairHash)
	encoded, err := testBeaconBlockVariation.EncodeSSZ(nil)
	require.NoError(t, err)
	block2 := &cltypes.SignedBeaconBlock{}
	require.NoError(t, block2.DecodeSSZ(encoded, int(clparams.AltairVersion)))
	hash2, err := block2.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(hash2), altairHash)
	// encode/decode for storage
	storageEncoded, err := block2.EncodeForStorage()
	require.NoError(t, err)
	_, _, _, _, err = cltypes.DecodeBeaconBlockForStorage(storageEncoded)
	require.NoError(t, err)
}

func TestPhase0Block(t *testing.T) {
	testBeaconBlockVariation.Block.Body.Version = clparams.Phase0Version
	require.Equal(t, testBeaconBlockVariation.Version(), clparams.Phase0Version)
	// Simple unit test: unmarshal + marshal + hashtreeroot
	hash, err := testBeaconBlockVariation.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(hash), phase0Hash)
	encoded, err := testBeaconBlockVariation.EncodeSSZ(nil)
	require.NoError(t, err)
	block2 := &cltypes.SignedBeaconBlock{}
	require.NoError(t, block2.DecodeSSZ(encoded, int(clparams.Phase0Version)))
	hash2, err := block2.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(hash2), phase0Hash)
	// encode/decode for storage
	storageEncoded, err := block2.EncodeForStorage()
	require.NoError(t, err)
	_, _, _, _, err = cltypes.DecodeBeaconBlockForStorage(storageEncoded)
	require.NoError(t, err)
}
