package cltypes

import (
	_ "embed"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/block_test_gnosis_deneb.json
var beaconBodyJSON []byte

//go:embed testdata/block_test_gnosis_deneb.ssz
var beaconBodySSZ []byte

func TestBeaconBody(t *testing.T) {
	// Create sample data
	randaoReveal := [96]byte{1, 2, 3}
	eth1Data := &Eth1Data{}
	graffiti := [32]byte{4, 5, 6}
	proposerSlashings := solid.NewStaticListSSZ[*ProposerSlashing](MaxProposerSlashings, 416)
	attesterSlashings := solid.NewDynamicListSSZ[*AttesterSlashing](MaxAttesterSlashings)
	attestations := solid.NewDynamicListSSZ[*solid.Attestation](MaxAttestations)
	deposits := solid.NewStaticListSSZ[*Deposit](MaxDeposits, 1240)
	voluntaryExits := solid.NewStaticListSSZ[*SignedVoluntaryExit](MaxVoluntaryExits, 112)
	syncAggregate := &SyncAggregate{}
	executionChanges := solid.NewStaticListSSZ[*SignedBLSToExecutionChange](MaxExecutionChanges, 172)
	blobKzgCommitments := solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)
	version := clparams.DenebVersion
	block := types.NewBlock(&types.Header{
		BaseFee: big.NewInt(1),
	}, []types.Transaction{types.NewTransaction(1, [20]byte{}, uint256.NewInt(1), 5, uint256.NewInt(2), nil)}, nil, nil, types.Withdrawals{&types.Withdrawal{
		Index: 69,
	}}, nil /*requests*/)

	// Test BeaconBody
	body := &BeaconBody{
		RandaoReveal:       randaoReveal,
		Eth1Data:           eth1Data,
		Graffiti:           graffiti,
		ProposerSlashings:  proposerSlashings,
		AttesterSlashings:  attesterSlashings,
		Attestations:       attestations,
		Deposits:           deposits,
		VoluntaryExits:     voluntaryExits,
		SyncAggregate:      syncAggregate,
		ExecutionPayload:   NewEth1BlockFromHeaderAndBody(block.Header(), block.RawBody(), &clparams.MainnetBeaconConfig),
		ExecutionChanges:   executionChanges,
		BlobKzgCommitments: blobKzgCommitments,
		Version:            version,
		beaconCfg:          &clparams.MainnetBeaconConfig,
	}

	// Test EncodeSSZ and DecodeSSZ
	_, err := body.EncodeSSZ(nil)
	assert.NoError(t, err)
	assert.Error(t, body.DecodeSSZ([]byte{1}, int(version)))

	// Test HashSSZ
	root, err := body.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.HexToHash("918d1ee08d700e422fcce6319cd7509b951d3ebfb1a05291aab9466b7e9826fc"), libcommon.Hash(root))

	// Test the blinded
	blinded, err := body.Blinded()
	assert.NoError(t, err)

	root2, err := blinded.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.HexToHash("918d1ee08d700e422fcce6319cd7509b951d3ebfb1a05291aab9466b7e9826fc"), libcommon.Hash(root2))

	block2 := blinded.Full(body.ExecutionPayload.Transactions, body.ExecutionPayload.Withdrawals)
	assert.Equal(t, block2.ExecutionPayload.version, body.ExecutionPayload.version)
	root3, err := block2.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.HexToHash("918d1ee08d700e422fcce6319cd7509b951d3ebfb1a05291aab9466b7e9826fc"), libcommon.Hash(root3))

	_, err = body.ExecutionPayload.RlpHeader(&libcommon.Hash{})
	assert.NoError(t, err)

	p, err := body.ExecutionPayload.PayloadHeader()
	assert.NoError(t, err)
	assert.NotNil(t, p)

	b := body.ExecutionPayload.Body()
	assert.NoError(t, err)
	assert.NotNil(t, b)
}

func TestBeaconBlockJson(t *testing.T) {
	_, bc := clparams.GetConfigsByNetwork(clparams.GnosisNetwork)
	block := NewSignedBeaconBlock(bc)
	block.Block.Body.Version = clparams.DenebVersion
	err := json.Unmarshal(beaconBodyJSON, block)
	require.NoError(t, err)
	map1 := make(map[string]interface{})
	map2 := make(map[string]interface{})
	err = json.Unmarshal(beaconBodyJSON, &map1)
	require.NoError(t, err)
	out, err := json.Marshal(block)
	require.NoError(t, err)
	err = json.Unmarshal(out, &map2)
	require.NoError(t, err)

	r, _ := block.Block.HashSSZ()

	block2 := NewSignedBeaconBlock(bc)
	if err := block2.DecodeSSZ(beaconBodySSZ, int(clparams.DenebVersion)); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, map1, map2)
	assert.Equal(t, libcommon.Hash(r), libcommon.HexToHash("0x1a9b89eb12282543a5fa0b0f251d8ec0c5c432121d7cb2a8d78461ea9d10c294"))
}
