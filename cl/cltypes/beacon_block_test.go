package cltypes

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/assert"
)

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
	}})

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

	_, err = body.ExecutionPayload.RlpHeader()
	assert.NoError(t, err)

	p, err := body.ExecutionPayload.PayloadHeader()
	assert.NoError(t, err)
	assert.NotNil(t, p)

	b := body.ExecutionPayload.Body()
	assert.NoError(t, err)
	assert.NotNil(t, b)
}
