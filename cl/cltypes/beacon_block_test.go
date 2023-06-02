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
	blobKzgCommitments := solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsPerBlock, 48)
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
		ExecutionPayload:   NewEth1BlockFromHeaderAndBody(block.Header(), block.RawBody()),
		ExecutionChanges:   executionChanges,
		BlobKzgCommitments: blobKzgCommitments,
		Version:            version,
	}

	// Test EncodeSSZ and DecodeSSZ
	_, err := body.EncodeSSZ(nil)
	assert.NoError(t, err)
	assert.Error(t, body.DecodeSSZ([]byte{1}, int(version)))

	// Test HashSSZ
	root, err := body.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.HexToHash("17892e0144f88a0aa3e19f8b5f55129aed3fce23f1f32a08518ccd47b6ecbcf9"), libcommon.Hash(root))

	_, err = body.ExecutionPayload.RlpHeader()
	assert.Error(t, err)

	p, err := body.ExecutionPayload.PayloadHeader()
	assert.NoError(t, err)
	assert.NotNil(t, p)

	b := body.ExecutionPayload.Body()
	assert.NoError(t, err)
	assert.NotNil(t, b)
}
