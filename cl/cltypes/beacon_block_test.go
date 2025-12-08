// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cltypes

import (
	_ "embed"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
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
	require.NoError(t, err)
	assert.Error(t, body.DecodeSSZ([]byte{1}, int(version)))

	// Test HashSSZ
	root, err := body.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, common.HexToHash("918d1ee08d700e422fcce6319cd7509b951d3ebfb1a05291aab9466b7e9826fc"), common.Hash(root))

	// Test the blinded
	blinded, err := body.Blinded()
	require.NoError(t, err)

	root2, err := blinded.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, common.HexToHash("918d1ee08d700e422fcce6319cd7509b951d3ebfb1a05291aab9466b7e9826fc"), common.Hash(root2))

	block2 := blinded.Full(body.ExecutionPayload.Transactions, body.ExecutionPayload.Withdrawals)
	assert.Equal(t, block2.ExecutionPayload.version, body.ExecutionPayload.version)
	root3, err := block2.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, common.HexToHash("918d1ee08d700e422fcce6319cd7509b951d3ebfb1a05291aab9466b7e9826fc"), common.Hash(root3))

	_, err = body.ExecutionPayload.RlpHeader(&common.Hash{}, common.Hash{})
	require.NoError(t, err)

	p, err := body.ExecutionPayload.PayloadHeader()
	require.NoError(t, err)
	assert.NotNil(t, p)

	b := body.ExecutionPayload.Body()
	require.NoError(t, err)
	assert.NotNil(t, b)
}

func TestBeaconBlockJson(t *testing.T) {
	_, bc := clparams.GetConfigsByNetwork(chainspec.GnosisChainID)
	block := NewSignedBeaconBlock(bc, clparams.DenebVersion)
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

	block2 := NewSignedBeaconBlock(bc, clparams.DenebVersion)
	if err := block2.DecodeSSZ(beaconBodySSZ, int(clparams.DenebVersion)); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, map1, map2)
	assert.Equal(t, common.Hash(r), common.HexToHash("0x1a9b89eb12282543a5fa0b0f251d8ec0c5c432121d7cb2a8d78461ea9d10c294"))
}
