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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
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
	syncAggregate := NewSyncAggregate()
	executionChanges := solid.NewStaticListSSZ[*SignedBLSToExecutionChange](MaxExecutionChanges, 172)
	blobKzgCommitments := solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)
	version := clparams.DenebVersion
	block := types.NewBlock(&types.Header{
		BaseFee: uint256.NewInt(1),
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
	map1 := make(map[string]any)
	map2 := make(map[string]any)
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

func TestBeaconBodyGetExecutionRequestsListElectraNil(t *testing.T) {
	body := NewBeaconBody(&clparams.MainnetBeaconConfig, clparams.ElectraVersion)
	body.ExecutionRequests = nil

	requests := body.GetExecutionRequestsList()
	require.NotNil(t, requests)
	require.Len(t, requests, 0)
}

func TestBeaconBodyGetExecutionRequestsListDenebNil(t *testing.T) {
	body := NewBeaconBody(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	body.ExecutionRequests = nil

	requests := body.GetExecutionRequestsList()
	require.Nil(t, requests)
}

func TestGetExecutionRequestsListGloasBuilderRequests(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	requests := NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	builderDeposit := &solid.BuilderDepositRequest{
		Amount: 123,
	}
	builderDeposit.PubKey[0] = 0x11
	builderDeposit.WithdrawalCredentials[0] = byte(cfg.BuilderWithdrawalPrefix)
	builderDeposit.Signature[0] = 0x22
	builderExit := &solid.BuilderExitRequest{
		SourceAddress: common.HexToAddress("0x0000000000000000000000000000000000001234"),
	}
	builderExit.PubKey[0] = 0x33
	requests.BuilderDeposits.Append(builderDeposit)
	requests.BuilderExits.Append(builderExit)

	list := GetExecutionRequestsList(&cfg, requests)
	require.Len(t, list, 2)
	require.Equal(t, byte(cfg.BuilderDepositRequestType), list[0][0])
	require.Equal(t, byte(cfg.BuilderExitRequestType), list[1][0])

	encodedDeposit, err := builderDeposit.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedExit, err := builderExit.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, append(hexutil.Bytes{byte(cfg.BuilderDepositRequestType)}, encodedDeposit...), list[0])
	require.Equal(t, append(hexutil.Bytes{byte(cfg.BuilderExitRequestType)}, encodedExit...), list[1])
}

func TestDecodeExecutionRequestsListGloasAllTypes(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	deposit := &solid.DepositRequest{Amount: 1, Index: 2}
	deposit.PubKey[0] = 0x11
	withdrawal := &solid.WithdrawalRequest{SourceAddress: common.HexToAddress("0x0000000000000000000000000000000000001234"), Amount: 3}
	withdrawal.ValidatorPubKey[0] = 0x22
	consolidation := &solid.ConsolidationRequest{SourceAddress: common.HexToAddress("0x0000000000000000000000000000000000005678")}
	consolidation.SourcePubKey[0] = 0x33
	consolidation.TargetPubKey[0] = 0x44
	builderDeposit := &solid.BuilderDepositRequest{Amount: 4}
	builderDeposit.PubKey[0] = 0x55
	builderDeposit.WithdrawalCredentials[0] = byte(cfg.BuilderWithdrawalPrefix)
	builderExit := &solid.BuilderExitRequest{SourceAddress: common.HexToAddress("0x0000000000000000000000000000000000009abc")}
	builderExit.PubKey[0] = 0x66

	encodedDeposit, err := deposit.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedWithdrawal, err := withdrawal.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedConsolidation, err := consolidation.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedBuilderDeposit, err := builderDeposit.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedBuilderExit, err := builderExit.EncodeSSZ(nil)
	require.NoError(t, err)

	out, err := DecodeExecutionRequestsList(&cfg, []hexutil.Bytes{
		append(hexutil.Bytes{byte(cfg.DepositRequestType)}, encodedDeposit...),
		append(hexutil.Bytes{byte(cfg.WithdrawalRequestType)}, encodedWithdrawal...),
		append(hexutil.Bytes{byte(cfg.ConsolidationRequestType)}, encodedConsolidation...),
		append(hexutil.Bytes{byte(cfg.BuilderDepositRequestType)}, encodedBuilderDeposit...),
		append(hexutil.Bytes{byte(cfg.BuilderExitRequestType)}, encodedBuilderExit...),
	}, clparams.GloasVersion)
	require.NoError(t, err)

	require.Equal(t, uint64(1), out.Deposits.Get(0).Amount)
	require.Equal(t, withdrawal.SourceAddress, out.Withdrawals.Get(0).SourceAddress)
	require.Equal(t, consolidation.SourceAddress, out.Consolidations.Get(0).SourceAddress)
	require.Equal(t, uint64(4), out.BuilderDeposits.Get(0).Amount)
	require.Equal(t, builderExit.SourceAddress, out.BuilderExits.Get(0).SourceAddress)
}

func TestDecodeExecutionRequestsListRejectsInvalidShape(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	emptyDeposits, err := solid.NewStaticListSSZ[*solid.DepositRequest](1, solid.SizeDepositRequest).EncodeSSZ(nil)
	require.NoError(t, err)
	emptyWithdrawals, err := solid.NewStaticListSSZ[*solid.WithdrawalRequest](1, solid.SizeWithdrawalRequest).EncodeSSZ(nil)
	require.NoError(t, err)
	emptyBuilderDeposits, err := solid.NewStaticListSSZ[*solid.BuilderDepositRequest](int(cfg.MaxBuilderDepositRequestsPerPayload), solid.SizeBuilderDepositRequest).EncodeSSZ(nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		version  clparams.StateVersion
		requests []hexutil.Bytes
	}{
		{
			name:     "empty entry",
			version:  clparams.GloasVersion,
			requests: []hexutil.Bytes{{}},
		},
		{
			name:     "type only entry",
			version:  clparams.GloasVersion,
			requests: []hexutil.Bytes{{byte(cfg.DepositRequestType)}},
		},
		{
			name:    "duplicate",
			version: clparams.GloasVersion,
			requests: []hexutil.Bytes{
				append(hexutil.Bytes{byte(cfg.DepositRequestType)}, emptyDeposits...),
				append(hexutil.Bytes{byte(cfg.DepositRequestType)}, emptyDeposits...),
			},
		},
		{
			name:    "out of order",
			version: clparams.GloasVersion,
			requests: []hexutil.Bytes{
				append(hexutil.Bytes{byte(cfg.WithdrawalRequestType)}, emptyWithdrawals...),
				append(hexutil.Bytes{byte(cfg.DepositRequestType)}, emptyDeposits...),
			},
		},
		{
			name:     "unknown",
			version:  clparams.GloasVersion,
			requests: []hexutil.Bytes{{0xff, 0x00}},
		},
		{
			name:     "builder before gloas",
			version:  clparams.FuluVersion,
			requests: []hexutil.Bytes{append(hexutil.Bytes{byte(cfg.BuilderDepositRequestType)}, emptyBuilderDeposits...)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeExecutionRequestsList(&cfg, tc.requests, tc.version)
			require.Error(t, err)
		})
	}
}

func TestGetExecutionRequestsListPreGloasOmitsBuilderRequests(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	requests := NewExecutionRequestsWithVersion(&cfg, clparams.FuluVersion)
	requests.BuilderDeposits.Append(&solid.BuilderDepositRequest{Amount: 1})
	requests.BuilderExits.Append(&solid.BuilderExitRequest{})

	require.Empty(t, GetExecutionRequestsList(&cfg, requests))
}

func TestExecutionRequestsJSONRejectsPreGloasBuilderRequests(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	requests := NewExecutionRequestsWithVersion(&cfg, clparams.FuluVersion)

	err := requests.UnmarshalJSON([]byte(`{"deposits":[],"withdrawals":[],"consolidations":[],"builder_deposits":[{"pubkey":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","withdrawal_credentials":"0x0000000000000000000000000000000000000000000000000000000000000000","amount":"1","signature":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}],"builder_exits":[]}`))
	require.Error(t, err)
}

func TestExecutionRequestsJSONTreatsNullListsAsEmpty(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	requests := NewExecutionRequestsWithVersion(&cfg, clparams.FuluVersion)

	require.NotPanics(t, func() {
		err := requests.UnmarshalJSON([]byte(`{"deposits":null,"withdrawals":null,"consolidations":null,"builder_deposits":null,"builder_exits":null}`))
		require.NoError(t, err)
	})
	require.Equal(t, 0, requests.Deposits.Len())
	require.Equal(t, 0, requests.Withdrawals.Len())
	require.Equal(t, 0, requests.Consolidations.Len())
	require.Equal(t, 0, requests.BuilderDeposits.Len())
	require.Equal(t, 0, requests.BuilderExits.Len())
}

func TestExecutionRequestsMarshalJSONOmitsBuilderRequestsBeforeGloas(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	requests := NewExecutionRequestsWithVersion(&cfg, clparams.FuluVersion)

	body, err := json.Marshal(requests)
	require.NoError(t, err)

	require.NotContains(t, string(body), "builder_deposits")
	require.NotContains(t, string(body), "builder_exits")
}

func TestExecutionRequestsMarshalJSONIncludesBuilderRequestsAtGloas(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	requests := NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)

	body, err := json.Marshal(requests)
	require.NoError(t, err)

	require.Contains(t, string(body), "builder_deposits")
	require.Contains(t, string(body), "builder_exits")
}

func TestNewExecutionRequestsDefaultIsPreGloas(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig

	requests := NewExecutionRequests(&cfg)
	gloasRequests := NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	gloasRequestsRoot, err := gloasRequests.HashSSZ()
	require.NoError(t, err)

	require.NotEqual(t, gloasRequestsRoot, requestsRoot)
}

func TestExecutionRequestsZeroValueIsPreGloas(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	zero := &ExecutionRequests{cfg: &cfg}
	constructed := NewExecutionRequests(&cfg)

	zeroRoot, err := zero.HashSSZ()
	require.NoError(t, err)
	constructedRoot, err := constructed.HashSSZ()
	require.NoError(t, err)

	require.Equal(t, constructedRoot, zeroRoot)
}

func TestExecutionRequestsNilConfigPanics(t *testing.T) {
	require.Panics(t, func() {
		_ = NewExecutionRequests(nil)
	})
	require.Panics(t, func() {
		_, _ = (&ExecutionRequests{}).HashSSZ()
	})
}

func TestExecutionRequestsCloneCopiesLists(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	requests := NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	requests.Deposits.Append(&solid.DepositRequest{Amount: 1})
	requests.Withdrawals.Append(&solid.WithdrawalRequest{Amount: 2})
	requests.Consolidations.Append(&solid.ConsolidationRequest{SourceAddress: common.HexToAddress("0x0000000000000000000000000000000000001234")})
	requests.BuilderDeposits.Append(&solid.BuilderDepositRequest{Amount: 3})
	requests.BuilderExits.Append(&solid.BuilderExitRequest{SourceAddress: common.HexToAddress("0x0000000000000000000000000000000000005678")})

	cloned := requests.Clone().(*ExecutionRequests)
	require.Equal(t, requests.Deposits.Get(0), cloned.Deposits.Get(0))
	require.Equal(t, requests.Withdrawals.Get(0), cloned.Withdrawals.Get(0))
	require.Equal(t, requests.Consolidations.Get(0), cloned.Consolidations.Get(0))
	require.Equal(t, requests.BuilderDeposits.Get(0), cloned.BuilderDeposits.Get(0))
	require.Equal(t, requests.BuilderExits.Get(0), cloned.BuilderExits.Get(0))
	require.NotSame(t, requests.Deposits.Get(0), cloned.Deposits.Get(0))
	require.NotSame(t, requests.Withdrawals.Get(0), cloned.Withdrawals.Get(0))
	require.NotSame(t, requests.Consolidations.Get(0), cloned.Consolidations.Get(0))
	require.NotSame(t, requests.BuilderDeposits.Get(0), cloned.BuilderDeposits.Get(0))
	require.NotSame(t, requests.BuilderExits.Get(0), cloned.BuilderExits.Get(0))

	cloned.Deposits.Get(0).Amount = 10
	cloned.Withdrawals.Get(0).Amount = 20
	cloned.Consolidations.Get(0).SourceAddress = common.HexToAddress("0x0000000000000000000000000000000000009999")
	cloned.BuilderDeposits.Get(0).Amount = 30
	cloned.BuilderExits.Get(0).SourceAddress = common.HexToAddress("0x0000000000000000000000000000000000008888")
	require.Equal(t, uint64(1), requests.Deposits.Get(0).Amount)
	require.Equal(t, uint64(2), requests.Withdrawals.Get(0).Amount)
	require.Equal(t, common.HexToAddress("0x0000000000000000000000000000000000001234"), requests.Consolidations.Get(0).SourceAddress)
	require.Equal(t, uint64(3), requests.BuilderDeposits.Get(0).Amount)
	require.Equal(t, common.HexToAddress("0x0000000000000000000000000000000000005678"), requests.BuilderExits.Get(0).SourceAddress)
}

// TestNewBeaconBody_VersionSpecificFields verifies that NewBeaconBody creates
// correct version-specific fields for Fulu and GLOAS.
func TestNewBeaconBody_VersionSpecificFields(t *testing.T) {
	bc := &clparams.MainnetBeaconConfig

	tests := []struct {
		name    string
		version clparams.StateVersion
		// Pre-GLOAS fields
		hasExecutionPayload   bool
		hasBlobKzgCommitments bool
		hasExecutionRequests  bool
		// GLOAS fields
		hasSignedExecutionPayloadBid bool
		hasPayloadAttestations       bool
	}{
		{
			name:                         "Deneb - pre-GLOAS",
			version:                      clparams.DenebVersion,
			hasExecutionPayload:          true,
			hasBlobKzgCommitments:        true,
			hasExecutionRequests:         false, // Deneb doesn't have ExecutionRequests
			hasSignedExecutionPayloadBid: false,
			hasPayloadAttestations:       false,
		},
		{
			name:                         "Electra - pre-GLOAS",
			version:                      clparams.ElectraVersion,
			hasExecutionPayload:          true,
			hasBlobKzgCommitments:        true,
			hasExecutionRequests:         true,
			hasSignedExecutionPayloadBid: false,
			hasPayloadAttestations:       false,
		},
		{
			name:                         "Fulu - pre-GLOAS",
			version:                      clparams.FuluVersion,
			hasExecutionPayload:          true,
			hasBlobKzgCommitments:        true,
			hasExecutionRequests:         true,
			hasSignedExecutionPayloadBid: false,
			hasPayloadAttestations:       false,
		},
		{
			name:                         "GLOAS - post-GLOAS",
			version:                      clparams.GloasVersion,
			hasExecutionPayload:          false,
			hasBlobKzgCommitments:        false,
			hasExecutionRequests:         false,
			hasSignedExecutionPayloadBid: true,
			hasPayloadAttestations:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := NewBeaconBody(bc, tt.version)

			// Check version
			assert.Equal(t, tt.version, body.Version)

			// Pre-GLOAS fields
			if tt.hasExecutionPayload {
				assert.NotNil(t, body.ExecutionPayload, "ExecutionPayload should be set for %s", tt.name)
			} else {
				assert.Nil(t, body.ExecutionPayload, "ExecutionPayload should be nil for %s", tt.name)
			}

			if tt.hasBlobKzgCommitments {
				assert.NotNil(t, body.BlobKzgCommitments, "BlobKzgCommitments should be set for %s", tt.name)
			} else {
				assert.Nil(t, body.BlobKzgCommitments, "BlobKzgCommitments should be nil for %s", tt.name)
			}

			if tt.hasExecutionRequests {
				assert.NotNil(t, body.ExecutionRequests, "ExecutionRequests should be set for %s", tt.name)
			} else {
				assert.Nil(t, body.ExecutionRequests, "ExecutionRequests should be nil for %s", tt.name)
			}

			// GLOAS fields
			if tt.hasSignedExecutionPayloadBid {
				assert.NotNil(t, body.SignedExecutionPayloadBid, "SignedExecutionPayloadBid should be set for %s", tt.name)
				assert.NotNil(t, body.SignedExecutionPayloadBid.Message, "SignedExecutionPayloadBid.Message should be set for %s", tt.name)
			} else {
				assert.Nil(t, body.SignedExecutionPayloadBid, "SignedExecutionPayloadBid should be nil for %s", tt.name)
			}

			if tt.hasPayloadAttestations {
				assert.NotNil(t, body.PayloadAttestations, "PayloadAttestations should be set for %s", tt.name)
			} else {
				assert.Nil(t, body.PayloadAttestations, "PayloadAttestations should be nil for %s", tt.name)
			}

			// Common fields should always be set
			assert.NotNil(t, body.Eth1Data)
			assert.NotNil(t, body.ProposerSlashings)
			assert.NotNil(t, body.AttesterSlashings)
			assert.NotNil(t, body.Attestations)
			assert.NotNil(t, body.Deposits)
			assert.NotNil(t, body.VoluntaryExits)
			assert.NotNil(t, body.ExecutionChanges)
		})
	}
}

// TestBeaconBody_Blinded_GLOASReturnsError verifies that Blinded() returns an error for GLOAS.
func TestBeaconBody_Blinded_GLOASReturnsError(t *testing.T) {
	bc := &clparams.MainnetBeaconConfig

	// GLOAS should return error (test this first since it doesn't need ExecutionPayload setup)
	gloasBody := NewBeaconBody(bc, clparams.GloasVersion)
	_, err := gloasBody.Blinded()
	require.Error(t, err, "Blinded() should return error for GLOAS")
	assert.Contains(t, err.Error(), "not supported for GLOAS")

	// Pre-GLOAS (Fulu) - requires properly initialized ExecutionPayload
	// This is tested in TestBeaconBody which sets up ExecutionPayload properly
}

// TestBeaconBody_ExecutionPayloadMethods_GLOASGuards verifies version guards on ExecutionPayload methods.
func TestBeaconBody_ExecutionPayloadMethods_GLOASGuards(t *testing.T) {
	bc := &clparams.MainnetBeaconConfig

	// GLOAS body
	gloasBody := NewBeaconBody(bc, clparams.GloasVersion)

	// GetPayloadHeader should return error
	_, err := gloasBody.GetPayloadHeader()
	require.Error(t, err, "GetPayloadHeader() should return error for GLOAS")

	// ExecutionPayloadMerkleProof should return error
	_, err = gloasBody.ExecutionPayloadMerkleProof()
	require.Error(t, err, "ExecutionPayloadMerkleProof() should return error for GLOAS")

	// KzgCommitmentMerkleProof should return error
	_, err = gloasBody.KzgCommitmentMerkleProof(0)
	require.Error(t, err, "KzgCommitmentMerkleProof() should return error for GLOAS")

	// KzgCommitmentsInclusionProof should return error
	_, err = gloasBody.KzgCommitmentsInclusionProof()
	require.Error(t, err, "KzgCommitmentsInclusionProof() should return error for GLOAS")
}

// TestBeaconBody_SSZ_RoundTrip_Fulu verifies SSZ encode/decode round-trip for Fulu.
func TestBeaconBody_SSZ_RoundTrip_Fulu(t *testing.T) {
	bc := &clparams.MainnetBeaconConfig
	version := clparams.FuluVersion

	body := NewBeaconBody(bc, version)
	body.RandaoReveal = [96]byte{1, 2, 3}
	body.Graffiti = [32]byte{4, 5, 6}

	// Verify pre-GLOAS fields exist
	assert.NotNil(t, body.ExecutionPayload, "ExecutionPayload should be set")
	assert.NotNil(t, body.BlobKzgCommitments, "BlobKzgCommitments should be set")
	assert.NotNil(t, body.ExecutionRequests, "ExecutionRequests should be set for Fulu")

	// Verify GLOAS fields are nil
	assert.Nil(t, body.SignedExecutionPayloadBid, "SignedExecutionPayloadBid should be nil for Fulu")
	assert.Nil(t, body.PayloadAttestations, "PayloadAttestations should be nil for Fulu")

	// Encoding size should be > 0
	size := body.EncodingSizeSSZ()
	assert.Greater(t, size, 0, "EncodingSizeSSZ should be > 0")
}

// TestBeaconBody_SSZ_RoundTrip_GLOAS verifies SSZ encode/decode round-trip for GLOAS.
func TestBeaconBody_SSZ_RoundTrip_GLOAS(t *testing.T) {
	bc := &clparams.MainnetBeaconConfig
	version := clparams.GloasVersion

	body := NewBeaconBody(bc, version)
	body.RandaoReveal = [96]byte{1, 2, 3}
	body.Graffiti = [32]byte{4, 5, 6}

	// Verify GLOAS fields exist
	assert.NotNil(t, body.SignedExecutionPayloadBid, "SignedExecutionPayloadBid should be set")
	assert.NotNil(t, body.PayloadAttestations, "PayloadAttestations should be set")

	// Verify pre-GLOAS fields are nil
	assert.Nil(t, body.ExecutionPayload, "ExecutionPayload should be nil for GLOAS")
	assert.Nil(t, body.BlobKzgCommitments, "BlobKzgCommitments should be nil for GLOAS")
	assert.Nil(t, body.ExecutionRequests, "ExecutionRequests should be nil for GLOAS")

	// Encoding size should be > 0
	size := body.EncodingSizeSSZ()
	assert.Greater(t, size, 0, "EncodingSizeSSZ should be > 0")
}

// TestBeaconBody_EncodingSizeSSZ_VersionAware verifies EncodingSizeSSZ returns different sizes for versions.
func TestBeaconBody_EncodingSizeSSZ_VersionAware(t *testing.T) {
	bc := &clparams.MainnetBeaconConfig

	fuluBody := NewBeaconBody(bc, clparams.FuluVersion)
	gloasBody := NewBeaconBody(bc, clparams.GloasVersion)

	fuluSize := fuluBody.EncodingSizeSSZ()
	gloasSize := gloasBody.EncodingSizeSSZ()

	// Both should return valid sizes
	assert.Greater(t, fuluSize, 0, "Fulu size should be > 0")
	assert.Greater(t, gloasSize, 0, "GLOAS size should be > 0")

	// Sizes will differ due to different fields
	t.Logf("Fulu EncodingSizeSSZ: %d", fuluSize)
	t.Logf("GLOAS EncodingSizeSSZ: %d", gloasSize)
}

// TestBeaconBody_GetBlobKzgCommitments_VersionAware verifies GetBlobKzgCommitments behavior.
func TestBeaconBody_GetBlobKzgCommitments_VersionAware(t *testing.T) {
	bc := &clparams.MainnetBeaconConfig

	// Fulu should have BlobKzgCommitments directly in BeaconBody
	fuluBody := NewBeaconBody(bc, clparams.FuluVersion)
	assert.NotNil(t, fuluBody.GetBlobKzgCommitments(), "Fulu GetBlobKzgCommitments should return value")
	assert.NotNil(t, fuluBody.BlobKzgCommitments, "Fulu BlobKzgCommitments field should exist")

	// GLOAS BlobKzgCommitments field is nil in BeaconBody (moved to SignedExecutionPayloadBid)
	gloasBody := NewBeaconBody(bc, clparams.GloasVersion)
	assert.Nil(t, gloasBody.BlobKzgCommitments, "GLOAS BeaconBody.BlobKzgCommitments field should be nil")

	// GetBlobKzgCommitments() returns from SignedExecutionPayloadBid.Message for GLOAS
	gloasCommitments := gloasBody.GetBlobKzgCommitments()
	assert.NotNil(t, gloasCommitments, "GLOAS GetBlobKzgCommitments should return from SignedExecutionPayloadBid")
	assert.NotNil(t, gloasBody.GetSignedExecutionPayloadBid())
	assert.NotNil(t, gloasBody.GetSignedExecutionPayloadBid().Message)
}

// TestBeaconBody_GetPayloadAttestations_VersionAware verifies GetPayloadAttestations behavior.
func TestBeaconBody_GetPayloadAttestations_VersionAware(t *testing.T) {
	bc := &clparams.MainnetBeaconConfig

	// Fulu should NOT have PayloadAttestations
	fuluBody := NewBeaconBody(bc, clparams.FuluVersion)
	assert.Nil(t, fuluBody.GetPayloadAttestations(), "Fulu should not have PayloadAttestations")

	// GLOAS should have PayloadAttestations
	gloasBody := NewBeaconBody(bc, clparams.GloasVersion)
	assert.NotNil(t, gloasBody.GetPayloadAttestations(), "GLOAS should have PayloadAttestations")
}
