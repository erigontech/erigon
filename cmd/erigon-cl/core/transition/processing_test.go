package transition

import (
	"encoding/hex"
	"math/big"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/require"
)

var txHashEmpty, _ = merkle_tree.TransactionsListRoot([][]byte{})
var (
	testPublicKeyRandao        = [48]byte{185, 225, 110, 228, 192, 192, 246, 253, 101, 180, 140, 141, 199, 89, 3, 139, 210, 238, 189, 151, 158, 72, 157, 8, 214, 152, 37, 190, 211, 42, 55, 195, 204, 105, 233, 224, 95, 87, 116, 69, 238, 39, 49, 151, 145, 131, 41, 97}
	testSignatureRandao        = [96]byte{176, 25, 98, 55, 102, 11, 185, 56, 3, 22, 58, 233, 230, 117, 206, 195, 18, 104, 110, 224, 140, 189, 89, 124, 247, 110, 34, 24, 137, 244, 55, 83, 15, 8, 202, 57, 11, 60, 112, 92, 226, 219, 183, 237, 167, 236, 67, 192, 11, 6, 2, 84, 137, 7, 71, 232, 61, 13, 111, 99, 125, 174, 150, 122, 177, 219, 187, 234, 12, 60, 229, 15, 199, 29, 125, 30, 238, 123, 23, 138, 38, 232, 248, 31, 234, 1, 149, 77, 70, 111, 237, 19, 7, 35, 209, 236}
	testInvalidSignatureRandao = [96]byte{184, 251, 223, 57, 162, 123, 12, 186, 44, 184, 215, 35, 161, 141, 224, 113, 181, 186, 115, 49, 235, 201, 109, 120, 232, 80, 255, 174, 134, 106, 251, 67, 181, 99, 103, 137, 52, 0, 32, 249, 129, 139, 187, 236, 191, 67, 189, 233, 1, 46, 128, 22, 101, 172, 228, 195, 232, 87, 204, 52, 44, 10, 62, 91, 250, 72, 104, 5, 160, 248, 0, 54, 135, 170, 198, 172, 15, 194, 222, 39, 74, 45, 5, 196, 225, 97, 99, 85, 253, 190, 142, 245, 16, 148, 29, 42}
	emptyBlock                 = &cltypes.Eth1Block{
		Header: &types.Header{
			BaseFee:   big.NewInt(0),
			Number:    big.NewInt(0),
			Bloom:     types.Bloom{},
			TxHashSSZ: txHashEmpty,
		},
		Body: &types.RawBody{},
	}
)

func getTestState(t *testing.T) *state.BeaconState {
	numVals := 2048
	validators := make([]*cltypes.Validator, numVals)
	for i := 0; i < numVals; i++ {
		validators[i] = &cltypes.Validator{
			ActivationEpoch: 0,
			ExitEpoch:       10000,
		}
	}
	b := state.GetEmptyBeaconState()
	b.SetValidators(validators)
	b.SetSlot(19)
	b.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 18})
	b.SetFork(&cltypes.Fork{
		Epoch:           0,
		PreviousVersion: [4]byte{0, 1, 2, 3},
		CurrentVersion:  [4]byte{3, 2, 1, 0},
	})
	return b
}

func getTestBlock(t *testing.T) *cltypes.BeaconBlock {
	header, err := hex.DecodeString("56bdd539e5c03fe53cff0f4af01d459c30f88818a2541339000c1f2a329e84bc")
	if err != nil {
		t.Fatalf("unable to decode test header: %v", err)
	}
	headerArr := [32]byte{}
	copy(headerArr[:], header)
	return &cltypes.BeaconBlock{
		Slot:          19,
		ProposerIndex: 1947,
		ParentRoot:    headerArr,
		Body: &cltypes.BeaconBody{
			Graffiti:         make([]byte, 32),
			Eth1Data:         &cltypes.Eth1Data{},
			SyncAggregate:    &cltypes.SyncAggregate{},
			ExecutionPayload: emptyBlock,
			Version:          clparams.BellatrixVersion,
		},
	}
}

func TestProcessBlockHeader(t *testing.T) {
	testStateSuccess := getTestState(t)
	testState := getTestState(t)
	testBlock := getTestBlock(t)

	badBlockSlot := getTestBlock(t)
	badBlockSlot.Slot = 0

	badLatestSlot := getTestState(t)
	badLatestSlot.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: testBlock.Slot})

	badProposerInd := getTestBlock(t)
	badProposerInd.ProposerIndex = 0

	badParentRoot := getTestBlock(t)
	badParentRoot.ParentRoot = libcommon.Hash{}

	badBlockBodyHash := getTestBlock(t)
	badBlockBodyHash.Body.Attestations = append(badBlockBodyHash.Body.Attestations, &cltypes.Attestation{})

	badStateSlashed := getTestState(t)
	validator, err := badStateSlashed.ValidatorAt(int(testBlock.ProposerIndex))
	require.NoError(t, err)
	validator.Slashed = true
	badStateSlashed.SetValidatorAt(int(testBlock.ProposerIndex), &validator)

	testCases := []struct {
		description string
		state       *state.BeaconState
		block       *cltypes.BeaconBlock
		wantErr     bool
	}{
		{
			description: "success",
			state:       testStateSuccess,
			block:       testBlock,
			wantErr:     false,
		},
		{
			description: "slot_not_equal_state",
			state:       testState,
			block:       badBlockSlot,
			wantErr:     true,
		},
		{
			description: "slot_matches_latest_block",
			state:       badLatestSlot,
			block:       testBlock,
			wantErr:     true,
		},
		{
			description: "bad_proposer_index",
			state:       testState,
			block:       badProposerInd,
			wantErr:     true,
		},
		{
			description: "bad_parent_root",
			state:       testState,
			block:       badParentRoot,
			wantErr:     true,
		},
		{
			description: "bad_block_body_hash",
			state:       testState,
			block:       badBlockBodyHash,
			wantErr:     true,
		},
		{
			description: "bad_state_slashed",
			state:       badStateSlashed,
			block:       testBlock,
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			s := New(tc.state, &clparams.MainnetBeaconConfig, nil, false)
			err := s.ProcessBlockHeader(tc.block)
			if tc.wantErr {
				if err == nil {
					t.Errorf("unexpected success, wanted error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestProcessRandao(t *testing.T) {
	testStateSuccess := getTestState(t)
	propInd, err := testStateSuccess.GetBeaconProposerIndex()
	if err != nil {
		t.Fatalf("unable to get proposer index: %v", err)
	}
	validator, err := testStateSuccess.ValidatorAt(int(propInd))
	require.NoError(t, err)
	validator.PublicKey = testPublicKeyRandao
	testStateSuccess.SetValidatorAt(int(propInd), &validator)
	testBlock := getTestBlock(t)
	testBlock.Body.RandaoReveal = testSignatureRandao
	testBody := testBlock.Body

	testBadStateNoVals := getTestState(t)
	testBadStateNoVals.SetValidators([]*cltypes.Validator{})

	testBlockBadSig1 := getTestBlock(t)
	testBlockBadSig1.Body.RandaoReveal = [96]byte{}

	testBlockBadSig2 := getTestBlock(t)
	testBlockBadSig2.Body.RandaoReveal = testInvalidSignatureRandao

	testCases := []struct {
		description string
		state       *state.BeaconState
		body        *cltypes.BeaconBody
		wantErr     bool
	}{
		{
			description: "success",
			state:       testStateSuccess,
			body:        testBody,
			wantErr:     false,
		},
		{
			description: "bad_proposer_index",
			state:       testBadStateNoVals,
			body:        testBody,
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			s := New(tc.state, &clparams.MainnetBeaconConfig, nil, true)
			err := s.ProcessRandao(tc.body.RandaoReveal, propInd)
			if tc.wantErr {
				if err == nil {
					t.Errorf("unexpected success, wanted error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestProcessEth1Data(t *testing.T) {
	Eth1DataA := &cltypes.Eth1Data{
		Root:         [32]byte{1, 2, 3},
		DepositCount: 42,
		BlockHash:    [32]byte{4, 5, 6},
	}
	eth1dataAHash, err := Eth1DataA.HashSSZ()
	if err != nil {
		t.Fatalf("unable to hash expected eth1data: %v", err)
	}
	Eth1DataB := &cltypes.Eth1Data{
		Root:         [32]byte{3, 2, 1},
		DepositCount: 43,
		BlockHash:    [32]byte{6, 5, 4},
	}
	eth1dataBHash, err := Eth1DataB.HashSSZ()
	if err != nil {
		t.Fatalf("unable to hash expected eth1data: %v", err)
	}
	successState := state.GetEmptyBeaconState()
	successState.SetEth1Data(Eth1DataB)

	// Fill all votes.
	for i := 0; i < int(clparams.MainnetBeaconConfig.EpochsPerEth1VotingPeriod)*int(clparams.MainnetBeaconConfig.SlotsPerEpoch)-1; i++ {
		successState.AddEth1DataVote(Eth1DataA)
	}
	successBody := &cltypes.BeaconBody{
		Eth1Data: Eth1DataA,
	}

	noUpdateState := state.GetEmptyBeaconState()
	noUpdateState.SetEth1Data(Eth1DataB)

	testCases := []struct {
		description  string
		state        *state.BeaconState
		body         *cltypes.BeaconBody
		expectedHash [32]byte
	}{
		{
			description:  "success_update",
			state:        successState,
			body:         successBody,
			expectedHash: eth1dataAHash,
		},
		{
			description:  "success_no_update",
			state:        noUpdateState,
			body:         successBody,
			expectedHash: eth1dataBHash,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			s := New(tc.state, &clparams.MainnetBeaconConfig, &clparams.GenesisConfig{}, false)
			err := s.ProcessEth1Data(tc.body.Eth1Data)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			gotEth1Data := tc.state.Eth1Data()
			gotHash, err := gotEth1Data.HashSSZ()
			if err != nil {
				t.Fatalf("unable to hash output eth1data: %v", err)
			}
			require.Equal(t, gotHash, tc.expectedHash)
		})
	}
}
