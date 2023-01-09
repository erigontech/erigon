package transition

import (
	"encoding/hex"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/common"
)

var (
	testPublicKeyRandao        = [48]byte{185, 225, 110, 228, 192, 192, 246, 253, 101, 180, 140, 141, 199, 89, 3, 139, 210, 238, 189, 151, 158, 72, 157, 8, 214, 152, 37, 190, 211, 42, 55, 195, 204, 105, 233, 224, 95, 87, 116, 69, 238, 39, 49, 151, 145, 131, 41, 97}
	testSignatureRandao        = [96]byte{176, 25, 98, 55, 102, 11, 185, 56, 3, 22, 58, 233, 230, 117, 206, 195, 18, 104, 110, 224, 140, 189, 89, 124, 247, 110, 34, 24, 137, 244, 55, 83, 15, 8, 202, 57, 11, 60, 112, 92, 226, 219, 183, 237, 167, 236, 67, 192, 11, 6, 2, 84, 137, 7, 71, 232, 61, 13, 111, 99, 125, 174, 150, 122, 177, 219, 187, 234, 12, 60, 229, 15, 199, 29, 125, 30, 238, 123, 23, 138, 38, 232, 248, 31, 234, 1, 149, 77, 70, 111, 237, 19, 7, 35, 209, 236}
	testInvalidSignatureRandao = [96]byte{184, 251, 223, 57, 162, 123, 12, 186, 44, 184, 215, 35, 161, 141, 224, 113, 181, 186, 115, 49, 235, 201, 109, 120, 232, 80, 255, 174, 134, 106, 251, 67, 181, 99, 103, 137, 52, 0, 32, 249, 129, 139, 187, 236, 191, 67, 189, 233, 1, 46, 128, 22, 101, 172, 228, 195, 232, 87, 204, 52, 44, 10, 62, 91, 250, 72, 104, 5, 160, 248, 0, 54, 135, 170, 198, 172, 15, 194, 222, 39, 74, 45, 5, 196, 225, 97, 99, 85, 253, 190, 142, 245, 16, 148, 29, 42}
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
	return state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
		Slot: 19,
		LatestBlockHeader: &cltypes.BeaconBlockHeader{
			Slot: 18,
		},
		Fork: &cltypes.Fork{
			Epoch:           0,
			PreviousVersion: [4]byte{0, 1, 2, 3},
			CurrentVersion:  [4]byte{3, 2, 1, 0},
		},
		Validators:  validators,
		RandaoMixes: make([][32]byte, EPOCHS_PER_HISTORICAL_VECTOR),
	})
}

func getTestBlock(t *testing.T) *cltypes.BeaconBlock {
	header, err := hex.DecodeString("56bdd539e5c03fe53cff0f4af01d459c30f88818a2541339000c1f2a329e84bc")
	if err != nil {
		t.Fatalf("unable to decode test header: %v", err)
	}
	headerArr := [32]byte{}
	copy(headerArr[:], header)
	return cltypes.NewBeaconBlock(&cltypes.BeaconBlockBellatrix{
		Slot:          19,
		ProposerIndex: 1947,
		ParentRoot:    headerArr,
		Body: &cltypes.BeaconBodyBellatrix{
			Graffiti:      make([]byte, 32),
			Eth1Data:      &cltypes.Eth1Data{},
			SyncAggregate: &cltypes.SyncAggregate{},
			ExecutionPayload: &cltypes.ExecutionPayload{
				LogsBloom:     make([]byte, 256),
				BaseFeePerGas: make([]byte, 32),
			},
		},
	})
}

func TestComputeShuffledIndex(t *testing.T) {
	testCases := []struct {
		description  string
		startInds    []uint64
		expectedInds []uint64
		seed         [32]byte
	}{
		{
			description:  "success",
			startInds:    []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			expectedInds: []uint64{0, 9, 8, 4, 6, 7, 3, 1, 2, 5},
			seed:         [32]byte{1, 128, 12},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			for i, val := range tc.startInds {
				got, err := ComputeShuffledIndex(val, uint64(len(tc.startInds)), tc.seed)
				// Non-failure case.
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if got != tc.expectedInds[i] {
					t.Errorf("unexpected result: got %d, want %d", got, tc.expectedInds[i])
				}
			}
		})
	}
}

func TestComputeProposerIndex(t *testing.T) {
	seed := [32]byte{}
	copy(seed[:], []byte("seed"))
	testCases := []struct {
		description string
		state       *state.BeaconState
		indices     []uint64
		seed        [32]byte
		expected    uint64
		wantErr     bool
	}{
		{
			description: "success",
			state: state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
				Validators: []*cltypes.Validator{
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
				},
			}),
			indices:  []uint64{0, 1, 2, 3, 4},
			seed:     seed,
			expected: 2,
		},
		{
			description: "single_active_index",
			state: state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
				Validators: []*cltypes.Validator{
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
				},
			}),
			indices:  []uint64{3},
			seed:     seed,
			expected: 3,
		},
		{
			description: "second_half_active",
			state: state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
				Validators: []*cltypes.Validator{
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
				},
			}),
			indices:  []uint64{5, 6, 7, 8, 9},
			seed:     seed,
			expected: 7,
		},
		{
			description: "zero_active_indices",
			indices:     []uint64{},
			seed:        seed,
			wantErr:     true,
		},
		{
			description: "active_index_out_of_range",
			indices:     []uint64{100},
			state: state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
				Validators: []*cltypes.Validator{
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
				},
			}),
			seed:    seed,
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			got, err := ComputeProposerIndex(tc.state, tc.indices, tc.seed)
			if tc.wantErr {
				if err == nil {
					t.Errorf("unexpected success, wanted error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.expected {
				t.Errorf("unexpected result: got %d, want %d", got, tc.expected)
			}
		})
	}
}

func TestGetBeaconProposerIndex(t *testing.T) {
	state := getTestState(t)
	numVals := 2048
	validators := make([]*cltypes.Validator, numVals)
	for i := 0; i < numVals; i++ {
		validators[i] = &cltypes.Validator{
			ActivationEpoch: 0,
			ExitEpoch:       10000,
		}
	}
	testCases := []struct {
		description string
		slot        uint64
		expected    uint64
	}{
		{
			description: "slot1",
			slot:        1,
			expected:    2039,
		},
		{
			description: "slot5",
			slot:        5,
			expected:    1895,
		},
		{
			description: "slot19",
			slot:        19,
			expected:    1947,
		},
		{
			description: "slot30",
			slot:        30,
			expected:    369,
		},
		{
			description: "slot43",
			slot:        43,
			expected:    464,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			state.SetSlot(tc.slot)
			got, err := GetBeaconProposerIndex(state)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.expected {
				t.Errorf("unexpected result: got %d, want %d", got, tc.expected)
			}
		})
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
	badParentRoot.ParentRoot = common.Hash{}

	badBlockBodyHash := getTestBlock(t)
	badBlockBodyHash.Body.Attestations = append(badBlockBodyHash.Body.Attestations, &cltypes.Attestation{})

	badStateSlashed := getTestState(t)
	badStateSlashed.ValidatorAt(int(testBlock.ProposerIndex)).Slashed = true

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
			err := ProcessBlockHeader(tc.state, tc.block)
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
	propInd, err := GetBeaconProposerIndex(testStateSuccess)
	if err != nil {
		t.Fatalf("unable to get proposer index: %v", err)
	}
	testStateSuccess.ValidatorAt(int(propInd)).PublicKey = testPublicKeyRandao

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
		{
			description: "bad_signature_err_processing",
			state:       testStateSuccess,
			body:        testBlockBadSig1.Body,
			wantErr:     true,
		},
		{
			description: "bad_signature_invalid",
			state:       testStateSuccess,
			body:        testBlockBadSig2.Body,
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := ProcessRandao(tc.state, tc.body)
			if tc.wantErr {
				if err == nil {
					t.Errorf("unexpected success, wanted error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			randaoOut := tc.state.RandaoMixes()[0]
			randaoExpected := utils.Keccak256(tc.body.RandaoReveal[:])
			for i := range tc.state.RandaoMixes()[0] {
				if randaoOut[i] != randaoExpected[i] {
					t.Errorf("unexpected output randao byte: got %x, want %x", randaoOut[i], randaoExpected[i])
				}
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
	eth1dataAHash, err := Eth1DataA.HashTreeRoot()
	if err != nil {
		t.Fatalf("unable to hash expected eth1data: %v", err)
	}
	Eth1DataB := &cltypes.Eth1Data{
		Root:         [32]byte{3, 2, 1},
		DepositCount: 43,
		BlockHash:    [32]byte{6, 5, 4},
	}
	eth1dataBHash, err := Eth1DataB.HashTreeRoot()
	if err != nil {
		t.Fatalf("unable to hash expected eth1data: %v", err)
	}
	successState := state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
		Eth1DataVotes: []*cltypes.Eth1Data{},
		Eth1Data:      Eth1DataB,
	})
	// Fill all votes.
	for i := 0; i < int(EPOCHS_PER_ETH1_VOTING_PERIOD)*int(SLOTS_PER_EPOCH)-1; i++ {
		successState.SetEth1DataVotes(append(successState.Eth1DataVotes(), Eth1DataA))
	}
	successBody := &cltypes.BeaconBody{
		Eth1Data: Eth1DataA,
	}

	noUpdateState := state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
		Eth1DataVotes: []*cltypes.Eth1Data{},
		Eth1Data:      Eth1DataB,
	})

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
			err := ProcessEth1Data(tc.state, tc.body)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			gotEth1Data := tc.state.Eth1Data()
			gotHash, err := gotEth1Data.HashTreeRoot()
			if err != nil {
				t.Fatalf("unable to hash output eth1data: %v", err)
			}
			for i := 0; i < len(tc.expectedHash); i++ {
				if gotHash[i] != tc.expectedHash[i] {
					t.Errorf("unexpected output byte: got %x, want %x", gotHash[i], tc.expectedHash[i])
				}
			}
		})
	}
}
