package transition

import (
	"encoding/hex"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
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
		Validators:  validators,
		RandaoMixes: make([][32]byte, EPOCHS_PER_HISTORICAL_VECTOR),
	})
}

func getTestBlock(t *testing.T) *cltypes.BeaconBlockBellatrix {
	header, err := hex.DecodeString("56bdd539e5c03fe53cff0f4af01d459c30f88818a2541339000c1f2a329e84bc")
	if err != nil {
		t.Fatalf("unable to decode test header: %v", err)
	}
	headerArr := [32]byte{}
	copy(headerArr[:], header)
	return &cltypes.BeaconBlockBellatrix{
		Slot:          19,
		ProposerIndex: 1947,
		ParentRoot:    headerArr,
		Body: &cltypes.BeaconBodyBellatrix{
			Graffiti: make([]byte, 32),
			Eth1Data: &cltypes.Eth1Data{},
			SyncAggregate: &cltypes.SyncAggregate{
				SyncCommiteeBits: make([]byte, 64),
			},
			ExecutionPayload: &cltypes.ExecutionPayload{
				LogsBloom:     make([]byte, 256),
				BaseFeePerGas: make([]byte, 32),
			},
		},
	}
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
	badBlockSlot.Slot = testStateSuccess.Slot() + 1

	badLatestSlot := getTestState(t)
	badLatestSlot.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: testBlock.Slot})

	badProposerInd := getTestBlock(t)
	badProposerInd.ProposerIndex += 1

	badParentRoot := getTestBlock(t)
	badParentRoot.ParentRoot[0] += 1

	badBlockBodyHash := getTestBlock(t)
	badBlockBodyHash.Body.Attestations = append(badBlockBodyHash.Body.Attestations, &cltypes.Attestation{})

	badStateSlashed := getTestState(t)
	badStateSlashed.ValidatorAt(int(testBlock.ProposerIndex)).Slashed = true

	testCases := []struct {
		description string
		state       *state.BeaconState
		block       *cltypes.BeaconBlockBellatrix
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
