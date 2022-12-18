package transition

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

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
	numVals := 2048
	validators := make([]*cltypes.Validator, numVals)
	for i := 0; i < numVals; i++ {
		validators[i] = &cltypes.Validator{
			ActivationEpoch: 0,
			ExitEpoch:       10000,
		}
	}

	state := state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
		Validators:  validators,
		RandaoMixes: make([][32]byte, EPOCHS_PER_HISTORICAL_VECTOR),
		Slot:        0,
	})
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
