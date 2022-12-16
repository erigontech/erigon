package transition

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
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
		state       *cltypes.BeaconStateBellatrix
		indices     []uint64
		seed        [32]byte
		expected    uint64
		wantErr     bool
	}{
		{
			description: "success",
			state: &cltypes.BeaconStateBellatrix{
				Validators: []*cltypes.Validator{
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
				},
			},
			indices:  []uint64{0, 1, 2, 3, 4},
			seed:     seed,
			expected: 2,
		},
		{
			description: "single_active_index",
			state: &cltypes.BeaconStateBellatrix{
				Validators: []*cltypes.Validator{
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
				},
			},
			indices:  []uint64{3},
			seed:     seed,
			expected: 3,
		},
		{
			description: "second_half_active",
			state: &cltypes.BeaconStateBellatrix{
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
			},
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
			state: &cltypes.BeaconStateBellatrix{
				Validators: []*cltypes.Validator{
					{EffectiveBalance: testBeaconConfig.MaxEffectiveBalance},
				},
			},
			seed:    seed,
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			got, err := ComputePropserIndex(tc.state, tc.indices, tc.seed)
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
