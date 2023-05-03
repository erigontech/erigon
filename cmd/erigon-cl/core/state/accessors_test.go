package state_test

import (
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/stretchr/testify/require"
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

func TestGetBlockRoot(t *testing.T) {
	epoch := uint64(2)
	testState := state.GetEmptyBeaconState()
	root := common.HexToHash("ff")
	testState.SetSlot(100)
	testState.SetBlockRootAt(int(epoch*32), root)
	retrieved, err := testState.GetBlockRoot(epoch)
	require.NoError(t, err)
	require.Equal(t, retrieved, root)
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
			got, err := state.GetBeaconProposerIndex()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.expected {
				t.Errorf("unexpected result: got %d, want %d", got, tc.expected)
			}
		})
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
				state := state.New(&clparams.MainnetBeaconConfig)
				preInputs := state.ComputeShuffledIndexPreInputs(tc.seed)
				got, err := state.ComputeShuffledIndex(val, uint64(len(tc.startInds)), tc.seed, preInputs, utils.Keccak256)
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

func generateBeaconStateWithValidators(n int) *state.BeaconState {
	b := state.GetEmptyBeaconState()
	for i := 0; i < n; i++ {
		b.AddValidator(&cltypes.Validator{EffectiveBalance: clparams.MainnetBeaconConfig.MaxEffectiveBalance}, clparams.MainnetBeaconConfig.MaxEffectiveBalance)
	}
	return b
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
			state:       generateBeaconStateWithValidators(5),
			indices:     []uint64{0, 1, 2, 3, 4},
			seed:        seed,
			expected:    2,
		},
		{
			description: "single_active_index",
			state:       generateBeaconStateWithValidators(5),
			indices:     []uint64{3},
			seed:        seed,
			expected:    3,
		},
		{
			description: "second_half_active",
			state:       generateBeaconStateWithValidators(10),
			indices:     []uint64{5, 6, 7, 8, 9},
			seed:        seed,
			expected:    7,
		},
		{
			description: "active_index_out_of_range",
			indices:     []uint64{100},
			state:       generateBeaconStateWithValidators(1),
			seed:        seed,
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			got, err := tc.state.ComputeProposerIndex(tc.indices, tc.seed)
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

func TestSyncReward(t *testing.T) {
	s := state.GetEmptyBeaconState()
	s.AddValidator(&cltypes.Validator{EffectiveBalance: 3099999999909, ExitEpoch: 2}, 3099999999909)
	propReward, partRew, err := s.SyncRewards()
	require.NoError(t, err)
	require.Equal(t, propReward, uint64(30))
	require.Equal(t, partRew, uint64(214))
}

func TestComputeCommittee(t *testing.T) {
	// Create 10 committees
	committeeCount := uint64(10)
	validatorCount := committeeCount * clparams.MainnetBeaconConfig.TargetCommitteeSize
	validators := make([]*cltypes.Validator, validatorCount)

	for i := 0; i < len(validators); i++ {
		var k [48]byte
		copy(k[:], strconv.Itoa(i))
		validators[i] = &cltypes.Validator{
			PublicKey: k,
			ExitEpoch: clparams.MainnetBeaconConfig.FarFutureEpoch,
		}
	}
	bState := state.GetEmptyBeaconState()
	bState.SetValidators(validators)
	bState.SetSlot(200)

	epoch := bState.Epoch()
	indices := bState.GetActiveValidatorsIndices(epoch)
	index := uint64(5)
	// Test shuffled indices are correct for index 5 committee
	committee5, err := bState.ComputeCommittee(indices, 200, index, committeeCount)
	require.NoError(t, err, "Could not compute committee")
	require.NotEqual(t, committee5, nil, "Committee has different shuffled indices")
}

func TestAttestationParticipationFlagIndices(t *testing.T) {
	beaconState := state.GetEmptyBeaconState()
	//beaconState, _ := util.DeterministicGenesisStateAltair(t, params.BeaconConfig().MaxValidatorsPerCommittee)
	beaconState.SetSlot(1)
	cfg := clparams.MainnetBeaconConfig

	tests := []struct {
		name                 string
		inputState           state.BeaconState
		inputData            *cltypes.AttestationData
		inputDelay           uint64
		participationIndices []uint8
	}{
		{
			name: "none",
			inputState: func() state.BeaconState {
				return *beaconState
			}(),
			inputData: &cltypes.AttestationData{
				Source: &cltypes.Checkpoint{},
				Target: &cltypes.Checkpoint{
					Root: [32]byte{2},
				},
			},
			inputDelay:           cfg.SlotsPerEpoch,
			participationIndices: []uint8{},
		},
		{
			name: "participated source",
			inputState: func() state.BeaconState {
				return *beaconState
			}(),
			inputData: &cltypes.AttestationData{
				Source: &cltypes.Checkpoint{},
				Target: &cltypes.Checkpoint{
					Root: [32]byte{2},
				},
			},
			inputDelay:           utils.IntegerSquareRoot(cfg.SlotsPerEpoch) - 1,
			participationIndices: []uint8{cfg.TimelySourceFlagIndex},
		},
		{
			name: "participated source and target",
			inputState: func() state.BeaconState {
				return *beaconState
			}(),
			inputData: &cltypes.AttestationData{
				Source: &cltypes.Checkpoint{},
				Target: &cltypes.Checkpoint{},
			},
			inputDelay:           utils.IntegerSquareRoot(cfg.SlotsPerEpoch) - 1,
			participationIndices: []uint8{cfg.TimelySourceFlagIndex, cfg.TimelyTargetFlagIndex},
		},
		{
			name: "participated source and target and head",
			inputState: func() state.BeaconState {
				return *beaconState
			}(),
			inputData: &cltypes.AttestationData{
				Source: &cltypes.Checkpoint{},
				Target: &cltypes.Checkpoint{},
			},
			inputDelay:           1,
			participationIndices: []uint8{cfg.TimelySourceFlagIndex, cfg.TimelyTargetFlagIndex, cfg.TimelyHeadFlagIndex},
		},
	}

	for _, test := range tests {
		flagIndices, err := test.inputState.GetAttestationParticipationFlagIndicies(test.inputData, test.inputDelay)
		require.NoError(t, err, test.name)
		require.Equal(t, test.participationIndices, flagIndices, test.name)
	}
}
