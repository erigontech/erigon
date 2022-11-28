package transition

import (
	"encoding/hex"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

var (
	testBeaconConfig = &clparams.BeaconChainConfig{
		SlotsPerHistoricalRoot: 8192,
	}
	stateHash0 = "0617561534e6a3ff7fed7f007ae993035b81110f7b7def36e14ff8cbb8034581"
	blockHash0 = "ea9052349d8c9107c4fa04f9a5c5033f6afc7f02e857359c25b426d9948aaaca"
	stateHash1 = "22765160fa57d0be679cdd8c91296f607a056d92c4d30642f06354c8f203abeb"
	blockHash1 = "ea9052349d8c9107c4fa04f9a5c5033f6afc7f02e857359c25b426d9948aaaca"

	stateHash42 = "76d8678d22ed0aa246d128440c6107d5561dcf88ec4472124ad42c72b71cda45"
	blockHash42 = "3ff92b54cba8067044f6b6ca0a69c7a6344154de2a38742e7a89b1057877fffa"
	stateHash43 = "07847da5516dd89b0038c17318feb76dceae92991e0d5aecf7828cf8fcb3be0e"
	blockHash43 = "3ff92b54cba8067044f6b6ca0a69c7a6344154de2a38742e7a89b1057877fffa"
	stateHash44 = "81954d95a6452e516c076f3254424cac99ae3e8c757f33d8aacb97fd8ef02864"
	blockHash44 = "3ff92b54cba8067044f6b6ca0a69c7a6344154de2a38742e7a89b1057877fffa"
)

func getTestBeaconState() *cltypes.BeaconState {
	return &cltypes.BeaconState{
		BlockRoots:        make([][32]byte, 8192),
		StateRoots:        make([][32]byte, 8192),
		RandaoMixes:       make([][32]byte, 65536),
		Slashings:         make([]uint64, 8192),
		JustificationBits: make([]byte, 1),
		CurrentSyncCommittee: &cltypes.SyncCommittee{
			PubKeys: make([][48]byte, 512),
		},
		NextSyncCommittee: &cltypes.SyncCommittee{
			PubKeys: make([][48]byte, 512),
		},
		LatestExecutionPayloadHeader: &cltypes.ExecutionHeader{
			LogsBloom:     make([]byte, 256),
			BaseFeePerGas: make([]byte, 32),
		},
		LatestBlockHeader: &cltypes.BeaconBlockHeader{
			Root: [32]byte{},
		},
		Fork:                        &cltypes.Fork{},
		Eth1Data:                    &cltypes.Eth1Data{},
		PreviousJustifiedCheckpoint: &cltypes.Checkpoint{},
		CurrentJustifiedCheckpoint:  &cltypes.Checkpoint{},
		FinalizedCheckpoint:         &cltypes.Checkpoint{},
	}
}

func prepareNextBeaconState(t *testing.T, slots []uint64, stateHashs, blockHashs []string) *cltypes.BeaconState {
	nextState := getTestBeaconState()
	// Set slot to initial index.
	for i, val := range slots {
		nextState.Slot = val
		hash, err := hex.DecodeString(stateHashs[i])
		if err != nil {
			t.Fatalf("unable to decode test hash: %v", err)
		}
		copy(nextState.StateRoots[val][:], hash)
		// Only copy if the previous is empty.
		if nextState.LatestBlockHeader.Root == [32]byte{} {
			copy(nextState.LatestBlockHeader.Root[:], hash)
		}
		hash, err = hex.DecodeString(blockHashs[i])
		if err != nil {
			t.Fatalf("unable to decode test hash: %v", err)
		}
		copy(nextState.BlockRoots[val][:], hash)
	}
	nextState.Slot = slots[len(slots)-1] + 1
	return nextState
}

func TestTransitionSlot(t *testing.T) {
	slot42 := getTestBeaconState()
	slot42.Slot = 42
	testCases := []struct {
		description   string
		prevState     *cltypes.BeaconState
		expectedState *cltypes.BeaconState
		wantErr       bool
	}{
		{
			description: "success",
			prevState:   getTestBeaconState(),
			expectedState: prepareNextBeaconState(
				t,
				[]uint64{0},
				[]string{stateHash0},
				[]string{blockHash0},
			),
			wantErr: false,
		},
		{
			description: "success_non_zero_slot",
			prevState:   slot42,
			expectedState: prepareNextBeaconState(
				t,
				[]uint64{42},
				[]string{stateHash42},
				[]string{blockHash42},
			),
			wantErr: false,
		},
		{
			description:   "failure_empty_state",
			prevState:     &cltypes.BeaconState{},
			expectedState: nil,
			wantErr:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			s := New(tc.prevState, testBeaconConfig, nil)
			err := s.transitionSlot(tc.prevState)
			if tc.wantErr {
				if err == nil {
					t.Errorf("unexpected success, wanted error")
				}
				return
			}

			// Non-failure case.
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// Manually increase the slot by one.
			tc.prevState.Slot += 1
			if got := tc.prevState; !cmp.Equal(got, tc.expectedState) {
				t.Errorf("unexpected result state: %v", cmp.Diff(got, tc.expectedState))
			}
		})
	}
}

func TestProcessSlots(t *testing.T) {
	slot42 := getTestBeaconState()
	slot42.Slot = 42
	testCases := []struct {
		description   string
		prevState     *cltypes.BeaconState
		expectedState *cltypes.BeaconState
		numSlots      uint64
		startSlot     uint64
		wantErr       bool
	}{
		{
			description: "success_2_slots",
			prevState:   getTestBeaconState(),
			expectedState: prepareNextBeaconState(
				t,
				[]uint64{0, 1},
				[]string{stateHash0, stateHash1},
				[]string{blockHash0, blockHash1},
			),
			numSlots:  2,
			startSlot: 0,
			wantErr:   false,
		},
		{
			description: "success_3_slots",
			prevState:   slot42,
			expectedState: prepareNextBeaconState(
				t,
				[]uint64{42, 43, 44},
				[]string{stateHash42, stateHash43, stateHash44},
				[]string{blockHash42, blockHash43, blockHash44},
			),
			numSlots:  3,
			startSlot: 42,
			wantErr:   false,
		},
		{
			description:   "error_slot_too_low",
			prevState:     getTestBeaconState(),
			expectedState: nil,
			startSlot:     0,
			numSlots:      0,
			wantErr:       true,
		},
		{
			description:   "error_empty_state",
			prevState:     &cltypes.BeaconState{},
			expectedState: nil,
			startSlot:     0,
			numSlots:      1,
			wantErr:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			s := New(tc.prevState, testBeaconConfig, nil)
			err := s.processSlots(tc.prevState, tc.startSlot+tc.numSlots)
			if tc.wantErr {
				if err == nil {
					t.Errorf("unexpected success, wanted error")
				}
				return
			}

			// Non-failure case.
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got := tc.prevState; !cmp.Equal(got, tc.expectedState) {
				t.Errorf("unexpected result state: %v", cmp.Diff(got, tc.expectedState))
			}
		})
	}
}
