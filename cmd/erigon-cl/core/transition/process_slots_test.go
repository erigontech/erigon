package transition

import (
	"encoding/hex"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

var (
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

func getTestBeaconState() *state.BeaconState {
	return state.GetEmptyBeaconState()
}

func getEmptyInvalidBeaconState() *state.BeaconState {
	b := state.GetEmptyBeaconState()
	b.SetCurrentSyncCommittee(&cltypes.SyncCommittee{})
	return b // Invalid public key length
}

func assertStateEq(t *testing.T, got *state.BeaconState, expected *state.BeaconState) {
	assert.Equal(t, got.LatestExecutionPayloadHeader(), expected.LatestExecutionPayloadHeader())

}

func prepareNextBeaconState(t *testing.T, slots []uint64, stateHashs, blockHashs []string, nextState *state.BeaconState) *state.BeaconState {
	// Set slot to initial index.
	for i, val := range slots {
		nextState.SetSlot(val)
		hash, err := hex.DecodeString(stateHashs[i])
		if err != nil {
			t.Fatalf("unable to decode test hash: %v", err)
		}
		nextState.SetStateRootAt(int(val), libcommon.BytesToHash(hash))
		latestBlockHeader := nextState.LatestBlockHeader()
		// Only copy if the previous is empty.
		if latestBlockHeader.Root == [32]byte{} {
			latestBlockHeader.Root = libcommon.BytesToHash(hash)
			nextState.SetLatestBlockHeader(&latestBlockHeader)
		}
		hash, err = hex.DecodeString(blockHashs[i])
		if err != nil {
			t.Fatalf("unable to decode test hash: %v", err)
		}
		nextState.SetBlockRootAt(int(val), libcommon.BytesToHash(hash))
	}
	nextState.SetSlot(slots[len(slots)-1] + 1)
	return nextState
}

func TestTransitionSlot(t *testing.T) {
	slot42 := getTestBeaconState()
	slot42.SetSlot(42)
	testCases := []struct {
		description   string
		prevState     *state.BeaconState
		expectedState *state.BeaconState
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
				getTestBeaconState(),
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
				getTestBeaconState(),
			),
			wantErr: false,
		},
		{
			description:   "failure_empty_state",
			prevState:     getEmptyInvalidBeaconState(),
			expectedState: nil,
			wantErr:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := transitionSlot(tc.prevState)
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
			tc.prevState.SetSlot(tc.prevState.Slot() + 1)
			assertStateEq(t, tc.prevState, tc.expectedState)
		})
	}
}

func TestProcessSlots(t *testing.T) {
	slot42 := getTestBeaconState()
	v := &cltypes.Validator{}
	v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	v.SetWithdrawableEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	slot42.AddValidator(v, 1)
	slot42.SetSlot(42)
	testCases := []struct {
		description   string
		prevState     *state.BeaconState
		expectedState *state.BeaconState
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
				getTestBeaconState(),
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
				getTestBeaconState(),
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
			prevState:     getEmptyInvalidBeaconState(),
			expectedState: nil,
			startSlot:     0,
			numSlots:      1,
			wantErr:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			v := &cltypes.Validator{}
			v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
			v.SetWithdrawableEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
			tc.prevState.AddValidator(v, 1)
			err := ProcessSlots(tc.prevState, tc.startSlot+tc.numSlots)
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
			assertStateEq(t, tc.prevState, tc.expectedState)
		})
	}
}
