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

	testPubKey    = [48]byte{152, 250, 212, 202, 139, 152, 8, 45, 224, 234, 128, 186, 142, 67, 172, 78, 124, 69, 173, 39, 48, 98, 75, 146, 244, 131, 55, 15, 138, 170, 246, 81, 77, 88, 244, 114, 1, 47, 123, 123, 55, 174, 58, 139, 90, 249, 237, 19}
	testSignature = [96]byte{141, 154, 97, 227, 33, 202, 245, 163, 252, 75, 124, 240, 197, 188, 117, 88, 146, 35, 171, 19, 247, 222, 208, 78, 160, 135, 37, 246, 251, 1, 170, 160, 121, 83, 11, 146, 207, 100, 82, 101, 243, 131, 17, 142, 201, 231, 170, 116, 5, 62, 23, 250, 166, 178, 120, 64, 214, 70, 122, 203, 30, 156, 153, 12, 69, 247, 193, 208, 73, 4, 245, 70, 97, 67, 42, 217, 30, 98, 191, 21, 190, 47, 168, 218, 36, 52, 59, 238, 88, 14, 100, 105, 16, 231, 157, 172}
	badSignature  = [96]byte{182, 82, 244, 116, 233, 59, 56, 251, 52, 194, 122, 255, 161, 96, 204, 165, 43, 97, 19, 48, 130, 187, 17, 200, 223, 62, 114, 194, 225, 19, 242, 174, 224, 24, 188, 83, 118, 45, 23, 192, 205, 200, 47, 165, 212, 35, 193, 189, 10, 165, 161, 72, 81, 250, 195, 186, 174, 197, 26, 208, 165, 254, 31, 214, 135, 140, 129, 47, 211, 59, 87, 136, 55, 242, 93, 149, 128, 30, 84, 126, 182, 157, 70, 90, 68, 113, 7, 92, 70, 230, 164, 54, 120, 16, 180, 151}
	testValidator = &cltypes.Validator{
		PublicKey: testPubKey,
	}
	testStateRoot = [32]byte{243, 188, 193, 154, 58, 176, 139, 235, 38, 219, 21, 196, 194, 30, 119, 102, 233, 246, 197, 228, 242, 75, 89, 204, 102, 150, 82, 251, 101, 124, 98, 78}

	stateHashValidator0 = "8545b3e58b298e4b847eba8d6bbc7264df21e36f75924e970aea914bbee82cd9"
	blockHashValidator0 = "f5b74f03650fb65362badf85660ab2f6e92e8df10af9a981a2b5a4df1d9f2479"
	stateHashValidator1 = "5e8ab9f10c2afd511b3fd1079b40686716c449b7195e27ecdf393db2d36467e8"
	blockHashValidator1 = "f5b74f03650fb65362badf85660ab2f6e92e8df10af9a981a2b5a4df1d9f2479"
)

func getEmptyBlock() *cltypes.SignedBeaconBlock {
	return &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Body: &cltypes.BeaconBody{
				Eth1Data:         &cltypes.Eth1Data{},
				SyncAggregate:    &cltypes.SyncAggregate{},
				ExecutionPayload: emptyBlock,
				Version:          clparams.BellatrixVersion,
			},
		},
	}
}

func getTestBeaconBlock() *cltypes.SignedBeaconBlock {
	return &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			ProposerIndex: 0,
			Body: &cltypes.BeaconBody{
				Eth1Data:         &cltypes.Eth1Data{},
				Graffiti:         make([]byte, 32),
				SyncAggregate:    &cltypes.SyncAggregate{},
				ExecutionPayload: emptyBlock,
				Version:          clparams.BellatrixVersion,
			},
			StateRoot: testStateRoot,
		},
		Signature: testSignature,
	}
}

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

func getTestBeaconStateWithValidator() *state.BeaconState {
	res := getTestBeaconState()
	res.SetValidators([]*cltypes.Validator{testValidator})
	return res
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
			nextState.SetLatestBlockHeader(latestBlockHeader)
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
			s := New(tc.prevState, testBeaconConfig, nil, false)
			err := s.transitionSlot()
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
			s := New(tc.prevState, &clparams.MainnetBeaconConfig, nil, false)
			err := s.processSlots(tc.startSlot + tc.numSlots)
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
