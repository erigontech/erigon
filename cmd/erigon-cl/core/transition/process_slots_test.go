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

	testPubKey    = [48]byte{152, 250, 212, 202, 139, 152, 8, 45, 224, 234, 128, 186, 142, 67, 172, 78, 124, 69, 173, 39, 48, 98, 75, 146, 244, 131, 55, 15, 138, 170, 246, 81, 77, 88, 244, 114, 1, 47, 123, 123, 55, 174, 58, 139, 90, 249, 237, 19}
	testSignature = [96]byte{141, 154, 97, 227, 33, 202, 245, 163, 252, 75, 124, 240, 197, 188, 117, 88, 146, 35, 171, 19, 247, 222, 208, 78, 160, 135, 37, 246, 251, 1, 170, 160, 121, 83, 11, 146, 207, 100, 82, 101, 243, 131, 17, 142, 201, 231, 170, 116, 5, 62, 23, 250, 166, 178, 120, 64, 214, 70, 122, 203, 30, 156, 153, 12, 69, 247, 193, 208, 73, 4, 245, 70, 97, 67, 42, 217, 30, 98, 191, 21, 190, 47, 168, 218, 36, 52, 59, 238, 88, 14, 100, 105, 16, 231, 157, 172}
	badSignature  = [96]byte{182, 82, 244, 116, 233, 59, 56, 251, 52, 194, 122, 255, 161, 96, 204, 165, 43, 97, 19, 48, 130, 187, 17, 200, 223, 62, 114, 194, 225, 19, 242, 174, 224, 24, 188, 83, 118, 45, 23, 192, 205, 200, 47, 165, 212, 35, 193, 189, 10, 165, 161, 72, 81, 250, 195, 186, 174, 197, 26, 208, 165, 254, 31, 214, 135, 140, 129, 47, 211, 59, 87, 136, 55, 242, 93, 149, 128, 30, 84, 126, 182, 157, 70, 90, 68, 113, 7, 92, 70, 230, 164, 54, 120, 16, 180, 151}
	testValidator = &cltypes.Validator{
		PublicKey: testPubKey,
	}
)

func getTestBeaconBlock() *cltypes.SignedBeaconBlockBellatrix {
	return &cltypes.SignedBeaconBlockBellatrix{
		Block: &cltypes.BeaconBlockBellatrix{
			ProposerIndex: 0,
			Body: &cltypes.BeaconBodyBellatrix{
				Graffiti: make([]byte, 32),
				SyncAggregate: &cltypes.SyncAggregate{
					SyncCommiteeBits: make([]byte, 64),
				},
				ExecutionPayload: &cltypes.ExecutionPayload{
					LogsBloom:     make([]byte, 256),
					BaseFeePerGas: make([]byte, 32),
				},
			},
		},
		Signature: testSignature,
	}
}

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

func getTestBeaconStateWithValidator() *cltypes.BeaconState {
	res := getTestBeaconState()
	res.Validators = append(res.Validators, testValidator)
	return res
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

func TestVerifyBlockSignature(t *testing.T) {
	badSigBlock := getTestBeaconBlock()
	badSigBlock.Signature = badSignature
	testCases := []struct {
		description string
		state       *cltypes.BeaconState
		block       *cltypes.SignedBeaconBlockBellatrix
		wantValid   bool
		wantErr     bool
	}{
		{
			description: "success",
			state:       getTestBeaconStateWithValidator(),
			block:       getTestBeaconBlock(),
			wantErr:     false,
			wantValid:   true,
		},
		{
			description: "failure_empty_block",
			state:       getTestBeaconStateWithValidator(),
			block: &cltypes.SignedBeaconBlockBellatrix{
				Block: &cltypes.BeaconBlockBellatrix{
					Body: &cltypes.BeaconBodyBellatrix{},
				},
			},
			wantErr: true,
		},
		{
			description: "failure_bad_signature",
			state:       getTestBeaconStateWithValidator(),
			block:       badSigBlock,
			wantErr:     false,
			wantValid:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			s := New(tc.state, testBeaconConfig, nil)
			valid, err := s.verifyBlockSignature(tc.state, tc.block)
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

			// Confirm that validity matches what we expect.
			if valid != tc.wantValid {
				t.Errorf("unexpected difference in validity: want %v, got %v", tc.wantValid, valid)
			}
		})
	}
}
