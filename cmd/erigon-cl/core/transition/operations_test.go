package transition

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/common"
)

const propInd = 49

var (
	testPublicKeySlashing        = [48]byte{152, 250, 212, 202, 139, 152, 8, 45, 224, 234, 128, 186, 142, 67, 172, 78, 124, 69, 173, 39, 48, 98, 75, 146, 244, 131, 55, 15, 138, 170, 246, 81, 77, 88, 244, 114, 1, 47, 123, 123, 55, 174, 58, 139, 90, 249, 237, 19}
	testSignature1Slashing       = [96]byte{179, 104, 72, 191, 213, 139, 46, 155, 142, 138, 167, 50, 171, 167, 231, 134, 31, 99, 131, 242, 224, 129, 206, 76, 251, 178, 222, 59, 93, 212, 112, 203, 85, 98, 243, 126, 170, 62, 222, 18, 139, 22, 133, 87, 206, 254, 254, 70, 7, 42, 7, 47, 113, 97, 248, 217, 225, 157, 154, 51, 45, 122, 145, 179, 242, 86, 234, 185, 234, 108, 137, 146, 176, 240, 185, 171, 163, 36, 59, 57, 119, 212, 11, 248, 218, 131, 109, 199, 108, 214, 111, 134, 27, 184, 196, 85}
	testSignature2Slashing       = [96]byte{169, 127, 218, 99, 123, 79, 24, 27, 138, 108, 78, 235, 220, 198, 118, 249, 160, 101, 179, 187, 11, 118, 39, 136, 56, 186, 213, 229, 213, 227, 222, 97, 142, 136, 48, 120, 79, 196, 187, 189, 107, 19, 155, 24, 61, 82, 29, 108, 1, 9, 188, 172, 199, 104, 80, 90, 192, 163, 166, 230, 109, 139, 149, 0, 20, 47, 46, 68, 185, 2, 3, 57, 220, 3, 7, 91, 243, 223, 46, 244, 247, 116, 57, 13, 49, 227, 26, 113, 44, 201, 221, 130, 150, 192, 143, 100}
	testInvalidSignatureSlashing = [96]byte{175, 121, 194, 204, 23, 153, 149, 62, 85, 130, 13, 116, 164, 165, 251, 183, 234, 72, 227, 255, 153, 244, 144, 223, 56, 126, 249, 105, 167, 225, 181, 244, 124, 0, 105, 244, 119, 44, 220, 81, 61, 86, 39, 0, 160, 141, 129, 189, 20, 63, 56, 209, 84, 8, 109, 198, 218, 205, 81, 27, 158, 233, 122, 30, 63, 146, 110, 9, 161, 170, 166, 77, 1, 5, 21, 44, 0, 24, 31, 98, 70, 255, 142, 176, 163, 73, 171, 238, 152, 73, 6, 231, 43, 21, 43, 208}
)

func getSuccessfulSlashing() *cltypes.ProposerSlashing {
	return &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          42,
				ProposerIndex: propInd,
				Root:          common.BytesToHash([]byte("root1")),
			},
			Signature: testSignature1Slashing,
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          42,
				ProposerIndex: propInd,
				Root:          common.BytesToHash([]byte("root2")),
			},
			Signature: testSignature2Slashing,
		},
	}
}

func TestProcessProposerSlashing(t *testing.T) {
	unchangingState := getTestState(t)
	unchangingState.SetSlashings([]uint64{0})
	unchangingState.SetValidatorAt(propInd, &cltypes.Validator{
		Slashed:           false,
		ActivationEpoch:   0,
		WithdrawableEpoch: 10000,
		PublicKey:         testPublicKeySlashing,
	})

	successState := getTestState(t)
	successState.SetSlashings([]uint64{0})
	successState.SetValidatorAt(propInd, &cltypes.Validator{
		Slashed:           false,
		ActivationEpoch:   0,
		WithdrawableEpoch: 10000,
		PublicKey:         testPublicKeySlashing,
	})
	successBalances := []uint64{}
	wantBalances := []uint64{}
	for i := 0; i < len(successState.Validators()); i++ {
		successBalances = append(successBalances, uint64(i+1))
		wantBalances = append(wantBalances, uint64(i+1))
	}
	successState.SetBalances(successBalances)

	successSlashing := getSuccessfulSlashing()

	failureSlashingSlots := getSuccessfulSlashing()
	failureSlashingSlots.Header1.Header.Slot += 1

	failureSlashingPIs := getSuccessfulSlashing()
	failureSlashingPIs.Header1.Header.ProposerIndex += 1

	failureSlashingRoots := getSuccessfulSlashing()
	failureSlashingRoots.Header1.Header.Root = failureSlashingRoots.Header2.Header.Root

	failureSlashingSlashable := getSuccessfulSlashing()
	failureSlashingSlashable.Header1.Header.ProposerIndex = 0
	failureSlashingSlashable.Header2.Header.ProposerIndex = 0

	failureSlashingErrorSig := getSuccessfulSlashing()
	failureSlashingErrorSig.Header1.Signature = [96]byte{}

	failureSlashingInvalidSig := getSuccessfulSlashing()
	failureSlashingInvalidSig.Header1.Signature = testInvalidSignatureSlashing

	testCases := []struct {
		description string
		state       *state.BeaconState
		slashing    *cltypes.ProposerSlashing
		wantErr     bool
	}{
		{
			description: "success",
			state:       successState,
			slashing:    successSlashing,
			wantErr:     false,
		},
		{
			description: "failure_slots_dont_match",
			state:       successState,
			slashing:    failureSlashingSlots,
			wantErr:     true,
		},
		{
			description: "failure_slots_prop_inds_dont_match",
			state:       successState,
			slashing:    failureSlashingPIs,
			wantErr:     true,
		},
		{
			description: "failure_slots_differ_root",
			state:       successState,
			slashing:    failureSlashingRoots,
			wantErr:     true,
		},
		{
			description: "failure_not_slashable",
			state:       successState,
			slashing:    failureSlashingSlashable,
			wantErr:     true,
		},
		{
			description: "failure_error_sig",
			state:       unchangingState,
			slashing:    failureSlashingErrorSig,
			wantErr:     true,
		},
		{
			description: "failure_invalid_sig",
			state:       unchangingState,
			slashing:    failureSlashingInvalidSig,
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := ProcessProposerSlashing(tc.state, tc.slashing)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("unexpected success, want error")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error, wanted success: %v", err)
			}
		})
	}
}
