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
	testSignature1Slashing       = [96]byte{144, 214, 122, 20, 108, 94, 14, 45, 68, 221, 175, 43, 187, 243, 162, 50, 135, 18, 184, 138, 227, 43, 191, 180, 180, 46, 94, 90, 62, 88, 63, 30, 110, 208, 113, 44, 137, 130, 163, 158, 227, 108, 23, 167, 243, 71, 32, 176, 21, 138, 152, 66, 143, 20, 119, 93, 5, 80, 49, 117, 115, 27, 58, 42, 68, 147, 9, 186, 67, 159, 216, 106, 192, 28, 156, 43, 4, 244, 122, 191, 191, 233, 35, 124, 69, 70, 25, 228, 196, 75, 196, 196, 234, 192, 73, 176}
	testSignature2Slashing       = [96]byte{153, 33, 67, 30, 188, 164, 75, 18, 158, 66, 161, 71, 177, 15, 242, 177, 167, 211, 15, 180, 226, 104, 138, 240, 190, 186, 167, 220, 91, 180, 39, 128, 221, 197, 235, 193, 48, 52, 28, 163, 173, 70, 249, 181, 53, 114, 12, 253, 11, 212, 80, 212, 57, 135, 102, 129, 174, 247, 126, 171, 69, 234, 48, 232, 128, 195, 156, 181, 13, 109, 99, 59, 7, 1, 3, 191, 186, 64, 131, 253, 27, 232, 3, 235, 82, 136, 216, 129, 68, 202, 1, 1, 192, 21, 1, 245}
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
	for i := 0; i < len(successState.Validators()); i++ {
		successBalances = append(successBalances, uint64(i+1))
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
